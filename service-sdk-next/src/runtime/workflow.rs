//! In-memory workflow execution. No history replay or durable execution is implied.
use super::{CommandId, CommandReply, Lifecycle, NewRuns};
use futures_util::FutureExt;
use std::{
    collections::BTreeMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

pub trait WorkflowFailure: Clone + Send + 'static {
    fn cancelled() -> Self;
    fn recovery(reason: &'static str) -> Self;
    fn needs_recovery(&self) -> bool;
    fn is_cancelled(&self) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowStatus {
    Waiting,
    Running { activity: Option<&'static str> },
    Cancelling,
    Completed,
    Failed,
    Cancelled,
    NeedsRecovery,
}

/// Activities are awaited to completion, even after cancellation. Only the
/// activity implementation can confirm that external effects have settled.
#[derive(Clone)]
pub struct WorkflowContext {
    pub id: u64,
    cancel: CancellationToken,
    enabled: watch::Receiver<bool>,
    status: Arc<Mutex<WorkflowStatus>>,
    changed: Arc<Notify>,
}

impl WorkflowContext {
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancel
    }

    pub async fn checkpoint<E: WorkflowFailure>(&self) -> Result<(), E> {
        let mut enabled = self.enabled.clone();
        loop {
            if self.cancel.is_cancelled() {
                return Err(E::cancelled());
            }
            if *enabled.borrow_and_update() {
                return Ok(());
            }
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(E::cancelled()),
                result = enabled.changed() => if result.is_err() { return Err(E::cancelled()); },
            }
        }
    }

    pub async fn activity<T, E: WorkflowFailure>(
        &self,
        name: &'static str,
        future: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        self.checkpoint::<E>().await?;
        *self.status.lock().unwrap() = WorkflowStatus::Running {
            activity: Some(name),
        };
        self.changed.notify_one();
        // Never select cancellation against a provider Future: dropping it is
        // not proof that an RPC or storage write has stopped.
        let result = future.await;
        *self.status.lock().unwrap() = WorkflowStatus::Running { activity: None };
        self.changed.notify_one();
        result
    }

    pub async fn sleep<E: WorkflowFailure>(&self, delay: Duration) -> Result<(), E> {
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(E::cancelled()),
            _ = tokio::time::sleep(delay) => Ok(()),
        }
    }
}

pub struct WorkflowCompletion<O, E> {
    id: u64,
    result: Result<O, E>,
}

pub struct Execution<I, O, E> {
    pub id: u64,
    pub command: CommandId,
    pub input: I,
    pub result: Option<Result<O, E>>,
    running: bool,
    recovery_block: bool,
    context: WorkflowContext,
}

impl<I, O, E> Execution<I, O, E> {
    pub fn status(&self) -> WorkflowStatus {
        if self.result.is_none() && self.context.cancel.is_cancelled() {
            WorkflowStatus::Cancelling
        } else {
            self.context.status.lock().unwrap().clone()
        }
    }
}

type Aggregate<O, E, R> = Box<dyn FnOnce(Vec<Result<O, E>>) -> Result<R, E> + Send>;
struct Batch<O, E, R> {
    ids: Vec<u64>,
    reply: CommandReply<R, E>,
    aggregate: Aggregate<O, E, R>,
}

/// One execution registry; batch records only correlate replies, and do not
/// have their own state machine. Resource policy belongs to the caller.
pub struct Workflows<I, O, E, R> {
    records: BTreeMap<u64, Execution<I, O, E>>,
    batches: BTreeMap<CommandId, Batch<O, E, R>>,
    next_id: u64,
    enabled: watch::Sender<bool>,
    changed: Arc<Notify>,
}

impl<I, O, E, R> Default for Workflows<I, O, E, R> {
    fn default() -> Self {
        Self {
            records: BTreeMap::new(),
            batches: BTreeMap::new(),
            next_id: 0,
            enabled: watch::channel(true).0,
            changed: Arc::new(Notify::new()),
        }
    }
}

impl<I: Clone + Send + 'static, O: Clone + Send + 'static, E: WorkflowFailure, R>
    Workflows<I, O, E, R>
{
    pub fn changed(&self) -> Arc<Notify> {
        self.changed.clone()
    }
    pub fn lifecycle(&self, state: Lifecycle) {
        self.enabled.send_if_modified(|value| {
            let enabled = matches!(state, Lifecycle::Running | Lifecycle::Draining);
            if *value == enabled {
                false
            } else {
                *value = enabled;
                true
            }
        });
    }
    pub fn executions(&self) -> impl Iterator<Item = &Execution<I, O, E>> {
        self.records.values()
    }
    pub fn is_idle(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn submit(
        &mut self,
        command: CommandId,
        inputs: Vec<I>,
        mut reply: CommandReply<R, E>,
        aggregate: impl FnOnce(Vec<Result<O, E>>) -> Result<R, E> + Send + 'static,
    ) {
        let mut ids = Vec::new();
        for input in inputs {
            self.next_id += 1;
            let id = self.next_id;
            let context = WorkflowContext {
                id,
                cancel: CancellationToken::new(),
                enabled: self.enabled.subscribe(),
                status: Arc::new(Mutex::new(WorkflowStatus::Waiting)),
                changed: self.changed.clone(),
            };
            self.records.insert(
                id,
                Execution {
                    id,
                    command,
                    input,
                    result: None,
                    running: false,
                    recovery_block: false,
                    context,
                },
            );
            ids.push(id);
        }
        reply.accepted(command);
        self.batches.insert(
            command,
            Batch {
                ids,
                reply,
                aggregate: Box::new(aggregate),
            },
        );
        self.reply_finished();
    }

    pub fn cancel_where(&mut self, predicate: impl Fn(&I) -> bool) {
        let ids: Vec<_> = self
            .records
            .values()
            .filter(|r| r.result.is_none() && predicate(&r.input))
            .map(|r| r.id)
            .collect();
        for id in ids {
            let record = &self.records[&id];
            record.context.cancel.cancel();
            if !record.running {
                self.finish(WorkflowCompletion {
                    id,
                    result: Err(E::cancelled()),
                });
            }
        }
        self.changed.notify_one();
    }

    pub fn start_ready<F, Fut>(
        &mut self,
        runs: &mut NewRuns<WorkflowCompletion<O, E>>,
        conflicts: impl Fn(&I, &I) -> bool,
        preempts: impl Fn(&I, &I) -> bool,
        start: F,
    ) where
        F: Fn(I, WorkflowContext) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<O, E>> + Send + 'static,
    {
        let waiting: Vec<_> = self
            .records
            .values()
            .filter(|r| !r.running && r.result.is_none())
            .map(|r| r.id)
            .collect();
        for id in waiting {
            let input = self.records[&id].input.clone();
            if self
                .records
                .values()
                .any(|r| r.recovery_block && conflicts(&input, &r.input))
            {
                self.finish(WorkflowCompletion {
                    id,
                    result: Err(E::recovery("resource requires reconciliation")),
                });
                continue;
            }
            for active in self
                .records
                .values()
                .filter(|r| r.running && r.result.is_none())
            {
                if preempts(&input, &active.input) {
                    active.context.cancel.cancel();
                }
            }
            if self.records.values().any(|r| {
                r.id != id
                    && r.result.is_none()
                    && (r.running || r.id < id)
                    && conflicts(&input, &r.input)
            }) {
                continue;
            }
            let record = self.records.get_mut(&id).unwrap();
            record.running = true;
            *record.context.status.lock().unwrap() = WorkflowStatus::Running { activity: None };
            let context = record.context.clone();
            let start = start.clone();
            runs.push(async move {
                let result = AssertUnwindSafe(async move { start(input, context).await })
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(E::recovery("workflow or activity panicked")));
                WorkflowCompletion { id, result }
            });
        }
    }

    pub fn finish(&mut self, completion: WorkflowCompletion<O, E>) {
        let record = self
            .records
            .get_mut(&completion.id)
            .expect("owned execution");
        let status = match &completion.result {
            Ok(_) => WorkflowStatus::Completed,
            Err(error) if error.needs_recovery() => WorkflowStatus::NeedsRecovery,
            Err(error) if error.is_cancelled() => WorkflowStatus::Cancelled,
            Err(_) => WorkflowStatus::Failed,
        };
        *record.context.status.lock().unwrap() = status;
        // A rejected waiter never acquired a resource and must not spread a
        // disk-local fence into a global one (e.g. a blocked allocation).
        record.recovery_block = record.running
            && completion
                .result
                .as_ref()
                .is_err_and(WorkflowFailure::needs_recovery);
        record.running = false;
        record.result = Some(completion.result);
        self.reply_finished();
    }

    fn reply_finished(&mut self) {
        let ready: Vec<_> = self
            .batches
            .iter()
            .filter(|(_, b)| b.ids.iter().all(|id| self.records[id].result.is_some()))
            .map(|(id, _)| *id)
            .collect();
        for id in ready {
            let mut batch = self.batches.remove(&id).unwrap();
            let results = batch
                .ids
                .iter()
                .map(|id| self.records[id].result.clone().unwrap())
                .collect();
            batch.reply.complete((batch.aggregate)(results));
        }
        // Bound ordinary terminal history. Recovery blocks and in-flight batch
        // members are never evicted. Queries may see the last 256 completions.
        let terminal: Vec<_> = self
            .records
            .values()
            .filter(|r| {
                r.result.is_some() && !r.recovery_block && !self.batches.contains_key(&r.command)
            })
            .map(|r| r.id)
            .collect();
        for id in terminal.iter().take(terminal.len().saturating_sub(256)) {
            self.records.remove(id);
        }
    }
}
