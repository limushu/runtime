//! Pool-local generic workflow scheduling and object exclusion. No domain imports.
mod context;
mod objects;
#[cfg(test)]
mod tests;
use crate::runtime::*;
pub(crate) use context::ActivityLease;
use context::ActivityTracker;
pub use context::WorkflowContext;
use futures_util::{FutureExt, future::BoxFuture};
pub use objects::{Access, ExecutionScope, ObjectClaim, ObjectKey};
use std::{
    collections::BTreeMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowError {
    Cancelled,
    Failed(String),
    NeedsRecovery(String),
}
impl std::fmt::Display for WorkflowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for WorkflowError {}
impl WorkflowFailure for WorkflowError {
    fn cancelled() -> Self {
        Self::Cancelled
    }
    fn recovery(reason: &'static str) -> Self {
        Self::NeedsRecovery(reason.into())
    }
    fn needs_recovery(&self) -> bool {
        matches!(self, Self::NeedsRecovery(_))
    }
    fn is_cancelled(&self) -> bool {
        matches!(self, Self::Cancelled)
    }
}
type Body =
    Box<dyn FnOnce(WorkflowContext) -> BoxFuture<'static, Result<(), WorkflowError>> + Send>;
/// One-shot definition. Typed outputs can use application channels; the manager
/// records completion without a disk/node-specific result enum.
pub struct WorkflowTask {
    name: String,
    claims: Vec<ObjectClaim>,
    body: Body,
}
impl WorkflowTask {
    pub fn new<F, Fut>(name: impl Into<String>, claims: Vec<ObjectClaim>, body: F) -> Self
    where
        F: FnOnce(WorkflowContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), WorkflowError>> + Send + 'static,
    {
        Self {
            name: name.into(),
            claims,
            body: Box::new(move |ctx| Box::pin(body(ctx))),
        }
    }
}
pub enum ProcessCommand {
    Start(WorkflowTask),
    Cancel(CommandId),
}
#[derive(Debug, Clone)]
pub struct WorkflowSnapshot {
    pub id: CommandId,
    pub name: String,
    pub objects: Vec<ObjectClaim>,
    pub status: WorkflowStatus,
    pub result: Option<Result<(), WorkflowError>>,
}
#[derive(Debug, Clone, Default)]
pub struct ProcessSnapshot {
    pub workflows: Vec<WorkflowSnapshot>,
}
struct Record {
    name: String,
    claims: Vec<ObjectClaim>,
    body: Option<Body>,
    reply: CommandReply<(), WorkflowError>,
    cancel: CancellationToken,
    status: Arc<Mutex<WorkflowStatus>>,
    running: bool,
    fenced: bool,
    result: Option<Result<(), WorkflowError>>,
}
pub struct ProcessCompletion {
    id: CommandId,
    result: Result<(), WorkflowError>,
}
pub struct ProcessManager {
    scope: ExecutionScope,
    records: BTreeMap<CommandId, Record>,
    enabled: watch::Sender<bool>,
    changed: Arc<Notify>,
}
impl ProcessManager {
    pub fn new(scope: ExecutionScope) -> Self {
        Self {
            scope,
            records: BTreeMap::new(),
            enabled: watch::channel(true).0,
            changed: Arc::new(Notify::new()),
        }
    }
    fn conflicts(a: &[ObjectClaim], b: &[ObjectClaim]) -> bool {
        a.iter().any(|a| b.iter().any(|b| a.conflicts(b)))
    }
    fn cancel(&mut self, id: CommandId) {
        if let Some(r) = self.records.get(&id)
            && r.result.is_none()
        {
            r.cancel.cancel();
            if !r.running {
                self.finish_running(ProcessCompletion {
                    id,
                    result: Err(WorkflowError::Cancelled),
                });
            }
        }
    }
}
impl Service for ProcessManager {
    type Command = ProcessCommand;
    type CommandReply = ();
    type Query = ();
    type QueryReply = ProcessSnapshot;
    type Completion = ProcessCompletion;
    type Error = WorkflowError;
    type Snapshot = ProcessSnapshot;
    fn query(&self, _: ()) -> Result<ProcessSnapshot, WorkflowError> {
        Ok(self.snapshot())
    }
    fn receive_command(&mut self, command: IncomingCommand<ProcessCommand, (), WorkflowError>) {
        let IncomingCommand {
            id,
            payload,
            mut reply,
        } = command;
        match payload {
            ProcessCommand::Start(task) => {
                reply.accepted(id);
                self.records.insert(
                    id,
                    Record {
                        name: task.name,
                        claims: task.claims,
                        body: Some(task.body),
                        reply,
                        cancel: CancellationToken::new(),
                        status: Arc::new(Mutex::new(WorkflowStatus::Waiting)),
                        running: false,
                        fenced: false,
                        result: None,
                    },
                );
            }
            ProcessCommand::Cancel(target) => {
                self.cancel(target);
                reply.accepted(id);
                reply.complete(Ok(()));
            }
        }
    }
    fn admit(&mut self) {}
    fn start_ready(&mut self, runs: &mut NewRuns<ProcessCompletion>) {
        let waiting: Vec<_> = self
            .records
            .iter()
            .filter(|(_, r)| !r.running && r.result.is_none())
            .map(|(id, _)| *id)
            .collect();
        for id in waiting {
            let claims = &self.records[&id].claims;
            if self
                .records
                .values()
                .any(|r| r.fenced && Self::conflicts(claims, &r.claims))
            {
                self.finish_running(ProcessCompletion {
                    id,
                    result: Err(WorkflowError::NeedsRecovery(
                        "object requires reconciliation".into(),
                    )),
                });
                continue;
            }
            if self.records.iter().any(|(other, r)| {
                *other != id
                    && r.result.is_none()
                    && (r.running || *other < id)
                    && Self::conflicts(claims, &r.claims)
            }) {
                continue;
            }
            let r = self.records.get_mut(&id).unwrap();
            r.running = true;
            *r.status.lock().unwrap() = WorkflowStatus::Running { activity: None };
            let tracker = ActivityTracker::default();
            let ctx = WorkflowContext {
                id,
                scope: self.scope.clone(),
                claims: r.claims.clone(),
                cancel: r.cancel.clone(),
                enabled: self.enabled.subscribe(),
                status: r.status.clone(),
                changed: self.changed.clone(),
                activities: tracker.clone(),
            };
            let body = r.body.take().unwrap();
            let cancel = r.cancel.clone();
            runs.push(async move {
                let result = AssertUnwindSafe(async move { body(ctx).await })
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| {
                        Err(WorkflowError::NeedsRecovery("workflow panicked".into()))
                    });
                // Independent workers outlive dropped response futures. Keep
                // objects until every accepted activity reaches a stable result.
                let uncertain = tracker.close_and_wait(&cancel).await;
                let result = if uncertain {
                    Err(WorkflowError::NeedsRecovery(
                        "activity result is uncertain".into(),
                    ))
                } else {
                    result
                };
                ProcessCompletion { id, result }
            });
        }
    }
    fn finish_running(&mut self, completion: ProcessCompletion) {
        let r = self.records.get_mut(&completion.id).unwrap();
        r.fenced = r.running
            && completion
                .result
                .as_ref()
                .is_err_and(WorkflowFailure::needs_recovery);
        r.running = false;
        r.body = None;
        *r.status.lock().unwrap() = match &completion.result {
            Ok(()) => WorkflowStatus::Completed,
            Err(WorkflowError::Cancelled) => WorkflowStatus::Cancelled,
            Err(WorkflowError::NeedsRecovery(_)) => WorkflowStatus::NeedsRecovery,
            Err(_) => WorkflowStatus::Failed,
        };
        r.result = Some(completion.result.clone());
        r.reply.complete(completion.result);
        let terminal: Vec<_> = self
            .records
            .iter()
            .filter(|(_, r)| r.result.is_some() && !r.fenced)
            .map(|(id, _)| *id)
            .collect();
        for id in terminal.iter().take(terminal.len().saturating_sub(256)) {
            self.records.remove(id);
        }
    }
    fn cancel_all(&mut self) {
        let ids: Vec<_> = self.records.keys().copied().collect();
        for id in ids {
            self.cancel(id);
        }
    }
    fn is_idle(&self) -> bool {
        self.records.values().all(|r| r.result.is_some())
    }
    fn lifecycle_changed(&mut self, lifecycle: Lifecycle) {
        let enabled = matches!(lifecycle, Lifecycle::Running | Lifecycle::Draining);
        self.enabled.send_if_modified(|v| {
            let changed = *v != enabled;
            *v = enabled;
            changed
        });
    }
    fn progress_notify(&self) -> Option<Arc<Notify>> {
        Some(self.changed.clone())
    }
    fn snapshot(&self) -> ProcessSnapshot {
        ProcessSnapshot {
            workflows: self
                .records
                .iter()
                .map(|(id, r)| WorkflowSnapshot {
                    id: *id,
                    name: r.name.clone(),
                    objects: r.claims.clone(),
                    status: if r.result.is_none() && r.cancel.is_cancelled() {
                        WorkflowStatus::Cancelling
                    } else {
                        r.status.lock().unwrap().clone()
                    },
                    result: r.result.clone(),
                })
                .collect(),
        }
    }
}
