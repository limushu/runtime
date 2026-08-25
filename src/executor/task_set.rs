use std::{
    collections::HashMap,
    sync::atomic::{AtomicU64, Ordering},
};

use futures::{
    FutureExt,
    future::{AbortHandle as FutureAbortHandle, Abortable, BoxFuture},
    stream::{FuturesUnordered, StreamExt},
};
use tokio::sync::{broadcast, oneshot};

use crate::{
    CancelReason, ConflictPolicy, RequestContext, RuntimeError, ServiceKey, TaskContext, TaskEvent,
    TaskExit, TaskId, TaskKey, TaskMeta, TaskSnapshot, TaskSpec, TaskState,
    cancellation::CancellationScope, task::TaskFactory,
};

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone)]
struct TaskInfo {
    id: TaskId,
    meta: TaskMeta,
    request: RequestContext,
}

struct TaskSlot {
    info: TaskInfo,
    state: TaskState,
    cancellation: CancellationScope,
    abort: FutureAbortHandle,
}

struct PendingTask<T, E> {
    info: TaskInfo,
    factory: TaskFactory<T, E>,
    completion: oneshot::Sender<TaskExit<T, E>>,
}

struct FinishedTask<T, E> {
    info: TaskInfo,
    exit: TaskExit<T, E>,
    completion: oneshot::Sender<TaskExit<T, E>>,
}

pub(super) struct TaskSet<K, T, E>
where
    K: ServiceKey,
    T: Send + 'static,
    E: Send + 'static,
{
    service: K,
    running: HashMap<TaskId, TaskSlot>,
    by_key: HashMap<TaskKey, TaskId>,
    pending: HashMap<TaskKey, PendingTask<T, E>>,
    futures: FuturesUnordered<BoxFuture<'static, FinishedTask<T, E>>>,
    events: broadcast::Sender<TaskEvent<K>>,
}

impl<K, T, E> TaskSet<K, T, E>
where
    K: ServiceKey,
    T: Send + 'static,
    E: Send + 'static,
{
    pub(super) fn new(service: K, events: broadcast::Sender<TaskEvent<K>>) -> Self {
        Self {
            service,
            running: HashMap::new(),
            by_key: HashMap::new(),
            pending: HashMap::new(),
            futures: FuturesUnordered::new(),
            events,
        }
    }

    pub(super) fn submit(
        &mut self,
        spec: TaskSpec<T, E>,
        request: RequestContext,
        control: crate::ControlHandle,
    ) -> Result<crate::TaskTicket<T, E>, RuntimeError> {
        let (meta, conflict, factory) = spec.into_parts();
        let id = TaskId(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed));
        let (completion, ticket) = oneshot::channel();
        let info = TaskInfo { id, meta, request };
        let task_ticket = crate::TaskTicket::new(id, control, ticket);

        if let Some(running_id) = self.by_key.get(&info.meta.key).copied() {
            match conflict {
                ConflictPolicy::Reject => {
                    return Err(RuntimeError::TaskAlreadyRunning(info.meta.key));
                }
                ConflictPolicy::Replace(reason) => {
                    if let Some(old_pending) = self.pending.remove(&info.meta.key) {
                        self.finish_pending(
                            old_pending,
                            CancelReason::requested("replaced while queued"),
                        );
                    }
                    self.request_cancel(running_id, reason, false)?;
                    self.emit(&info, TaskState::Queued);
                    self.pending.insert(
                        info.meta.key.clone(),
                        PendingTask {
                            info,
                            factory,
                            completion,
                        },
                    );
                    return Ok(task_ticket);
                }
            }
        }

        self.start(PendingTask {
            info,
            factory,
            completion,
        });
        Ok(task_ticket)
    }

    fn start(&mut self, pending: PendingTask<T, E>) {
        let PendingTask {
            info,
            factory,
            completion,
        } = pending;
        if info.request.cancellation.is_cancelled() {
            let reason = info
                .request
                .cancellation
                .reason()
                .unwrap_or(CancelReason::ParentCancelled);
            self.emit(&info, TaskState::Cancelled);
            let _ = completion.send(TaskExit::Cancelled(reason));
            return;
        }

        let context = TaskContext::new(info.id, info.meta.key.clone(), info.request.clone());
        let cancellation = info.request.cancellation.clone();
        let cancellation_for_result = cancellation.clone();
        let info_for_result = info.clone();
        let (abort, registration) = FutureAbortHandle::new_pair();
        let future = Abortable::new(factory(context), registration)
            .map(move |result| {
                let exit = match result {
                    Err(_) => TaskExit::Aborted,
                    Ok(Ok(_value)) if cancellation_for_result.is_cancelled() => {
                        TaskExit::Cancelled(
                            cancellation_for_result
                                .reason()
                                .unwrap_or(CancelReason::ParentCancelled),
                        )
                    }
                    Ok(Ok(value)) => TaskExit::Completed(value),
                    Ok(Err(_)) if cancellation_for_result.is_cancelled() => TaskExit::Cancelled(
                        cancellation_for_result
                            .reason()
                            .unwrap_or(CancelReason::ParentCancelled),
                    ),
                    Ok(Err(error)) => TaskExit::Failed(error),
                };
                FinishedTask {
                    info: info_for_result,
                    exit,
                    completion,
                }
            })
            .boxed();

        self.by_key.insert(info.meta.key.clone(), info.id);
        self.running.insert(
            info.id,
            TaskSlot {
                info: info.clone(),
                state: TaskState::Running,
                cancellation,
                abort,
            },
        );
        self.futures.push(future);
        self.emit(&info, TaskState::Running);
    }

    pub(super) fn request_cancel(
        &mut self,
        task_id: TaskId,
        reason: CancelReason,
        force: bool,
    ) -> Result<(), RuntimeError> {
        if let Some(slot) = self.running.get_mut(&task_id) {
            slot.cancellation.cancel(reason);
            slot.state = TaskState::Cancelling;
            if force {
                slot.abort.abort();
            }
            let info = slot.info.clone();
            self.emit(&info, TaskState::Cancelling);
            return Ok(());
        }

        let pending_key = self
            .pending
            .iter()
            .find_map(|(key, pending)| (pending.info.id == task_id).then(|| key.clone()));
        if let Some(key) = pending_key {
            let pending = self.pending.remove(&key).expect("pending task exists");
            self.finish_pending(pending, reason);
            return Ok(());
        }

        Err(RuntimeError::TaskNotFound(task_id))
    }

    pub(super) fn cancel_all(&mut self, reason: CancelReason, force: bool) {
        let pending = std::mem::take(&mut self.pending);
        for (_, task) in pending {
            self.finish_pending(task, reason.clone());
        }
        let ids: Vec<_> = self.running.keys().copied().collect();
        for id in ids {
            let _ = self.request_cancel(id, reason.clone(), force);
        }
    }

    pub(super) async fn next_finished(&mut self) -> Option<()> {
        let finished = self.futures.next().await?;
        self.running.remove(&finished.info.id);
        if self.by_key.get(&finished.info.meta.key) == Some(&finished.info.id) {
            self.by_key.remove(&finished.info.meta.key);
        }

        let state = match &finished.exit {
            TaskExit::Completed(_) => TaskState::Completed,
            TaskExit::Failed(_) => TaskState::Failed,
            TaskExit::Cancelled(_) => TaskState::Cancelled,
            TaskExit::Aborted => TaskState::Aborted,
        };
        self.emit(&finished.info, state);
        let key = finished.info.meta.key.clone();
        let _ = finished.completion.send(finished.exit);

        if let Some(next) = self.pending.remove(&key) {
            self.start(next);
        }
        Some(())
    }

    fn finish_pending(&self, pending: PendingTask<T, E>, reason: CancelReason) {
        self.emit(&pending.info, TaskState::Cancelled);
        let _ = pending.completion.send(TaskExit::Cancelled(reason));
    }

    pub(super) fn snapshots(&self) -> Vec<TaskSnapshot<K>> {
        let mut snapshots: Vec<_> = self
            .running
            .values()
            .map(|slot| self.snapshot(&slot.info, slot.state))
            .chain(
                self.pending
                    .values()
                    .map(|pending| self.snapshot(&pending.info, TaskState::Queued)),
            )
            .collect();
        snapshots.sort_by_key(|snapshot| snapshot.task_id.0);
        snapshots
    }

    fn snapshot(&self, info: &TaskInfo, state: TaskState) -> TaskSnapshot<K> {
        TaskSnapshot {
            service: self.service.clone(),
            task_id: info.id,
            key: info.meta.key.clone(),
            label: info.meta.label.clone(),
            state,
            visibility: info.meta.visibility,
            operation_id: info.request.operation_id,
            trace: info.request.trace.child(info.id),
        }
    }

    fn emit(&self, info: &TaskInfo, state: TaskState) {
        let _ = self.events.send(TaskEvent {
            task: self.snapshot(info, state),
        });
    }

    pub(super) fn len(&self) -> usize {
        self.running.len() + self.pending.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.running.is_empty() && self.pending.is_empty()
    }

    pub(super) fn has_running(&self) -> bool {
        !self.running.is_empty()
    }
}
