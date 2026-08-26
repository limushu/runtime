use std::{
    collections::HashMap,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
};

use tokio::sync::{broadcast, oneshot, watch};

use crate::{
    CancelReason, OperationId, RequestId, RuntimeError, ServiceKey, TaskEvent, TaskId, TaskKey,
    TaskSnapshot, TaskState, TraceContext, cancellation::CancellationScope,
};

use super::{ConflictPolicy, RequestContext, TaskContext, TaskMeta};

static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy)]
pub(crate) enum HandlerOutcome {
    Completed,
    Failed,
    Aborted,
}

struct TaskRecord {
    id: TaskId,
    meta: TaskMeta,
    request_id: RequestId,
    operation_id: OperationId,
    trace: TraceContext,
    context: TaskContext,
}

struct RunningTask {
    record: TaskRecord,
    state: TaskState,
}

struct PendingTask {
    record: TaskRecord,
    ready: oneshot::Sender<Result<TaskContext, RuntimeError>>,
}

struct ManagerState {
    running: HashMap<TaskId, RunningTask>,
    by_key: HashMap<TaskKey, TaskId>,
    pending: HashMap<TaskKey, PendingTask>,
    by_request: HashMap<RequestId, Vec<TaskId>>,
    primary_by_request: HashMap<RequestId, TaskId>,
    terminal_cancellation: HashMap<RequestId, CancelReason>,
}

impl ManagerState {
    fn new() -> Self {
        Self {
            running: HashMap::new(),
            by_key: HashMap::new(),
            pending: HashMap::new(),
            by_request: HashMap::new(),
            primary_by_request: HashMap::new(),
            terminal_cancellation: HashMap::new(),
        }
    }

    fn remember(&mut self, request_id: RequestId, task_id: TaskId) {
        self.by_request.entry(request_id).or_default().push(task_id);
        self.primary_by_request.entry(request_id).or_insert(task_id);
    }

    fn forget_active(&mut self, request_id: RequestId, task_id: TaskId) {
        if let Some(tasks) = self.by_request.get_mut(&request_id) {
            tasks.retain(|current| *current != task_id);
            if tasks.is_empty() {
                self.by_request.remove(&request_id);
            }
        }
    }
}

pub struct ServiceTaskManager<K: ServiceKey> {
    service: K,
    state: Mutex<ManagerState>,
    snapshots: watch::Sender<Vec<TaskSnapshot<K>>>,
    events: broadcast::Sender<TaskEvent<K>>,
}

impl<K: ServiceKey> ServiceTaskManager<K> {
    pub fn new(service: K) -> Self {
        let (snapshots, _) = watch::channel(Vec::new());
        let (events, _) = broadcast::channel(256);
        Self {
            service,
            state: Mutex::new(ManagerState::new()),
            snapshots,
            events,
        }
    }

    pub async fn create_new_task(
        &self,
        request: &RequestContext,
        meta: TaskMeta,
        conflict: ConflictPolicy,
    ) -> Result<TaskContext, RuntimeError> {
        let task_id = TaskId(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed));
        let cancellation = request
            .task
            .as_ref()
            .map(|parent| parent.cancellation.child())
            .unwrap_or_else(CancellationScope::root);
        let context = TaskContext::new(task_id, meta.key.clone(), cancellation);
        let record = TaskRecord {
            id: task_id,
            meta,
            request_id: request.request_id,
            operation_id: request.operation_id,
            trace: request.trace,
            context: context.clone(),
        };

        let mut emitted = Vec::new();
        let acquisition = {
            let mut state = self.state.lock().expect("task manager poisoned");
            if let Some(running_id) = state.by_key.get(&record.meta.key).copied() {
                match conflict {
                    ConflictPolicy::Reject => {
                        return Err(RuntimeError::TaskAlreadyRunning(record.meta.key));
                    }
                    ConflictPolicy::Replace(reason) => {
                        if let Some(running) = state.running.get_mut(&running_id) {
                            running.record.context.cancellation.cancel(reason);
                            running.state = TaskState::Cancelling;
                            emitted.push(self.snapshot(&running.record, TaskState::Cancelling));
                        }

                        if let Some(old) = state.pending.remove(&record.meta.key) {
                            let old_reason =
                                CancelReason::requested("replaced while waiting for task slot");
                            old.record.context.cancellation.cancel(old_reason.clone());
                            let _ = old
                                .ready
                                .send(Err(RuntimeError::TaskCancelled(old_reason.clone())));
                            state.forget_active(old.record.request_id, old.record.id);
                            state
                                .terminal_cancellation
                                .insert(old.record.request_id, old_reason);
                            emitted.push(self.snapshot(&old.record, TaskState::Cancelled));
                        }

                        let (ready, waiting) = oneshot::channel();
                        state.remember(record.request_id, record.id);
                        emitted.push(self.snapshot(&record, TaskState::Queued));
                        state
                            .pending
                            .insert(record.meta.key.clone(), PendingTask { record, ready });
                        Acquisition::Waiting(waiting)
                    }
                }
            } else {
                state.remember(record.request_id, record.id);
                state.by_key.insert(record.meta.key.clone(), record.id);
                emitted.push(self.snapshot(&record, TaskState::Running));
                state.running.insert(
                    record.id,
                    RunningTask {
                        record,
                        state: TaskState::Running,
                    },
                );
                Acquisition::Ready(context)
            }
        };
        self.publish(emitted);

        match acquisition {
            Acquisition::Ready(context) => Ok(context),
            Acquisition::Waiting(waiting) => waiting
                .await
                .map_err(|_| RuntimeError::ChannelClosed("task acquisition".into()))?,
        }
    }

    pub fn cancel(&self, task_id: TaskId, reason: CancelReason) -> Result<(), RuntimeError> {
        let mut emitted = Vec::new();
        let result = {
            let mut state = self.state.lock().expect("task manager poisoned");
            if let Some(running) = state.running.get_mut(&task_id) {
                running.record.context.cancellation.cancel(reason);
                running.state = TaskState::Cancelling;
                emitted.push(self.snapshot(&running.record, TaskState::Cancelling));
                Ok(())
            } else {
                let pending_key = state
                    .pending
                    .iter()
                    .find_map(|(key, pending)| (pending.record.id == task_id).then(|| key.clone()));
                if let Some(key) = pending_key {
                    let pending = state.pending.remove(&key).expect("pending task exists");
                    pending.record.context.cancellation.cancel(reason.clone());
                    let _ = pending
                        .ready
                        .send(Err(RuntimeError::TaskCancelled(reason.clone())));
                    state.forget_active(pending.record.request_id, pending.record.id);
                    state
                        .terminal_cancellation
                        .insert(pending.record.request_id, reason);
                    emitted.push(self.snapshot(&pending.record, TaskState::Cancelled));
                    Ok(())
                } else {
                    Err(RuntimeError::TaskNotFound(task_id))
                }
            }
        };
        self.publish(emitted);
        result
    }

    pub(crate) fn abort_all(&self) {
        let mut emitted = Vec::new();
        {
            let mut state = self.state.lock().expect("task manager poisoned");
            let pending = std::mem::take(&mut state.pending);
            for (_, task) in pending {
                let _ = task.ready.send(Err(RuntimeError::ResponseDropped));
                state.forget_active(task.record.request_id, task.record.id);
                emitted.push(self.snapshot(&task.record, TaskState::Aborted));
            }
            let running = std::mem::take(&mut state.running);
            for (_, task) in running {
                emitted.push(self.snapshot(&task.record, TaskState::Aborted));
            }
            state.by_key.clear();
            state.by_request.clear();
        }
        self.publish(emitted);
    }

    pub(crate) fn finish_request(
        &self,
        request_id: RequestId,
        outcome: HandlerOutcome,
    ) -> Option<CancelReason> {
        let mut emitted = Vec::new();
        let cancellation = {
            let mut state = self.state.lock().expect("task manager poisoned");
            let mut cancellation = state.terminal_cancellation.remove(&request_id);
            let task_ids = state.by_request.remove(&request_id).unwrap_or_default();
            let mut released_keys = Vec::new();

            for task_id in task_ids {
                if let Some(running) = state.running.remove(&task_id) {
                    if state.by_key.get(&running.record.meta.key) == Some(&task_id) {
                        state.by_key.remove(&running.record.meta.key);
                    }
                    if cancellation.is_none() {
                        cancellation = running.record.context.cancellation_reason();
                    }
                    let terminal = terminal_state(outcome, cancellation.as_ref());
                    emitted.push(self.snapshot(&running.record, terminal));
                    released_keys.push(running.record.meta.key.clone());
                }
            }

            state.primary_by_request.remove(&request_id);
            for key in released_keys {
                self.activate_pending(&mut state, &key, &mut emitted);
            }
            cancellation
        };
        self.publish(emitted);
        cancellation
    }

    pub(crate) fn task_for_request(&self, request_id: RequestId) -> Option<TaskId> {
        self.state
            .lock()
            .expect("task manager poisoned")
            .primary_by_request
            .get(&request_id)
            .copied()
    }

    pub(crate) fn cancellation_for_request(&self, request_id: RequestId) -> Option<CancelReason> {
        let state = self.state.lock().expect("task manager poisoned");
        if let Some(reason) = state.terminal_cancellation.get(&request_id) {
            return Some(reason.clone());
        }
        state.by_request.get(&request_id).and_then(|tasks| {
            tasks.iter().find_map(|task_id| {
                state
                    .running
                    .get(task_id)
                    .and_then(|task| task.record.context.cancellation_reason())
            })
        })
    }

    pub fn snapshots(&self) -> Vec<TaskSnapshot<K>> {
        let state = self.state.lock().expect("task manager poisoned");
        let mut snapshots: Vec<_> = state
            .running
            .values()
            .map(|task| self.snapshot(&task.record, task.state))
            .chain(
                state
                    .pending
                    .values()
                    .map(|task| self.snapshot(&task.record, TaskState::Queued)),
            )
            .collect();
        snapshots.sort_by_key(|snapshot| snapshot.task_id.0);
        snapshots
    }

    pub fn len(&self) -> usize {
        let state = self.state.lock().expect("task manager poisoned");
        state.running.len() + state.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn watch_snapshots(&self) -> watch::Receiver<Vec<TaskSnapshot<K>>> {
        self.snapshots.subscribe()
    }

    pub(crate) fn events(&self) -> broadcast::Sender<TaskEvent<K>> {
        self.events.clone()
    }

    fn activate_pending(
        &self,
        state: &mut ManagerState,
        key: &TaskKey,
        emitted: &mut Vec<TaskSnapshot<K>>,
    ) {
        let Some(pending) = state.pending.remove(key) else {
            return;
        };
        let PendingTask { record, ready } = pending;
        let context = record.context.clone();
        let task_id = record.id;
        let request_id = record.request_id;
        state.by_key.insert(record.meta.key.clone(), task_id);
        emitted.push(self.snapshot(&record, TaskState::Running));
        state.running.insert(
            task_id,
            RunningTask {
                record,
                state: TaskState::Running,
            },
        );
        if ready.send(Ok(context)).is_err()
            && let Some(task) = state.running.remove(&task_id)
        {
            state.by_key.remove(&task.record.meta.key);
            state.forget_active(request_id, task_id);
            emitted.push(self.snapshot(&task.record, TaskState::Aborted));
        }
    }

    fn snapshot(&self, record: &TaskRecord, state: TaskState) -> TaskSnapshot<K> {
        TaskSnapshot {
            service: self.service.clone(),
            task_id: record.id,
            key: record.meta.key.clone(),
            label: record.meta.label.clone(),
            state,
            visibility: record.meta.visibility,
            request_id: record.request_id,
            operation_id: record.operation_id,
            trace: record.trace,
        }
    }

    fn publish(&self, emitted: Vec<TaskSnapshot<K>>) {
        if emitted.is_empty() {
            return;
        }
        for task in emitted {
            let _ = self.events.send(TaskEvent { task });
        }
        self.snapshots.send_replace(self.snapshots());
    }
}

enum Acquisition {
    Ready(TaskContext),
    Waiting(oneshot::Receiver<Result<TaskContext, RuntimeError>>),
}

fn terminal_state(outcome: HandlerOutcome, cancellation: Option<&CancelReason>) -> TaskState {
    if cancellation.is_some() {
        return TaskState::Cancelled;
    }
    match outcome {
        HandlerOutcome::Completed => TaskState::Completed,
        HandlerOutcome::Failed => TaskState::Failed,
        HandlerOutcome::Aborted => TaskState::Aborted,
    }
}
