use super::{ServiceActivity, ServiceLifecycle, TaskControl, TaskOutcome};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{broadcast, watch};
use tokio_util::sync::CancellationToken;

/// Optional non-blocking adapter for persisting or exporting structured
/// runtime events. Panics are isolated so observability cannot change business
/// decisions.
pub trait RuntimeEventSink: Send + Sync + 'static {
    fn record(&self, event: &RuntimeEvent);
}

static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_TASK_ID: AtomicU64 = AtomicU64::new(1);

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(u64);

        impl $name {
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_type!(OperationId);
id_type!(RequestId);
id_type!(TaskId);

impl RequestId {
    pub(crate) fn next() -> Self {
        Self(NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed))
    }
}

impl TaskId {
    pub(crate) fn next() -> Self {
        Self(NEXT_TASK_ID.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationSpec {
    domain: String,
    kind: String,
    scope: String,
    summary: String,
    causes: Vec<OperationId>,
    trace: Option<TraceContext>,
}

impl OperationSpec {
    pub fn new(
        domain: impl Into<String>,
        kind: impl Into<String>,
        scope: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            domain: domain.into(),
            kind: kind.into(),
            scope: scope.into(),
            summary: summary.into(),
            causes: Vec::new(),
            trace: None,
        }
    }

    pub fn caused_by(mut self, cause: OperationId) -> Self {
        self.causes.push(cause);
        self
    }

    pub fn with_trace(mut self, trace: TraceContext) -> Self {
        self.trace = Some(trace);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
    trace_id: String,
}

impl TraceContext {
    pub fn new(trace_id: impl Into<String>) -> Self {
        Self {
            trace_id: trace_id.into(),
        }
    }

    pub fn trace_id(&self) -> &str {
        &self.trace_id
    }
}

/// Stable causal identity propagated across service calls. It contains no
/// business phase or workflow state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationContext {
    id: OperationId,
    domain: String,
    kind: String,
    scope: String,
    summary: String,
    causes: Vec<OperationId>,
    trace: TraceContext,
    observable: bool,
}

impl OperationContext {
    pub fn root(spec: OperationSpec) -> Self {
        let id = OperationId(NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed));
        Self {
            id,
            domain: spec.domain,
            kind: spec.kind,
            scope: spec.scope,
            summary: spec.summary,
            causes: spec.causes,
            trace: spec
                .trace
                .unwrap_or_else(|| TraceContext::new(format!("operation-{}", id.get()))),
            observable: true,
        }
    }

    pub(crate) fn transient() -> Self {
        let id = OperationId(NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed));
        Self {
            id,
            domain: "runtime".into(),
            kind: "request".into(),
            scope: "transient".into(),
            summary: "untracked request".into(),
            causes: Vec::new(),
            trace: TraceContext::new(format!("transient-{}", id.get())),
            observable: false,
        }
    }

    pub fn id(&self) -> OperationId {
        self.id
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn scope(&self) -> &str {
        &self.scope
    }

    pub fn summary(&self) -> &str {
        &self.summary
    }

    pub fn causes(&self) -> &[OperationId] {
        &self.causes
    }

    pub fn trace(&self) -> &TraceContext {
        &self.trace
    }

    pub fn is_observable(&self) -> bool {
        self.observable
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Running,
    Cancelling,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSnapshot {
    pub id: TaskId,
    pub operation_id: OperationId,
    pub trace_id: String,
    pub key: String,
    pub kind: String,
    pub summary: String,
    pub state: TaskState,
    pub cancellable: bool,
    pub progress: Option<u8>,
    pub milestone: Option<String>,
    pub blocked_on: Option<String>,
    pub started_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceSnapshot {
    pub revision: u64,
    pub service: String,
    pub domain: String,
    pub lifecycle: ServiceLifecycle,
    pub activity: ServiceActivity,
    pub queued_requests: usize,
    pub in_flight_requests: usize,
    pub accepted_requests: u64,
    pub completed_requests: u64,
    pub rejected_requests: u64,
    pub active_tasks: Vec<TaskSnapshot>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeEvent {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub service: String,
    pub kind: RuntimeEventKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeEventKind {
    OperationStarted(OperationContext),
    OperationFinished {
        operation: OperationId,
        error: Option<String>,
    },
    LifecycleChanged {
        from: ServiceLifecycle,
        to: ServiceLifecycle,
        reason: String,
    },
    ActivityChanged {
        from: ServiceActivity,
        to: ServiceActivity,
    },
    RequestAccepted {
        request: RequestId,
        operation: OperationId,
    },
    RequestRejected {
        operation: OperationId,
        reason: String,
    },
    RequestFinished {
        request: RequestId,
        operation: OperationId,
        error: Option<String>,
    },
    TaskStarted(TaskSnapshot),
    TaskProgress {
        task: TaskId,
        progress: u8,
    },
    TaskMilestone {
        task: TaskId,
        message: String,
    },
    TaskBlocked {
        task: TaskId,
        dependency: Option<String>,
    },
    TaskCancelRequested {
        task: TaskId,
        cause: String,
    },
    TaskFinished {
        task: TaskId,
        operation: OperationId,
        outcome: TaskOutcome,
    },
    StateTransition {
        task: TaskId,
        operation: OperationId,
        object: String,
        event: String,
        from: String,
        action: String,
        to: String,
    },
}

#[derive(Clone)]
pub struct ServiceObserver {
    hub: ObservationHub,
    receiver: watch::Receiver<ServiceSnapshot>,
}

impl ServiceObserver {
    pub fn snapshot(&self) -> ServiceSnapshot {
        self.receiver.borrow().clone()
    }

    pub async fn changed(&mut self) -> Result<ServiceSnapshot, watch::error::RecvError> {
        self.receiver.changed().await?;
        Ok(self.snapshot())
    }

    pub async fn wait_for(
        &mut self,
        mut predicate: impl FnMut(&ServiceSnapshot) -> bool,
    ) -> Result<ServiceSnapshot, watch::error::RecvError> {
        loop {
            let snapshot = self.snapshot();
            if predicate(&snapshot) {
                return Ok(snapshot);
            }
            self.receiver.changed().await?;
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<RuntimeEvent> {
        self.hub.inner.events.subscribe()
    }

    pub fn history(&self) -> Vec<RuntimeEvent> {
        self.hub
            .inner
            .state
            .lock()
            .expect("observation state poisoned")
            .history
            .iter()
            .cloned()
            .collect()
    }
}

#[derive(Clone)]
pub(crate) struct ObservationHub {
    inner: Arc<ObservationInner>,
}

struct ObservationInner {
    state: Mutex<ObservationState>,
    snapshots: watch::Sender<ServiceSnapshot>,
    events: broadcast::Sender<RuntimeEvent>,
    history_limit: usize,
    sink: Option<Arc<dyn RuntimeEventSink>>,
}

struct ObservationState {
    snapshot: ServiceSnapshot,
    tasks: HashMap<TaskId, ActiveTask>,
    history: VecDeque<RuntimeEvent>,
}

struct ActiveTask {
    snapshot: TaskSnapshot,
    cancellation: CancellationToken,
}

pub(crate) struct TaskRegistration {
    pub operation: OperationId,
    pub trace_id: String,
    pub key: String,
    pub kind: String,
    pub summary: String,
    pub cancellable: bool,
    pub cancellation: CancellationToken,
}

impl ObservationHub {
    pub(crate) fn new(
        service: impl Into<String>,
        domain: impl Into<String>,
        history_limit: usize,
        sink: Option<Arc<dyn RuntimeEventSink>>,
    ) -> (Self, ServiceObserver) {
        let snapshot = ServiceSnapshot {
            revision: 0,
            service: service.into(),
            domain: domain.into(),
            lifecycle: ServiceLifecycle::Initializing,
            activity: ServiceActivity::Idle,
            queued_requests: 0,
            in_flight_requests: 0,
            accepted_requests: 0,
            completed_requests: 0,
            rejected_requests: 0,
            active_tasks: Vec::new(),
            last_error: None,
        };
        let (snapshots, receiver) = watch::channel(snapshot.clone());
        let (events, _) = broadcast::channel(history_limit.max(16));
        let hub = Self {
            inner: Arc::new(ObservationInner {
                state: Mutex::new(ObservationState {
                    snapshot,
                    tasks: HashMap::new(),
                    history: VecDeque::new(),
                }),
                snapshots,
                events,
                history_limit: history_limit.max(1),
                sink,
            }),
        };
        let observer = ServiceObserver {
            hub: hub.clone(),
            receiver,
        };
        (hub, observer)
    }

    pub(crate) fn snapshot(&self) -> ServiceSnapshot {
        self.inner
            .state
            .lock()
            .expect("observation state poisoned")
            .snapshot
            .clone()
    }

    pub(crate) fn lifecycle(&self, to: ServiceLifecycle, reason: impl Into<String>) {
        let reason = reason.into();
        self.update(|state| {
            let from = state.snapshot.lifecycle;
            state.snapshot.lifecycle = to;
            RuntimeEventKind::LifecycleChanged { from, to, reason }
        });
    }

    pub(crate) fn fail(&self, error: impl Into<String>) {
        let error = error.into();
        let tasks: Vec<_> = self
            .inner
            .state
            .lock()
            .expect("observation state poisoned")
            .tasks
            .keys()
            .copied()
            .collect();
        for task in tasks {
            self.finish_task(task, TaskOutcome::Aborted);
        }
        self.update(|state| {
            let from = state.snapshot.lifecycle;
            state.snapshot.lifecycle = ServiceLifecycle::Failed;
            state.snapshot.activity = ServiceActivity::Idle;
            state.snapshot.queued_requests = 0;
            state.snapshot.in_flight_requests = 0;
            state.snapshot.last_error = Some(error.clone());
            RuntimeEventKind::LifecycleChanged {
                from,
                to: ServiceLifecycle::Failed,
                reason: error,
            }
        });
    }

    pub(crate) fn force_abort(&self, reason: impl Into<String>) {
        let reason = reason.into();
        let tasks: Vec<_> = self
            .inner
            .state
            .lock()
            .expect("observation state poisoned")
            .tasks
            .keys()
            .copied()
            .collect();
        for task in tasks {
            self.finish_task(task, TaskOutcome::Aborted);
        }
        self.update(|state| {
            let from = state.snapshot.lifecycle;
            state.snapshot.lifecycle = ServiceLifecycle::Stopped;
            state.snapshot.activity = ServiceActivity::Idle;
            state.snapshot.queued_requests = 0;
            state.snapshot.in_flight_requests = 0;
            RuntimeEventKind::LifecycleChanged {
                from,
                to: ServiceLifecycle::Stopped,
                reason,
            }
        });
    }

    pub(crate) fn task_control(&self, task: TaskId) -> Option<TaskControl> {
        self.inner
            .state
            .lock()
            .expect("observation state poisoned")
            .tasks
            .contains_key(&task)
            .then(|| TaskControl::new(task, self.clone()))
    }

    pub(crate) fn request_enqueued(&self) {
        self.update_queue_depth(|queued| queued.saturating_add(1));
    }

    pub(crate) fn request_dequeued(&self) {
        self.update_queue_depth(|queued| queued.saturating_sub(1));
    }

    fn update_queue_depth(&self, update: impl FnOnce(usize) -> usize) {
        let mut state = self.inner.state.lock().expect("observation state poisoned");
        let queued = update(state.snapshot.queued_requests);
        if state.snapshot.queued_requests == queued {
            return;
        }
        state.snapshot.queued_requests = queued;
        state.snapshot.revision += 1;
        let snapshot = state.snapshot.clone();
        drop(state);
        self.inner.snapshots.send_replace(snapshot);
    }

    pub(crate) fn request_accepted(
        &self,
        request: RequestId,
        operation: &OperationContext,
    ) -> Option<ServiceActivity> {
        if operation.is_observable() {
            let operation = operation.clone();
            self.update(|_| RuntimeEventKind::OperationStarted(operation));
        }
        self.update_activity(|state| {
            state.snapshot.accepted_requests += 1;
            state.snapshot.in_flight_requests += 1;
            RuntimeEventKind::RequestAccepted {
                request,
                operation: operation.id(),
            }
        })
    }

    pub(crate) fn request_rejected(&self, operation: OperationId, reason: impl Into<String>) {
        let reason = reason.into();
        self.update(|state| {
            state.snapshot.rejected_requests += 1;
            RuntimeEventKind::RequestRejected { operation, reason }
        });
    }

    pub(crate) fn request_finished(
        &self,
        request: RequestId,
        operation: OperationId,
        error: Option<String>,
        observable: bool,
    ) -> Option<ServiceActivity> {
        let operation_error = error.clone();
        let activity = self.update_activity(|state| {
            state.snapshot.in_flight_requests = state.snapshot.in_flight_requests.saturating_sub(1);
            state.snapshot.completed_requests += 1;
            if let Some(error) = &error {
                state.snapshot.last_error = Some(error.clone());
            }
            RuntimeEventKind::RequestFinished {
                request,
                operation,
                error,
            }
        });
        if observable {
            self.update(|_| RuntimeEventKind::OperationFinished {
                operation,
                error: operation_error,
            });
        }
        activity
    }

    pub(crate) fn start_task(&self, registration: TaskRegistration) -> TaskId {
        let task = TaskId::next();
        let snapshot = TaskSnapshot {
            id: task,
            operation_id: registration.operation,
            trace_id: registration.trace_id,
            key: registration.key,
            kind: registration.kind,
            summary: registration.summary,
            state: if registration.cancellation.is_cancelled() {
                TaskState::Cancelling
            } else {
                TaskState::Running
            },
            cancellable: registration.cancellable,
            progress: None,
            milestone: None,
            blocked_on: None,
            started_at_ms: now_ms(),
        };
        self.update(|state| {
            state.tasks.insert(
                task,
                ActiveTask {
                    snapshot: snapshot.clone(),
                    cancellation: registration.cancellation,
                },
            );
            RuntimeEventKind::TaskStarted(snapshot)
        });
        task
    }

    pub(crate) fn task_progress(&self, task: TaskId, progress: u8) {
        self.update(|state| {
            if let Some(active) = state.tasks.get_mut(&task) {
                active.snapshot.progress = Some(progress.min(100));
            }
            RuntimeEventKind::TaskProgress {
                task,
                progress: progress.min(100),
            }
        });
    }

    pub(crate) fn task_milestone(&self, task: TaskId, message: impl Into<String>) {
        let message = message.into();
        self.update(|state| {
            if let Some(active) = state.tasks.get_mut(&task) {
                active.snapshot.milestone = Some(message.clone());
            }
            RuntimeEventKind::TaskMilestone { task, message }
        });
    }

    pub(crate) fn task_blocked(&self, task: TaskId, dependency: Option<String>) {
        self.update(|state| {
            if let Some(active) = state.tasks.get_mut(&task) {
                active.snapshot.blocked_on = dependency.clone();
            }
            RuntimeEventKind::TaskBlocked { task, dependency }
        });
    }

    pub(crate) fn cancel_task(
        &self,
        task: TaskId,
        cause: impl Into<String>,
    ) -> Result<(), super::ControlError> {
        let cause = cause.into();
        let cancellation = {
            let state = self.inner.state.lock().expect("observation state poisoned");
            let Some(active) = state.tasks.get(&task) else {
                return Err(super::ControlError::TaskNotFound(task));
            };
            if !active.snapshot.cancellable {
                return Err(super::ControlError::TaskNotCancellable(task));
            }
            active.cancellation.clone()
        };

        self.update(|state| {
            if let Some(active) = state.tasks.get_mut(&task) {
                active.snapshot.state = TaskState::Cancelling;
            }
            RuntimeEventKind::TaskCancelRequested { task, cause }
        });
        cancellation.cancel();
        Ok(())
    }

    pub(crate) fn cancel_all(&self, cause: &str) {
        let tasks: Vec<_> = self
            .inner
            .state
            .lock()
            .expect("observation state poisoned")
            .tasks
            .keys()
            .copied()
            .collect();
        for task in tasks {
            let _ = self.cancel_task(task, cause);
        }
    }

    pub(crate) fn finish_task(&self, task: TaskId, outcome: TaskOutcome) {
        let operation = {
            let state = self.inner.state.lock().expect("observation state poisoned");
            let Some(active) = state.tasks.get(&task) else {
                return;
            };
            active.snapshot.operation_id
        };
        self.update(|state| {
            state.tasks.remove(&task);
            RuntimeEventKind::TaskFinished {
                task,
                operation,
                outcome,
            }
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn state_transition(
        &self,
        task: TaskId,
        operation: OperationId,
        object: impl Into<String>,
        event: impl Into<String>,
        from: impl Into<String>,
        action: impl Into<String>,
        to: impl Into<String>,
    ) {
        self.update(|_| RuntimeEventKind::StateTransition {
            task,
            operation,
            object: object.into(),
            event: event.into(),
            from: from.into(),
            action: action.into(),
            to: to.into(),
        });
    }

    fn update_activity(
        &self,
        update: impl FnOnce(&mut ObservationState) -> RuntimeEventKind,
    ) -> Option<ServiceActivity> {
        let before = self.snapshot().activity;
        self.update(update);
        let after = if self.snapshot().in_flight_requests == 0 {
            ServiceActivity::Idle
        } else {
            ServiceActivity::Busy
        };
        if before == after {
            return None;
        }
        self.update(|state| {
            state.snapshot.activity = after;
            RuntimeEventKind::ActivityChanged {
                from: before,
                to: after,
            }
        });
        Some(after)
    }

    fn update(&self, update: impl FnOnce(&mut ObservationState) -> RuntimeEventKind) {
        let (snapshot, event) = {
            let mut state = self.inner.state.lock().expect("observation state poisoned");
            let kind = update(&mut state);
            state.snapshot.revision += 1;
            state.snapshot.active_tasks = state
                .tasks
                .values()
                .map(|task| task.snapshot.clone())
                .collect();
            state.snapshot.active_tasks.sort_by_key(|task| task.id);
            let event = RuntimeEvent {
                sequence: state.snapshot.revision,
                timestamp_ms: now_ms(),
                service: state.snapshot.service.clone(),
                kind,
            };
            state.history.push_back(event.clone());
            while state.history.len() > self.inner.history_limit {
                state.history.pop_front();
            }
            (state.snapshot.clone(), event)
        };
        self.inner.snapshots.send_replace(snapshot);
        if let Some(sink) = &self.inner.sink {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| sink.record(&event)));
        }
        let _ = self.inner.events.send(event);
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}
