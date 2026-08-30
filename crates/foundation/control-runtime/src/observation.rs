use crate::{CallId, CancelCause, Footprint, OperationId, ServiceId, TaskAttemptId};
use std::sync::Arc;
use tokio::sync::{broadcast, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLifecycle {
    Running,
    Paused,
    Draining,
    Stopping,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Activity {
    Idle,
    Busy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceSnapshot {
    pub service_id: ServiceId,
    pub lifecycle: ServiceLifecycle,
    pub activity: Activity,
    pub running_futures: usize,
    pub active_tasks: usize,
    pub queued_requests: usize,
}

impl ServiceSnapshot {
    pub(crate) fn initial(service_id: ServiceId) -> Self {
        Self {
            service_id,
            lifecycle: ServiceLifecycle::Running,
            activity: Activity::Idle,
            running_futures: 0,
            active_tasks: 0,
            queued_requests: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    Completed,
    Cancelled,
    Failed(Arc<str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallOutcome {
    Completed,
    Cancelled,
    Failed(Arc<str>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObservationEvent {
    OperationStarted {
        service_id: ServiceId,
        operation_id: OperationId,
        call_id: CallId,
        label: Arc<str>,
    },
    OperationFinished {
        service_id: ServiceId,
        operation_id: OperationId,
        outcome: CallOutcome,
    },
    CallStarted {
        service_id: ServiceId,
        operation_id: OperationId,
        call_id: CallId,
        parent_call_id: Option<CallId>,
        parent_task_attempt_id: Option<TaskAttemptId>,
    },
    CallFinished {
        service_id: ServiceId,
        operation_id: OperationId,
        call_id: CallId,
        outcome: CallOutcome,
    },
    Milestone {
        service_id: ServiceId,
        operation_id: OperationId,
        call_id: CallId,
        label: Arc<str>,
    },
    StateTransition {
        service_id: ServiceId,
        operation_id: OperationId,
        call_id: CallId,
        object: crate::ObjectKey,
        from: Arc<str>,
        to: Arc<str>,
        reason: Arc<str>,
    },
    LifecycleChanged {
        service_id: ServiceId,
        lifecycle: ServiceLifecycle,
    },
    ActivityChanged {
        service_id: ServiceId,
        activity: Activity,
    },
    TaskStarted {
        service_id: ServiceId,
        task_attempt_id: TaskAttemptId,
        parent_task_attempt_id: Option<TaskAttemptId>,
        operation_id: OperationId,
        call_id: CallId,
        footprint: Footprint,
        kind: Arc<str>,
    },
    TaskCancelRequested {
        service_id: ServiceId,
        task_attempt_id: TaskAttemptId,
        operation_id: OperationId,
        cause: CancelCause,
    },
    TaskFinished {
        service_id: ServiceId,
        task_attempt_id: TaskAttemptId,
        operation_id: OperationId,
        outcome: TaskOutcome,
    },
    RequestJoined {
        service_id: ServiceId,
        operation_id: OperationId,
        footprint: Footprint,
    },
    RequestMerged {
        service_id: ServiceId,
        operation_id: OperationId,
        footprint: Footprint,
    },
    RequestQueued {
        service_id: ServiceId,
        footprint: Footprint,
    },
}

#[derive(Clone)]
pub struct ServiceObserver {
    snapshot_rx: watch::Receiver<ServiceSnapshot>,
    event_tx: broadcast::Sender<ObservationEvent>,
}

impl ServiceObserver {
    pub fn snapshot(&self) -> ServiceSnapshot {
        self.snapshot_rx.borrow().clone()
    }

    pub fn subscribe_snapshots(&self) -> watch::Receiver<ServiceSnapshot> {
        self.snapshot_rx.clone()
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<ObservationEvent> {
        self.event_tx.subscribe()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ObservationHub {
    snapshot_tx: watch::Sender<ServiceSnapshot>,
    event_tx: broadcast::Sender<ObservationEvent>,
}

impl ObservationHub {
    pub(crate) fn new(service_id: ServiceId) -> (Self, ServiceObserver) {
        let (snapshot_tx, snapshot_rx) =
            watch::channel(ServiceSnapshot::initial(service_id.clone()));
        let (event_tx, _) = broadcast::channel(256);
        (
            Self {
                snapshot_tx,
                event_tx: event_tx.clone(),
            },
            ServiceObserver {
                snapshot_rx,
                event_tx,
            },
        )
    }

    pub(crate) fn publish_snapshot(&self, snapshot: ServiceSnapshot) {
        self.snapshot_tx.send_replace(snapshot);
    }

    pub(crate) fn publish(&self, event: ObservationEvent) {
        let _ = self.event_tx.send(event);
    }
}
