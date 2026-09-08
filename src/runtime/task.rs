use super::{ObservationHub, OperationContext, OperationId, TaskId};
use crate::runtime::observation::TaskRegistration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskMeta {
    key: String,
    kind: String,
    summary: String,
    cancellable: bool,
}

impl TaskMeta {
    pub fn new(
        key: impl Into<String>,
        kind: impl Into<String>,
        summary: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            kind: kind.into(),
            summary: summary.into(),
            cancellable: true,
        }
    }

    pub fn non_cancellable(mut self) -> Self {
        self.cancellable = false;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TaskOutcome {
    Completed,
    Cancelled,
    Failed(String),
    Aborted,
}

/// A small control reference stored by object-slot mechanisms. It does not
/// own or execute the Future.
#[derive(Clone)]
pub struct TaskControl {
    id: TaskId,
    hub: ObservationHub,
}

impl TaskControl {
    pub(crate) fn new(id: TaskId, hub: ObservationHub) -> Self {
        Self { id, hub }
    }

    pub fn id(&self) -> TaskId {
        self.id
    }

    pub fn request_cancel(&self, cause: impl Into<String>) {
        let _ = self.hub.cancel_task(self.id, cause);
    }
}

/// Observability and control attached to a portion of an already-running
/// request Future. The Task is metadata around work, not the work's owner.
pub struct TaskAttempt {
    id: TaskId,
    operation: OperationId,
    hub: ObservationHub,
    cancellation: CancellationToken,
    finished: bool,
}

impl TaskAttempt {
    pub(crate) fn start(
        hub: ObservationHub,
        operation: &OperationContext,
        meta: TaskMeta,
        cancellation: CancellationToken,
    ) -> Self {
        let id = hub.start_task(TaskRegistration {
            operation: operation.id(),
            trace_id: operation.trace().trace_id().to_owned(),
            key: meta.key,
            kind: meta.kind,
            summary: meta.summary,
            cancellable: meta.cancellable,
            cancellation: cancellation.clone(),
        });
        Self {
            id,
            operation: operation.id(),
            hub,
            cancellation,
            finished: false,
        }
    }

    pub fn id(&self) -> TaskId {
        self.id
    }

    pub fn control(&self) -> TaskControl {
        TaskControl::new(self.id, self.hub.clone())
    }

    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub fn progress(&self, percent: u8) {
        self.hub.task_progress(self.id, percent);
    }

    pub fn milestone(&self, message: impl Into<String>) {
        self.hub.task_milestone(self.id, message);
    }

    pub fn blocked_on(&self, dependency: impl Into<String>) {
        self.hub.task_blocked(self.id, Some(dependency.into()));
    }

    pub fn unblocked(&self) {
        self.hub.task_blocked(self.id, None);
    }

    pub fn transition(
        &self,
        object: impl Into<String>,
        event: impl Into<String>,
        from: impl Into<String>,
        action: impl Into<String>,
        to: impl Into<String>,
    ) {
        self.hub
            .state_transition(self.id, self.operation, object, event, from, action, to);
    }

    pub fn finish(mut self, outcome: TaskOutcome) {
        self.hub.finish_task(self.id, outcome);
        self.finished = true;
    }
}

impl Drop for TaskAttempt {
    fn drop(&mut self) {
        if !self.finished {
            self.hub.finish_task(self.id, TaskOutcome::Aborted);
        }
    }
}
