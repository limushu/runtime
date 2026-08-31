use crate::observation::ObservationHub;
use crate::{CallId, ObjectKey, ObservationEvent, OperationId, ServiceId, TaskAttemptId};
use futures::future::BoxFuture;
use std::sync::{Arc, OnceLock};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CancelCause(Arc<str>);

impl CancelCause {
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub struct CancellationScope {
    token: CancellationToken,
    cause: Arc<OnceLock<CancelCause>>,
    parent: Option<Arc<CancellationScope>>,
    linked: Option<Arc<CancellationScope>>,
}

impl CancellationScope {
    pub(crate) fn root() -> Self {
        Self {
            token: CancellationToken::new(),
            cause: Arc::new(OnceLock::new()),
            parent: None,
            linked: None,
        }
    }

    fn child(&self) -> Self {
        Self {
            token: self.token.child_token(),
            cause: Arc::new(OnceLock::new()),
            parent: Some(Arc::new(self.clone())),
            linked: None,
        }
    }

    /// Create a local cancellation scope that observes both the causal parent
    /// and an independent owner (for example, the service lifetime).
    pub(crate) fn linked_to(&self, owner: &CancellationScope) -> Self {
        Self {
            token: self.token.child_token(),
            cause: Arc::new(OnceLock::new()),
            parent: Some(Arc::new(self.clone())),
            linked: Some(Arc::new(owner.clone())),
        }
    }

    pub fn request(&self, cause: CancelCause) {
        let _ = self.cause.set(cause);
        self.token.cancel();
    }

    pub fn cause(&self) -> Option<&CancelCause> {
        self.cause
            .get()
            .or_else(|| self.parent.as_ref().and_then(|parent| parent.cause()))
            .or_else(|| self.linked.as_ref().and_then(|linked| linked.cause()))
    }

    pub fn is_requested(&self) -> bool {
        self.token.is_cancelled()
            || self
                .parent
                .as_ref()
                .is_some_and(|parent| parent.is_requested())
            || self
                .linked
                .as_ref()
                .is_some_and(|linked| linked.is_requested())
    }

    pub fn requested(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if self.is_requested() {
                return;
            }
            match (&self.parent, &self.linked) {
                (Some(parent), Some(linked)) => {
                    tokio::select! {
                        _ = self.token.cancelled() => {}
                        _ = parent.requested() => {}
                        _ = linked.requested() => {}
                    }
                }
                (Some(parent), None) => {
                    tokio::select! {
                        _ = self.token.cancelled() => {}
                        _ = parent.requested() => {}
                    }
                }
                (None, Some(linked)) => {
                    tokio::select! {
                        _ = self.token.cancelled() => {}
                        _ = linked.requested() => {}
                    }
                }
                (None, None) => self.token.cancelled().await,
            }
        })
    }
}

/// Causal and cancellation context carried across service calls.
///
/// It deliberately contains no business phase or status.
#[derive(Debug, Clone)]
pub struct WorkflowContext {
    operation_id: OperationId,
    call_id: CallId,
    parent_call_id: Option<CallId>,
    operation_owner: ServiceId,
    current_service: ServiceId,
    label: Arc<str>,
    cancellation: CancellationScope,
    task_attempt_id: Option<TaskAttemptId>,
    parent_task_attempt_id: Option<TaskAttemptId>,
    observation: Option<ObservationHub>,
}

impl WorkflowContext {
    pub fn root(owner: ServiceId, label: impl Into<Arc<str>>) -> Self {
        Self {
            operation_id: OperationId::next(),
            call_id: CallId::next(),
            parent_call_id: None,
            operation_owner: owner.clone(),
            current_service: owner,
            label: label.into(),
            cancellation: CancellationScope::root(),
            task_attempt_id: None,
            parent_task_attempt_id: None,
            observation: None,
        }
    }

    pub fn child(&self, owner: ServiceId, label: impl Into<Arc<str>>) -> Self {
        Self {
            operation_id: self.operation_id,
            call_id: CallId::next(),
            parent_call_id: Some(self.call_id),
            operation_owner: self.operation_owner.clone(),
            current_service: owner,
            label: label.into(),
            cancellation: self.cancellation.child(),
            task_attempt_id: None,
            parent_task_attempt_id: self.task_attempt_id.or(self.parent_task_attempt_id),
            observation: None,
        }
    }

    pub(crate) fn with_observation(mut self, observation: ObservationHub) -> Self {
        self.observation = Some(observation);
        self
    }

    pub(crate) fn with_task_attempt(mut self, task_attempt_id: TaskAttemptId) -> Self {
        self.task_attempt_id = Some(task_attempt_id);
        self
    }

    pub(crate) fn with_cancellation_owner(mut self, owner: &CancellationScope) -> Self {
        self.cancellation = self.cancellation.linked_to(owner);
        self
    }

    pub(crate) fn with_cancellation(mut self, cancellation: CancellationScope) -> Self {
        self.cancellation = cancellation;
        self
    }

    pub fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    pub fn call_id(&self) -> CallId {
        self.call_id
    }

    pub fn parent_call_id(&self) -> Option<CallId> {
        self.parent_call_id
    }

    pub fn operation_owner(&self) -> &ServiceId {
        &self.operation_owner
    }

    pub fn current_service(&self) -> &ServiceId {
        &self.current_service
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn task_attempt_id(&self) -> Option<TaskAttemptId> {
        self.task_attempt_id
    }

    pub fn parent_task_attempt_id(&self) -> Option<TaskAttemptId> {
        self.parent_task_attempt_id
    }

    pub fn cancellation(&self) -> &CancellationScope {
        &self.cancellation
    }

    /// Validate a business-defined stable boundary.
    ///
    /// Workflows call this after an awaited side effect has settled and before
    /// committing their next local transition. Cancellation stays out of the
    /// happy-path steps while stale workflows are prevented from writing.
    pub fn stable_boundary(&self) -> crate::RuntimeResult<()> {
        if self.cancellation.is_requested() {
            Err(crate::RuntimeError::Cancelled)
        } else {
            Ok(())
        }
    }

    pub fn milestone(&self, label: impl Into<Arc<str>>) {
        if let Some(observation) = &self.observation {
            observation.publish(ObservationEvent::Milestone {
                service_id: self.current_service.clone(),
                operation_id: self.operation_id,
                call_id: self.call_id,
                label: label.into(),
            });
        }
    }

    pub fn state_transition(
        &self,
        object: ObjectKey,
        from: impl Into<Arc<str>>,
        to: impl Into<Arc<str>>,
        reason: impl Into<Arc<str>>,
    ) {
        if let Some(observation) = &self.observation {
            observation.publish(ObservationEvent::StateTransition {
                service_id: self.current_service.clone(),
                operation_id: self.operation_id,
                call_id: self.call_id,
                object,
                from: from.into(),
                to: to.into(),
                reason: reason.into(),
            });
        }
    }
}
