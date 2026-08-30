use crate::{
    CancelCause, ObjectKey, OperationId, RuntimeResult, ServiceId, ServiceRequest, WorkflowContext,
};
use async_trait::async_trait;
use std::fmt::Debug;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Footprint(Arc<[ObjectKey]>);

impl Footprint {
    pub fn one(key: ObjectKey) -> Self {
        Self(Arc::from([key]))
    }

    pub fn keys(&self) -> &[ObjectKey] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowMeta<K> {
    pub key: ObjectKey,
    pub footprint: Footprint,
    pub kind: K,
    pub label: Arc<str>,
}

impl<K> WorkflowMeta<K> {
    pub fn object(key: ObjectKey, kind: K, label: impl Into<Arc<str>>) -> Self {
        Self {
            footprint: Footprint::one(key.clone()),
            key,
            kind,
            label: label.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionClass<K> {
    Inline,
    Workflow(WorkflowMeta<K>),
}

/// Business-facing projection of the object registry. Runtime storage and
/// TaskAttempt identity remain private to the service container.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectActivity<K> {
    Idle,
    Pending {
        operation_id: OperationId,
        kind: K,
    },
    Running {
        operation_id: OperationId,
        kind: K,
    },
    Cancelling {
        current_operation_id: OperationId,
        current_kind: K,
        replacement_operation_id: OperationId,
        replacement_kind: K,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectDecision<R> {
    Start,
    JoinExisting,
    JoinPending,
    JoinReplacement,
    Queue,
    CancelThenStart { cause: CancelCause },
    Complete(R),
    Reject { reason: Arc<str> },
}

#[async_trait]
pub trait Service: Send + Sync + 'static {
    type Request: ServiceRequest;
    type WorkflowKind: Clone + Debug + Eq + Send + Sync + 'static;

    fn id(&self) -> ServiceId {
        <Self::Request as ServiceRequest>::service_id()
    }

    fn classify(&self, request: &Self::Request) -> ExecutionClass<Self::WorkflowKind>;

    fn decide(
        &self,
        _context: &WorkflowContext,
        _request: &Self::Request,
        _incoming: &WorkflowMeta<Self::WorkflowKind>,
        activity: &ObjectActivity<Self::WorkflowKind>,
    ) -> RuntimeResult<ObjectDecision<<Self::Request as ServiceRequest>::Response>> {
        Ok(match activity {
            ObjectActivity::Idle => ObjectDecision::Start,
            ObjectActivity::Pending { .. }
            | ObjectActivity::Running { .. }
            | ObjectActivity::Cancelling { .. } => ObjectDecision::Queue,
        })
    }

    async fn handle(
        &self,
        request: Self::Request,
        context: WorkflowContext,
    ) -> RuntimeResult<<Self::Request as ServiceRequest>::Response>;
}
