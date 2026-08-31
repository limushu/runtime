use crate::{ObjectKey, RuntimeResult, ServiceId, ServiceRequest, WorkflowContext};
use async_trait::async_trait;
use std::fmt::Debug;
use std::sync::Arc;

/// What should happen when every caller waiting for a workflow disappears.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrphanPolicy {
    /// Request cooperative cancellation and wait for the workflow to settle.
    Cancel,
    /// Keep running after admission. Useful for facts that must converge even
    /// when the producer disconnects.
    Continue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowMeta<K> {
    pub key: ObjectKey,
    pub kind: K,
    pub label: Arc<str>,
    pub orphan_policy: OrphanPolicy,
}

impl<K> WorkflowMeta<K> {
    pub fn object(key: ObjectKey, kind: K, label: impl Into<Arc<str>>) -> Self {
        Self {
            key,
            kind,
            label: label.into(),
            orphan_policy: OrphanPolicy::Cancel,
        }
    }

    pub fn continue_when_orphaned(mut self) -> Self {
        self.orphan_policy = OrphanPolicy::Continue;
        self
    }
}

/// The complete request plan returned by domain code.
///
/// For `Ensure`, the hidden actor cell starts an idle workflow, joins the same
/// workflow, or cooperatively replaces a different workflow. Domain code only
/// states the desired workflow and never inspects runtime activity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestPlan<K, R> {
    Inline,
    Ensure(WorkflowMeta<K>),
    Enqueue(WorkflowMeta<K>),
    Complete(R),
    Reject { reason: Arc<str> },
}

#[async_trait]
pub trait Service: Send + Sync + 'static {
    type Request: ServiceRequest;
    type WorkflowKind: Clone + Debug + Eq + Send + Sync + 'static;

    fn id(&self) -> ServiceId;

    fn plan(
        &self,
        request: &Self::Request,
    ) -> RuntimeResult<RequestPlan<Self::WorkflowKind, <Self::Request as ServiceRequest>::Response>>;

    async fn handle(
        &self,
        request: Self::Request,
        context: WorkflowContext,
    ) -> RuntimeResult<<Self::Request as ServiceRequest>::Response>;
}
