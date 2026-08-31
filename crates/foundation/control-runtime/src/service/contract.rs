use crate::{CancelCause, ObjectKey, RuntimeResult, ServiceId, ServiceRequest, WorkflowContext};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestRoute<K> {
    /// A normal Future without object admission or managed-task bookkeeping.
    Untracked,
    Workflow(WorkflowMeta<K>),
}

/// The small, business-facing projection of one object's intent slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectActivity<K> {
    Idle,
    Busy {
        current_kind: K,
        replacement_kind: Option<K>,
    },
}

impl<K> ObjectActivity<K> {
    pub fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }

    pub fn target_kind(&self) -> Option<&K> {
        match self {
            Self::Idle => None,
            Self::Busy {
                current_kind,
                replacement_kind,
            } => replacement_kind.as_ref().or(Some(current_kind)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Admission<R> {
    Start,
    Join,
    Queue,
    Replace { cause: CancelCause },
    Complete(R),
    Reject { reason: Arc<str> },
}

#[async_trait]
pub trait Service: Send + Sync + 'static {
    type Request: ServiceRequest;
    type WorkflowKind: Clone + Debug + Eq + Send + Sync + 'static;

    fn id(&self) -> ServiceId;

    fn route(&self, request: &Self::Request) -> RequestRoute<Self::WorkflowKind>;

    fn admit(
        &self,
        _context: &WorkflowContext,
        _request: &Self::Request,
        activity: &ObjectActivity<Self::WorkflowKind>,
    ) -> RuntimeResult<Admission<<Self::Request as ServiceRequest>::Response>> {
        Ok(match activity {
            ObjectActivity::Idle => Admission::Start,
            ObjectActivity::Busy { .. } => Admission::Queue,
        })
    }

    async fn handle(
        &self,
        request: Self::Request,
        context: WorkflowContext,
    ) -> RuntimeResult<<Self::Request as ServiceRequest>::Response>;
}
