//! Pool-specific application policy, deliberately outside the generic runtime.
use crate::*;
use std::sync::Arc;
use workflow_foundation::{
    AdmissionDecision, AdmissionPolicy, CoordinatorClient, CoordinatorSnapshot, ObjectKey, Workflow,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OperationKind {
    ObserveNode {
        id: NodeId,
        sequence: u64,
        connected: bool,
    },
    LoadNode(NodeId),
    OnlineDisk(DiskId),
    OfflineDisk(DiskId),
    CreateVolume(VolumeId),
}
#[derive(Debug, Clone)]
pub struct PoolOperation {
    pub kind: OperationKind,
    pub objects: Vec<ObjectKey>,
}

#[derive(Default)]
pub struct PoolConflictPolicy;
impl AdmissionPolicy<PoolOperation> for PoolConflictPolicy {
    fn decide(&self, incoming: &PoolOperation, existing: &PoolOperation) -> AdmissionDecision {
        use AdmissionDecision::*;
        use OperationKind::*;
        if !incoming
            .objects
            .iter()
            .any(|o| existing.objects.contains(o))
        {
            return Queue;
        }
        if incoming.kind == existing.kind {
            return Reject(
                "equivalent operation already in flight; observe its original Run".into(),
            );
        }
        match (&incoming.kind, &existing.kind) {
            (OfflineDisk(a), OnlineDisk(b)) if a == b => CancelExisting,
            (
                ObserveNode {
                    id: a,
                    sequence: next,
                    ..
                },
                ObserveNode {
                    id: b,
                    sequence: old,
                    ..
                },
            ) if a == b => {
                if next > old {
                    CancelExisting
                } else {
                    Reject("stale or conflicting node observation already in flight".into())
                }
            }
            (
                ObserveNode {
                    id,
                    connected: false,
                    ..
                },
                LoadNode(node),
            ) if id == node => CancelExisting,
            // Volume allocation/publication is not preempted by this first policy.
            // Normal object FIFO ensures it reaches a safe boundary before offline.
            _ => Queue,
        }
    }
}
pub(crate) trait PoolWorkflow: Workflow {
    fn operation(&self) -> PoolOperation;
}

/// Application coordination service: chooses policy and submits complete operations.
/// The runtime atomically applies that policy against accepted, nonterminal runs.
#[derive(Clone)]
pub struct PoolWorkflowCoordinator {
    runtime: CoordinatorClient,
    policy: Arc<PoolConflictPolicy>,
}
impl PoolWorkflowCoordinator {
    pub(crate) fn new(runtime: CoordinatorClient) -> Self {
        Self {
            runtime,
            policy: Arc::new(PoolConflictPolicy),
        }
    }
    pub fn snapshot(&self) -> CoordinatorSnapshot {
        self.runtime.snapshot()
    }
    pub(crate) async fn submit<W: PoolWorkflow>(&self, workflow: W) -> Outcome<Run<W::Output>> {
        let operation = workflow.operation();
        self.runtime
            .submit_with_policy(workflow, operation, self.policy.clone())
            .await
    }
}
