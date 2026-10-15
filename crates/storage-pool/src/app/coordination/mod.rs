//! Pool-specific application policy, deliberately outside the generic runtime.
use crate::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
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
    OfflineNode {
        id: NodeId,
        sequence: u64,
    },
    OnlineNode {
        id: NodeId,
        sequence: u64,
    },
    OnlineDisk(DiskId),
    OfflineDisk(DiskId),
    CreateVolume(VolumeId),
}
#[derive(Debug, Clone)]
pub struct PoolOperation {
    pub kind: OperationKind,
    pub objects: Vec<ObjectKey>,
}

mod rules;
pub use rules::{
    CoordinationRule, OperationClass, OperationSelector, PoolRules, RulePolicy, RuleRelation,
};

/// Backwards-compatible default policy; configurable applications use RulePolicy.
#[derive(Default)]
pub struct PoolConflictPolicy;
impl AdmissionPolicy<PoolOperation> for PoolConflictPolicy {
    fn decide(&self, incoming: &PoolOperation, existing: &PoolOperation) -> AdmissionDecision {
        RulePolicy::default().decide(incoming, existing)
    }
    fn evaluate(
        &self,
        incoming: &PoolOperation,
        existing: &PoolOperation,
    ) -> workflow_foundation::AdmissionEvaluation {
        RulePolicy::default().evaluate(incoming, existing)
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
    policy: Arc<dyn AdmissionPolicy<PoolOperation>>,
    routing: Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
}
impl PoolWorkflowCoordinator {
    pub(crate) fn new(
        runtime: CoordinatorClient,
        policy: Arc<dyn AdmissionPolicy<PoolOperation>>,
    ) -> Self {
        Self {
            runtime,
            policy,
            routing: Arc::default(),
        }
    }
    pub(crate) fn register_disk(&self, spec: &DiskSpec) -> Outcome<()> {
        let mut routing = self.routing.lock().unwrap();
        if let Some(old) = routing.get(&spec.id)
            && (old.node != spec.node
                || old.tier != spec.tier
                || old.capacity_blks != spec.capacity_blks)
        {
            return Err(crate::domain::reject("disk routing identity is immutable"));
        }
        routing.insert(spec.id.clone(), spec.clone());
        Ok(())
    }
    pub(crate) fn node_disks(&self, node: &NodeId) -> Vec<DiskSpec> {
        self.routing
            .lock()
            .unwrap()
            .values()
            .filter(|s| &s.node == node)
            .cloned()
            .collect()
    }
    pub fn tasks(&self) -> Vec<workflow_foundation::TaskSnapshot> {
        self.runtime.tasks()
    }
    pub fn task(
        &self,
        id: workflow_foundation::WorkflowId,
    ) -> Outcome<workflow_foundation::TaskSnapshot> {
        self.runtime.task(id)
    }
    pub fn observe_task(
        &self,
        id: workflow_foundation::WorkflowId,
    ) -> Outcome<workflow_foundation::TaskObserver> {
        self.runtime.observe_task(id)
    }
    pub fn suspend(&self, id: workflow_foundation::WorkflowId) -> Outcome<()> {
        self.runtime.suspend(id)
    }
    pub fn resume(&self, id: workflow_foundation::WorkflowId) -> Outcome<()> {
        self.runtime.resume(id)
    }
    pub fn cancel(&self, id: workflow_foundation::WorkflowId) -> Outcome<()> {
        self.runtime.cancel(id)
    }
    pub async fn wait_suspended(&self, id: workflow_foundation::WorkflowId) -> Outcome<()> {
        self.runtime.wait_suspended(id).await
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
