//! Ordered typed rules. Safety of object ownership and settlement stays in the runtime.
use super::*;
use workflow_foundation::AdmissionEvaluation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationClass {
    ObserveConnected,
    ObserveDisconnected,
    LoadNode,
    OfflineNode,
    OnlineNode,
    OnlineDisk,
    OfflineDisk,
    CreateVolume,
}
impl OperationKind {
    pub fn class(&self) -> OperationClass {
        match self {
            Self::ObserveNode {
                connected: true, ..
            } => OperationClass::ObserveConnected,
            Self::ObserveNode {
                connected: false, ..
            } => OperationClass::ObserveDisconnected,
            Self::LoadNode(_) => OperationClass::LoadNode,
            Self::OfflineNode { .. } => OperationClass::OfflineNode,
            Self::OnlineNode { .. } => OperationClass::OnlineNode,
            Self::OnlineDisk(_) => OperationClass::OnlineDisk,
            Self::OfflineDisk(_) => OperationClass::OfflineDisk,
            Self::CreateVolume(_) => OperationClass::CreateVolume,
        }
    }
    fn node_change(&self) -> Option<(&NodeId, u64)> {
        match self {
            Self::ObserveNode { id, sequence, .. }
            | Self::OfflineNode { id, sequence }
            | Self::OnlineNode { id, sequence } => Some((id, *sequence)),
            _ => None,
        }
    }
    fn disk(&self) -> Option<&DiskId> {
        match self {
            Self::OnlineDisk(id) | Self::OfflineDisk(id) => Some(id),
            _ => None,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OperationSelector {
    Class(OperationClass),
    NodeChange,
    Any,
}
impl OperationSelector {
    fn matches(self, kind: &OperationKind) -> bool {
        match self {
            Self::Class(class) => kind.class() == class,
            Self::NodeChange => kind.node_change().is_some(),
            Self::Any => true,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuleRelation {
    OverlappingObjects,
    SameDisk,
    SameNodeChange,
}
#[derive(Debug, Clone)]
pub struct CoordinationRule {
    pub name: String,
    pub incoming: OperationSelector,
    pub existing: OperationSelector,
    pub relation: RuleRelation,
    pub decision: AdmissionDecision,
}
impl CoordinationRule {
    pub fn new(
        name: impl Into<String>,
        incoming: OperationSelector,
        existing: OperationSelector,
        relation: RuleRelation,
        decision: AdmissionDecision,
    ) -> Self {
        Self {
            name: name.into(),
            incoming,
            existing,
            relation,
            decision,
        }
    }
    fn matches(&self, incoming: &PoolOperation, existing: &PoolOperation) -> bool {
        self.incoming.matches(&incoming.kind)
            && self.existing.matches(&existing.kind)
            && match self.relation {
                RuleRelation::OverlappingObjects => true,
                RuleRelation::SameDisk => incoming
                    .kind
                    .disk()
                    .zip(existing.kind.disk())
                    .is_some_and(|(a, b)| a == b),
                RuleRelation::SameNodeChange => incoming
                    .kind
                    .node_change()
                    .zip(existing.kind.node_change())
                    .is_some_and(|((a, _), (b, _))| a == b),
            }
    }
}
#[derive(Debug, Clone)]
pub struct PoolRules {
    /// First matching rule wins, after overlap and stale-node checks.
    pub rules: Vec<CoordinationRule>,
    pub duplicate: AdmissionDecision,
}
impl Default for PoolRules {
    fn default() -> Self {
        use OperationClass::*;
        use OperationSelector::{Class, NodeChange};
        use RuleRelation::*;
        let mut rules = vec![CoordinationRule::new(
            "newer-node-replaces-old",
            NodeChange,
            NodeChange,
            SameNodeChange,
            AdmissionDecision::CancelExisting,
        )];
        for (name, incoming, existing, relation) in [
            (
                "node-down-cancels-disk-up",
                OfflineNode,
                OnlineDisk,
                OverlappingObjects,
            ),
            (
                "node-down-cancels-disk-down",
                OfflineNode,
                OfflineDisk,
                OverlappingObjects,
            ),
            (
                "node-down-cancels-load",
                OfflineNode,
                LoadNode,
                OverlappingObjects,
            ),
            (
                "node-up-cancels-disk-down",
                OnlineNode,
                OfflineDisk,
                OverlappingObjects,
            ),
            (
                "node-up-cancels-load",
                OnlineNode,
                LoadNode,
                OverlappingObjects,
            ),
            (
                "connected-cancels-disk-down",
                ObserveConnected,
                OfflineDisk,
                OverlappingObjects,
            ),
            (
                "disk-down-cancels-disk-up",
                OfflineDisk,
                OnlineDisk,
                SameDisk,
            ),
            (
                "disconnected-cancels-load",
                ObserveDisconnected,
                LoadNode,
                OverlappingObjects,
            ),
        ] {
            rules.push(CoordinationRule::new(
                name,
                Class(incoming),
                Class(existing),
                relation,
                AdmissionDecision::CancelExisting,
            ));
        }
        Self {
            rules,
            duplicate: AdmissionDecision::Reject(
                "equivalent operation already in flight; observe its original Run".into(),
            ),
        }
    }
}
#[derive(Default)]
pub struct RulePolicy {
    rules: PoolRules,
}
impl RulePolicy {
    pub fn new(rules: PoolRules) -> Outcome<Self> {
        let mut names = std::collections::BTreeSet::new();
        for rule in &rules.rules {
            if rule.name.trim().is_empty()
                || rule.name.len() > 128
                || !names.insert(rule.name.clone())
            {
                return Err(crate::domain::reject(
                    "rule names must be nonempty, unique, and at most 128 bytes",
                ));
            }
        }
        Ok(Self { rules })
    }
    pub fn rules(&self) -> &PoolRules {
        &self.rules
    }
}
impl AdmissionPolicy<PoolOperation> for RulePolicy {
    fn decide(&self, incoming: &PoolOperation, existing: &PoolOperation) -> AdmissionDecision {
        self.evaluate(incoming, existing).decision
    }
    fn evaluate(&self, incoming: &PoolOperation, existing: &PoolOperation) -> AdmissionEvaluation {
        if !incoming
            .objects
            .iter()
            .any(|key| existing.objects.contains(key))
        {
            return AdmissionEvaluation::named("disjoint", AdmissionDecision::Queue);
        }
        if let Some(((a, next), (b, old))) =
            incoming.kind.node_change().zip(existing.kind.node_change())
            && a == b
            && next <= old
        {
            return AdmissionEvaluation::named(
                "stale-node-guard",
                AdmissionDecision::Reject(
                    "stale or conflicting node transition already in flight".into(),
                ),
            );
        }
        if incoming.kind == existing.kind {
            return AdmissionEvaluation::named("duplicate", self.rules.duplicate.clone());
        }
        for rule in &self.rules.rules {
            if rule.matches(incoming, existing) {
                return AdmissionEvaluation::named(&rule.name, rule.decision.clone());
            }
        }
        AdmissionEvaluation::named("default-queue", AdmissionDecision::Queue)
    }
}
