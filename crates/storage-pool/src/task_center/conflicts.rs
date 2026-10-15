//! The single business-conflict entry point. No selectors, priorities or plugin policies.
//! Add a concrete match arm here and a scenario test alongside it.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictDecision {
    Independent,
    Wait,
    CancelThenWait,
    Reject(&'static str),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    pub decision: ConflictDecision,
    pub reason: &'static str,
}
impl Conflict {
    fn new(decision: ConflictDecision, reason: &'static str) -> Self {
        Self { decision, reason }
    }
}
impl PoolOperation {
    pub(crate) fn overlaps(&self, other: &Self) -> bool {
        self.objects.iter().any(|key| other.objects.contains(key))
    }
}
impl OperationKind {
    fn node_change(&self) -> Option<(&NodeId, u64)> {
        match self {
            Self::ObserveNode { id, sequence, .. }
            | Self::OnlineNode { id, sequence }
            | Self::OfflineNode { id, sequence } => Some((id, *sequence)),
            _ => None,
        }
    }
}
impl TaskCenter {
    /// Read top to bottom: object relation, freshness, duplicates, named business cases.
    /// Pure inspection; the actor applies decisions only after ALL conflicts are checked.
    pub fn conflict(incoming: &PoolOperation, existing: &PoolOperation) -> Conflict {
        use ConflictDecision::*;
        use OperationKind::*;
        if !incoming.overlaps(existing) {
            return Conflict::new(Independent, "disjoint");
        }
        if let Some(((next_node, next), (old_node, old))) =
            incoming.kind.node_change().zip(existing.kind.node_change())
            && next_node == old_node
        {
            if next <= old {
                return Conflict::new(
                    Reject("stale or conflicting node transition already in flight"),
                    "stale-node-guard",
                );
            }
            return Conflict::new(CancelThenWait, "newer-node-replaces-old");
        }
        if incoming.kind == existing.kind {
            return Conflict::new(
                Reject("equivalent operation already in flight; observe its original Run"),
                "duplicate",
            );
        }
        let reason = match (&incoming.kind, &existing.kind) {
            (OfflineNode { .. }, OnlineDisk(_)) => "node-down-cancels-disk-up",
            (OfflineNode { .. }, OfflineDisk(_)) => "node-down-cancels-disk-down",
            (OfflineNode { .. }, LoadNode(_)) => "node-down-cancels-load",
            (OnlineNode { .. }, OfflineDisk(_)) => "node-up-cancels-disk-down",
            (OnlineNode { .. }, LoadNode(_)) => "node-up-cancels-load",
            (
                ObserveNode {
                    connected: true, ..
                },
                OfflineDisk(_),
            ) => "connected-cancels-disk-down",
            (
                ObserveNode {
                    connected: false, ..
                },
                LoadNode(_),
            ) => "disconnected-cancels-load",
            (OfflineDisk(a), OnlineDisk(b)) if a == b => "disk-down-cancels-disk-up",
            _ => return Conflict::new(Wait, "overlapping-objects"),
        };
        Conflict::new(CancelThenWait, reason)
    }
}
