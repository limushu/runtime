//! Pool business rules. The scheduler only interprets the returned decisions.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictDecision {
    Independent,
    Wait,
    CancelThenWait,
    /// Wait for safe suspension, retaining that suspension until the requester finishes.
    SuspendThenWait,
    Reject(&'static str),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    pub decision: ConflictDecision,
    pub reason: &'static str,
}
impl Conflict {
    pub fn new(decision: ConflictDecision, reason: &'static str) -> Self {
        Self { decision, reason }
    }
}
impl PoolOperation {
    pub(crate) fn overlaps(&self, other: &Self) -> bool {
        (self.kind == OperationKind::Shrink
            && (other.kind == OperationKind::Shrink || other.kind.is_topology()))
            || (other.kind == OperationKind::Shrink && self.kind.is_topology())
            || matches!(
                self.kind,
                OperationKind::DeletePool | OperationKind::UnloadPool | OperationKind::LoadPool
            )
            || matches!(
                other.kind,
                OperationKind::DeletePool | OperationKind::UnloadPool | OperationKind::LoadPool
            )
            || self.objects.iter().any(|key| other.objects.contains(key))
    }
}
impl OperationKind {
    pub(crate) fn is_topology(&self) -> bool {
        matches!(
            self,
            Self::Topology(_)
                | Self::ObserveNode { .. }
                | Self::LoadNode(_)
                | Self::OnlineNode { .. }
                | Self::OfflineNode { .. }
                | Self::OnlineDisk(_)
                | Self::OfflineDisk(_)
                | Self::AttachNode(_)
                | Self::DetachNode(_)
                | Self::RemoveDisk(_)
                | Self::CreateTier(_)
                | Self::DeleteTier(_)
                | Self::LoadPool
                | Self::UnloadPool
                | Self::DeletePool
        )
    }
    fn node_change(&self) -> Option<(&NodeId, u64)> {
        match self {
            Self::ObserveNode { id, sequence, .. }
            | Self::OnlineNode { id, sequence }
            | Self::OfflineNode { id, sequence } => Some((id, *sequence)),
            _ => None,
        }
    }
}
pub trait TaskRules: Send + Sync {
    fn conflict(&self, incoming: &PoolOperation, existing: &PoolOperation) -> Conflict;
}
#[derive(Default)]
pub struct PoolRules;
impl TaskRules for PoolRules {
    /// Inspect every rule before the scheduler changes any existing task.
    fn conflict(&self, incoming: &PoolOperation, existing: &PoolOperation) -> Conflict {
        use ConflictDecision::*;
        use OperationKind::*;
        // Pool-wide rules precede disjoint-object and duplicate detection.
        if incoming.kind == Shrink && existing.kind == Shrink {
            return Conflict::new(
                Reject("one unfinished shrink per pool"),
                "one-shrink-per-pool",
            );
        }
        if incoming.kind == Shrink && existing.kind.is_topology() {
            return Conflict::new(Wait, "shrink-waits-topology");
        }
        if incoming.kind.is_topology() && existing.kind == Shrink {
            return Conflict::new(SuspendThenWait, "topology-suspends-shrink");
        }
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
                Reject("equivalent operation already in flight; observe its original TaskRun"),
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
