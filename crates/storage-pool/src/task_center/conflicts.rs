//! Default storage policy: admission conflicts and directional scheduling relations.
//! Applications can inject another policy without changing the TaskCenter event loop.
use super::*;

impl PoolOperation {
    pub(crate) fn overlaps(&self, other: &Self) -> bool {
        matches!(
            self.kind,
            OperationKind::DeletePool | OperationKind::UnloadPool | OperationKind::LoadPool
        ) || matches!(
            other.kind,
            OperationKind::DeletePool | OperationKind::UnloadPool | OperationKind::LoadPool
        ) || self.objects.iter().any(|key| other.objects.contains(key))
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
            return conflict_result(Independent, "disjoint");
        }
        if let Some(((next_node, next), (old_node, old))) =
            incoming.kind.node_change().zip(existing.kind.node_change())
            && next_node == old_node
        {
            if next <= old {
                return conflict_result(
                    Reject("stale or conflicting node transition already in flight"),
                    "stale-node-guard",
                );
            }
            return conflict_result(CancelThenWait, "newer-node-replaces-old");
        }
        if incoming.kind == existing.kind {
            return conflict_result(
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
            _ => return conflict_result(Wait, "overlapping-objects"),
        };
        conflict_result(CancelThenWait, reason)
    }
}

fn conflict_result(decision: ConflictDecision, reason: &'static str) -> Conflict {
    Conflict { decision, reason }
}

/// Default storage policy. Applications can replace it without changing the event loop.
pub struct PoolSchedulingPolicy;
impl OperationKind {
    fn topology(&self) -> bool {
        !matches!(
            self,
            Self::Shrink(_)
                | Self::Custom(_)
                | Self::CreateVolume(_)
                | Self::DeleteVolume(_)
                | Self::RenamePool
        )
    }
}
impl SchedulingPolicy<PoolOperation> for PoolSchedulingPolicy {
    fn admission(&self, incoming: &PoolOperation, existing: &PoolOperation) -> Conflict {
        if matches!(incoming.kind, OperationKind::Shrink(_))
            || matches!(existing.kind, OperationKind::Shrink(_))
        {
            return Conflict {
                decision: ConflictDecision::Wait,
                reason: "shrink-coordination",
            };
        }
        TaskCenter::conflict(incoming, existing)
    }
    fn relation(&self, task: &PoolOperation, other: &PoolOperation) -> Relation {
        if matches!(task.kind, OperationKind::Shrink(_)) && other.kind.topology() {
            return Relation::YieldTo("topology");
        }
        if matches!(other.kind, OperationKind::Shrink(_)) && task.kind.topology() {
            return Relation::Independent;
        }
        if (matches!(task.kind, OperationKind::Shrink(_))
            && matches!(other.kind, OperationKind::Shrink(_)))
            || task.overlaps(other)
        {
            Relation::Exclusive
        } else {
            Relation::Independent
        }
    }
}
