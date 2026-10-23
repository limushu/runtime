//! Queue ownership, topology suspension and uncertain-result fences.
use super::*;
pub(super) struct Entry {
    pub(super) operation: PoolOperation,
    pub(super) control: RunControl,
    pub(super) start: Option<StartPermit>,
    pub(super) task_id: Option<TaskId>,
}
#[derive(Default)]
pub(super) struct State {
    pub(super) entries: BTreeMap<WorkflowId, Entry>,
    pub(super) fences: Vec<(Option<TaskId>, PoolOperation)>,
    pub(super) pending_tasks: BTreeMap<TaskId, PoolOperation>,
}
impl State {
    // Reap before every admission too: a terminal notification may still be in the mailbox.
    pub(super) fn reap(&mut self) {
        let terminal: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(id, e)| {
                matches!(e.control.state(), RunState::Completed | RunState::Failed(_))
                    .then_some(*id)
            })
            .collect();
        for id in terminal {
            let entry = self.entries.remove(&id).unwrap();
            if matches!(
                entry.control.state(),
                RunState::Failed(Failure::Uncertain(_))
            ) {
                self.fences.push((entry.task_id, entry.operation));
            }
        }
    }
    pub(super) fn inspect(
        &self,
        operation: &PoolOperation,
        recovery_task: Option<TaskId>,
    ) -> Outcome<Vec<ConflictMatch>> {
        if self
            .pending_tasks
            .iter()
            .any(|(id, pending)| Some(*id) != recovery_task && pending.overlaps(operation))
        {
            return Err(crate::support::reject(
                "unfinished task owns these objects; continue or cancel the original task",
            ));
        }
        let mut matches = Vec::new();
        for (id, entry) in &self.entries {
            let conflict = TaskCenter::conflict(operation, &entry.operation);
            if let ConflictDecision::Reject(reason) = conflict.decision {
                return Err(crate::support::reject(reason));
            }
            if conflict.decision != ConflictDecision::Independent {
                matches.push(ConflictMatch {
                    existing: *id,
                    conflict,
                });
            }
        }
        Ok(matches)
    }
    pub(super) fn snapshot(&self) -> SchedulingSnapshot {
        SchedulingSnapshot {
            queued: self.entries.values().filter(|e| e.start.is_some()).count(),
            running: self.entries.values().filter(|e| e.start.is_none()).count(),
            fenced: self.fences.len(),
        }
    }
    pub(super) fn pump(&mut self) {
        // All accepted topo tasks block shrink, including queued tasks. A topology fence
        // retains the pause until business reconciliation; terminal Uncertain is not release.
        let topo_pending = self
            .entries
            .values()
            .any(|e| e.operation.kind.is_topology())
            || self.fences.iter().any(|(_, f)| f.kind.is_topology())
            || self.pending_tasks.values().any(|p| p.kind.is_topology());
        for entry in self.entries.values() {
            if entry.operation.kind == OperationKind::Shrink {
                let _ = entry.control.set_system_pause(topo_pending);
            }
        }
        let owners: Vec<_> = self
            .entries
            .iter()
            .map(|(id, entry)| {
                (
                    *id,
                    entry.operation.clone(),
                    entry.start.is_none(),
                    entry.control.pause_acknowledged(),
                )
            })
            .collect();
        for (id, entry) in self.entries.iter_mut() {
            if entry.start.is_none() {
                continue;
            }
            if self
                .fences
                .iter()
                .any(|(_, f)| f.overlaps(&entry.operation))
            {
                entry
                    .start
                    .take()
                    .unwrap()
                    .reject(crate::support::reject("object requires reconciliation"));
                continue;
            }
            if entry.operation.kind == OperationKind::Shrink && topo_pending {
                continue;
            }
            let blocked = owners.iter().any(|(other_id, operation, started, paused)| {
                if other_id == id || (!started && other_id > id) {
                    return false;
                }
                if !operation.overlaps(&entry.operation) {
                    return false;
                }
                // Explicit priority exception. A queued shrink has no effects; a started
                // shrink can be bypassed ONLY after an actual safe-pause acknowledgement.
                if entry.operation.kind.is_topology() && operation.kind == OperationKind::Shrink {
                    return *started && !paused;
                }
                true
            });
            let recovered = self
                .pending_tasks
                .iter()
                .any(|(owner, p)| Some(*owner) != entry.task_id && p.overlaps(&entry.operation));
            if !blocked && !recovered {
                entry.start.take().unwrap().start();
                // Include this newly started owner when considering later queue entries.
                // Earlier queued owners already block later conflicts in `owners`.
            }
        }
    }
}
