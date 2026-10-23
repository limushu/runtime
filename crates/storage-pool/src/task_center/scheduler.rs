//! Queue ownership, topology suspension and uncertain-result fences.
use super::submission::Submission;
use super::*;
pub(super) struct Admitted {
    pub id: WorkflowId,
    pub task_id: Option<TaskId>,
    pub conflicts: Vec<ConflictMatch>,
    pub publish: Box<dyn FnOnce() + Send>,
}
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
    pub(super) restored_claims: BTreeMap<TaskId, PoolOperation>,
}
impl State {
    /// Admission is a synchronous scheduling transaction. No database work runs here.
    pub(super) fn admit(
        &mut self,
        submission: Submission,
        driver: &mut WorkflowDriver,
        closed: bool,
        limit: usize,
    ) -> Option<Admitted> {
        self.reap();
        let decision = if closed && !submission.administrative {
            Err(Failure::Unavailable)
        } else if self.entries.len() >= limit {
            Err(Failure::Busy)
        } else {
            self.inspect(&submission.operation, submission.restored_task)
        };
        let conflicts = match decision {
            Ok(matches) => matches,
            Err(error) => {
                (submission.accept)(Err(error));
                return None;
            }
        };
        // A replacement must exist before transferring claims or cancelling predecessors.
        let accepted = (submission.accept)(Ok(driver))?;
        self.reap();
        if let Some(task) = submission.restored_task {
            // Transfer a logical claim, not a running Future. No old execution is cancelled.
            self.restored_claims.remove(&task);
            self.fences.retain(|(owner, _)| *owner != Some(task));
        }
        let fenced = self
            .fences
            .iter()
            .any(|(_, f)| f.overlaps(&submission.operation));
        let id = accepted.control.id;
        self.entries.insert(
            id,
            Entry {
                operation: submission.operation,
                control: accepted.control,
                start: Some(accepted.start),
                task_id: accepted.task_id,
            },
        );
        for found in &conflicts {
            if !fenced
                && found.conflict.decision == ConflictDecision::CancelThenWait
                && let Some(entry) = self.entries.get(&found.existing)
            {
                entry.control.cancel();
            }
        }
        self.reap();
        self.pump();
        Some(Admitted {
            id,
            task_id: accepted.task_id,
            conflicts,
            publish: accepted.publish,
        })
    }
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
        restored_task: Option<TaskId>,
    ) -> Outcome<Vec<ConflictMatch>> {
        // Restored executions can join the queue together. pump keeps their start permits
        // blocked by any overlapping claim that has not been transferred or reconciled.
        if restored_task.is_none()
            && self
                .restored_claims
                .iter()
                .any(|(_, pending)| pending.overlaps(operation))
        {
            return Err(crate::support::reject(
                "unfinished task owns these objects; inspect its recovery report",
            ));
        }
        if restored_task.is_some()
            && self.fences.iter().any(|(owner, operation_fence)| {
                *owner != restored_task && operation_fence.overlaps(operation)
            })
        {
            return Err(crate::support::reject("object requires reconciliation"));
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
            || self.restored_claims.values().any(|p| p.kind.is_topology());
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
                .restored_claims
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
