//! Read-only task and scheduling views.
use super::*;
use std::ops::Deref;
#[derive(Debug, Clone)]
pub struct ConflictMatch {
    pub existing: WorkflowId,
    pub conflict: Conflict,
}
/// Runtime observation, admission-time conflict explanations and live topology blockers.
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub execution: workflow_foundation::TaskSnapshot,
    pub conflicts: Vec<ConflictMatch>,
    /// Live, accepted topology tasks delaying this shrink (including queued topology).
    pub topology_blockers: Vec<WorkflowId>,
}
impl Deref for TaskSnapshot {
    type Target = workflow_foundation::TaskSnapshot;
    fn deref(&self) -> &Self::Target {
        &self.execution
    }
}
pub struct TaskObserver {
    pub(super) execution: workflow_foundation::TaskObserver,
    pub(super) conflicts: Vec<ConflictMatch>,
    pub(super) blockers: Blockers,
}
impl TaskObserver {
    pub fn snapshot(&self) -> TaskSnapshot {
        let execution = self.execution.snapshot();
        TaskSnapshot {
            topology_blockers: self
                .blockers
                .lock()
                .unwrap()
                .get(&execution.id)
                .cloned()
                .unwrap_or_default(),
            execution,
            conflicts: self.conflicts.clone(),
        }
    }
    pub async fn changed(&mut self) -> Outcome<TaskSnapshot> {
        let execution = self.execution.changed().await?;
        Ok(TaskSnapshot {
            topology_blockers: self
                .blockers
                .lock()
                .unwrap()
                .get(&execution.id)
                .cloned()
                .unwrap_or_default(),
            execution,
            conflicts: self.conflicts.clone(),
        })
    }
}
#[derive(Debug, Clone, Default)]
pub struct SchedulingSnapshot {
    pub queued: usize,
    pub running: usize,
    /// Logical ownership fences; only reconciliation can release these (not implemented).
    pub fenced: usize,
}
