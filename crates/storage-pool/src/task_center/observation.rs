//! Logical task observation. Execution details are absent until recovery rebuilds it.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictMatch {
    pub existing: TaskId,
    pub conflict: Conflict,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitCondition {
    /// The predecessor must finish with a known result.
    Completion,
    /// A running predecessor must acknowledge a safe pause.
    SafePause,
    /// This task stays suspended until the requesting task finishes.
    Release,
    /// A recovered or uncertain predecessor needs reconstruction/reconciliation.
    Reconciliation,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskWait {
    pub task: TaskId,
    pub condition: WaitCondition,
    pub reason: &'static str,
}
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub id: TaskId,
    pub name: String,
    pub operation: PoolOperation,
    pub status: TaskStatus,
    pub last_error: Option<Failure>,
    pub execution: Option<workflow_foundation::ExecutionSnapshot>,
    pub conflicts: Vec<ConflictMatch>,
    /// Current unsatisfied conditions, including the causes of system suspension.
    pub waits: Vec<TaskWait>,
}
pub struct TaskObserver {
    pub(super) record: Arc<registry::TaskRecord>,
    pub(super) changes: watch::Receiver<u64>,
    pub(super) execution: Option<workflow_foundation::ExecutionObserver>,
    pub(super) execution_id: Option<WorkflowId>,
}
impl TaskObserver {
    pub fn snapshot(&self) -> TaskSnapshot {
        self.record.snapshot()
    }
    pub async fn changed(&mut self) -> Outcome<TaskSnapshot> {
        loop {
            tokio::select! {
                result = self.changes.changed() => {
                    result.map_err(|_| Failure::Unavailable)?;
                    let next = self.record.execution_observer();
                    let id = next.as_ref().map(|execution| execution.snapshot().id);
                    if id != self.execution_id {
                        self.execution_id = id;
                        self.execution = next;
                    }
                }
                result = async {
                    match &mut self.execution {
                        Some(execution) => execution.changed().await.map(|_| ()),
                        None => std::future::pending().await,
                    }
                } => {
                    if let Err(error) = result {
                        if self.snapshot().status.terminal() { return Err(error); }
                        // An uncertain execution ended; the logical task is still alive.
                        // Wait for reconciliation/reconstruction instead of polling it again.
                        self.execution = None;
                        continue;
                    }
                }
            }
            return Ok(self.snapshot());
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct SchedulingSnapshot {
    pub queued: usize,
    pub running: usize,
    pub recovering: usize,
    pub fenced: usize,
}
