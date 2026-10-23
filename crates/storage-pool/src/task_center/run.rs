use super::*;
use workflow_foundation::{ActivityHistory, Signal, TaskProgress};

/// Receipt for a logical task and its current, typed execution result.
/// Dropping the receipt does not cancel the task. TaskCenter remains its owner.
pub struct TaskRun<T> {
    pub control: TaskControl,
    execution: workflow_foundation::Run<T>,
}
impl<T> TaskRun<T> {
    pub(super) fn new(id: TaskId, execution: workflow_foundation::Run<T>) -> Self {
        Self {
            control: TaskControl {
                id,
                name: execution.control.name,
                execution: execution.control.clone(),
            },
            execution,
        }
    }
    pub fn id(&self) -> TaskId {
        self.control.id
    }
    pub fn execution_id(&self) -> WorkflowId {
        self.execution.id()
    }
    pub async fn result(&mut self) -> Outcome<T> {
        self.execution.result().await
    }
}

/// Control of this execution, carrying the logical TaskId. For control after
/// takeover, resolve TaskId through TaskCenter instead of retaining this handle.
#[derive(Clone)]
pub struct TaskControl {
    pub id: TaskId,
    pub name: &'static str,
    execution: RunControl,
}
impl TaskControl {
    pub fn state(&self) -> RunState {
        self.execution.state()
    }
    pub fn progress(&self) -> TaskProgress {
        self.execution.progress()
    }
    pub fn activities(&self) -> ActivityHistory {
        self.execution.activities()
    }
    pub fn pause_requested(&self) -> bool {
        self.execution.pause_requested()
    }
    pub fn user_pause_requested(&self) -> bool {
        self.execution.user_pause_requested()
    }
    pub fn system_pause_requested(&self) -> bool {
        self.execution.system_pause_requested()
    }
    pub fn cancellation_requested(&self) -> bool {
        self.execution.cancellation_requested()
    }
    pub fn pause_acknowledged(&self) -> bool {
        self.execution.pause_acknowledged()
    }
    pub fn pause(&self) -> Outcome<()> {
        self.execution.pause()
    }
    pub fn resume(&self) -> Outcome<()> {
        self.execution.resume()
    }
    pub fn cancel(&self) {
        self.execution.cancel();
    }
    pub fn recheck(&self) {
        self.execution.recheck();
    }
    pub async fn signal(&self, signal: Signal) -> Outcome<()> {
        self.execution.signal(signal).await
    }
    pub async fn wait_paused(&self) -> Outcome<()> {
        self.execution.wait_paused().await
    }
    pub async fn changed(&mut self) -> Outcome<RunState> {
        self.execution.changed().await
    }
}
