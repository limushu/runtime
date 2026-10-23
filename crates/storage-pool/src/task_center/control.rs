//! User controls resolve the logical TaskId through TaskCenter, with optional audit flush.
//! Suspend/resume act on the current execution; reconstruction belongs to recovery.rs.
use super::*;

impl TaskCenter {
    pub(super) fn control(&self, id: TaskId) -> Outcome<RunControl> {
        if self.stop.is_cancelled() {
            return Err(Failure::Unavailable);
        }
        self.registry
            .lock()
            .unwrap()
            .get(id)?
            .control()
            .filter(|control| !matches!(control.state(), RunState::Completed | RunState::Failed(_)))
            .ok_or_else(|| {
                crate::support::reject(
                    "task has no live execution; inspect its status/recovery report",
                )
            })
    }
    pub fn suspend(&self, id: TaskId) -> Outcome<()> {
        self.control(id)?.pause()
    }
    pub fn resume(&self, id: TaskId) -> Outcome<()> {
        self.control(id)?.resume()
    }
    pub fn recheck(&self, id: TaskId) -> Outcome<()> {
        self.control(id)?.recheck();
        Ok(())
    }
    pub fn cancel(&self, id: TaskId) -> Outcome<()> {
        self.control(id)?.cancel();
        Ok(())
    }
    pub async fn wait_suspended(&self, id: TaskId) -> Outcome<()> {
        self.control(id)?.wait_paused().await
    }
    pub async fn suspend_task(&self, id: TaskId) -> Outcome<()> {
        self.suspend(id)?;
        self.flush_history().await
    }
    pub async fn resume_task(&self, id: TaskId) -> Outcome<()> {
        self.resume(id)?;
        self.flush_history().await
    }
    pub async fn cancel_task(&self, id: TaskId) -> Outcome<()> {
        if self.stop.is_cancelled() {
            return Err(Failure::Unavailable);
        }
        if let Ok(control) = self.control(id) {
            control.cancel();
            self.flush_history().await
        } else {
            let task = self.task(id)?;
            if task.execution.is_some()
                || !matches!(task.status, TaskStatus::Ready | TaskStatus::Paused)
            {
                return Err(crate::support::reject(
                    "task has ended or requires business reconciliation",
                ));
            }
            self.persistence()?.cancel_inactive(id).await?;
            let (reply, received) = oneshot::channel();
            self.scheduling_commands
                .send(SchedulingCommand::CancelInactive { task: id, reply })
                .await
                .map_err(|_| Failure::Unavailable)?;
            received.await.map_err(|_| Failure::Unavailable)
        }
    }
}
