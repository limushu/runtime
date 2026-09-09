use super::{
    DiskIoState, DiskUuid, MemberDiskCommand, MemberDiskMutation, MemberDiskReply,
    MemberDiskService, MemberDiskServiceError, MemberDiskState,
};
use crate::service::CommandContext;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

enum Step {
    Transitioned(&'static str),
    Stable,
}

impl MemberDiskService {
    /// Drives ordinary async business steps. Cancellation is observed once,
    /// between completed transitions, rather than scattered through handlers.
    pub(super) async fn drive_to_stable(
        &self,
        command: &MemberDiskCommand,
        context: &CommandContext<DiskUuid>,
    ) -> Result<MemberDiskReply, MemberDiskServiceError> {
        loop {
            match self.reconcile_once(command, context).await? {
                Step::Stable => return self.reply_for(command.disk()),
                Step::Transitioned(action) => {
                    self.task_progress(
                        command.disk(),
                        progress(self.state(command.disk())?.0),
                        action,
                    );
                    context.milestone(action);
                    if context.cancellation().is_cancelled() {
                        return Err(MemberDiskServiceError::Cancelled);
                    }
                }
            }
        }
    }

    /// Selects and executes exactly one state transition, then returns. The
    /// next iteration rereads the private, committed MemberDisk object.
    async fn reconcile_once(
        &self,
        command: &MemberDiskCommand,
        context: &CommandContext<DiskUuid>,
    ) -> Result<Step, MemberDiskServiceError> {
        let disk = command.disk();
        let (state, shrinking) = self.state(disk)?;
        match state {
            MemberDiskState::UpActive => self.when_up_active(disk, command, shrinking).await,
            MemberDiskState::UpInactive => {
                self.when_up_inactive(disk, command, shrinking, context)
                    .await
            }
            MemberDiskState::DownActive => {
                self.when_down_active(disk, command, shrinking, context)
                    .await
            }
            MemberDiskState::DownInactive => {
                self.when_down_inactive(disk, command, shrinking, context)
                    .await
            }
            MemberDiskState::Removed => self.when_removed(disk, command, context).await,
        }
    }

    async fn when_up_active(
        &self,
        disk: &DiskUuid,
        command: &MemberDiskCommand,
        shrinking: bool,
    ) -> Result<Step, MemberDiskServiceError> {
        match command {
            MemberDiskCommand::DiskDown { .. } => self.set_disk_down(disk).await,
            MemberDiskCommand::DiskUp { .. } if !shrinking => Ok(Step::Stable),
            MemberDiskCommand::Shrink { .. } if !shrinking => self.mark_shrink(disk).await,
            MemberDiskCommand::DiskUp { .. } | MemberDiskCommand::Shrink { .. } => {
                self.disable_allocation(disk).await
            }
        }
    }

    async fn when_up_inactive(
        &self,
        disk: &DiskUuid,
        command: &MemberDiskCommand,
        shrinking: bool,
        context: &CommandContext<DiskUuid>,
    ) -> Result<Step, MemberDiskServiceError> {
        match command {
            MemberDiskCommand::DiskDown { .. } => self.set_disk_down(disk).await,
            MemberDiskCommand::DiskUp { .. } if !shrinking => {
                self.open_and_serve(disk, context).await
            }
            MemberDiskCommand::Shrink { .. } if !shrinking => self.mark_shrink(disk).await,
            MemberDiskCommand::DiskUp { .. } | MemberDiskCommand::Shrink { .. } => {
                if self.has_references(disk).await? {
                    self.evacuate(disk, context).await
                } else {
                    self.set_disk_down(disk).await
                }
            }
        }
    }

    async fn when_down_active(
        &self,
        disk: &DiskUuid,
        command: &MemberDiskCommand,
        shrinking: bool,
        context: &CommandContext<DiskUuid>,
    ) -> Result<Step, MemberDiskServiceError> {
        match command {
            MemberDiskCommand::Shrink { .. } if !shrinking => self.mark_shrink(disk).await,
            _ if shrinking => self.disable_allocation(disk).await,
            MemberDiskCommand::DiskDown { observed_at, .. } => {
                self.wait_then_disable(disk, *observed_at, context).await
            }
            MemberDiskCommand::DiskUp { .. } => self.open_and_serve(disk, context).await,
            MemberDiskCommand::Shrink { .. } => unreachable!(),
        }
    }

    async fn when_down_inactive(
        &self,
        disk: &DiskUuid,
        command: &MemberDiskCommand,
        shrinking: bool,
        context: &CommandContext<DiskUuid>,
    ) -> Result<Step, MemberDiskServiceError> {
        match command {
            MemberDiskCommand::Shrink { .. } if !shrinking => self.mark_shrink(disk).await,
            MemberDiskCommand::DiskUp { .. } if !shrinking => {
                self.open_and_serve(disk, context).await
            }
            MemberDiskCommand::DiskDown { .. }
            | MemberDiskCommand::DiskUp { .. }
            | MemberDiskCommand::Shrink { .. } => {
                if self.has_references(disk).await? {
                    self.evacuate(disk, context).await
                } else {
                    self.remove(disk).await
                }
            }
        }
    }

    async fn when_removed(
        &self,
        disk: &DiskUuid,
        command: &MemberDiskCommand,
        context: &CommandContext<DiskUuid>,
    ) -> Result<Step, MemberDiskServiceError> {
        match command {
            MemberDiskCommand::DiskUp { .. } => self.open_and_serve(disk, context).await,
            MemberDiskCommand::DiskDown { .. } | MemberDiskCommand::Shrink { .. } => {
                Ok(Step::Stable)
            }
        }
    }

    async fn set_disk_down(&self, disk: &DiskUuid) -> Result<Step, MemberDiskServiceError> {
        loop {
            match self.pool_nodes.set_disk_down(disk).await {
                Ok(()) => break,
                Err(_) => tokio::time::sleep(self.config.mandatory_retry_delay).await,
            }
        }
        self.commit(disk, MemberDiskMutation::SetIo(DiskIoState::Down))
            .await?;
        Ok(Step::Transitioned("stop IO and publish disk DOWN"))
    }

    async fn mark_shrink(&self, disk: &DiskUuid) -> Result<Step, MemberDiskServiceError> {
        self.commit(disk, MemberDiskMutation::RequestShrink).await?;
        Ok(Step::Transitioned("commit shrink intent"))
    }

    async fn disable_allocation(&self, disk: &DiskUuid) -> Result<Step, MemberDiskServiceError> {
        self.commit(disk, MemberDiskMutation::DisableAllocation)
            .await?;
        Ok(Step::Transitioned("disable new BLK allocation"))
    }

    async fn wait_then_disable(
        &self,
        disk: &DiskUuid,
        observed_at: u64,
        context: &CommandContext<DiskUuid>,
    ) -> Result<Step, MemberDiskServiceError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let elapsed = Duration::from_millis(now.saturating_sub(observed_at));
        let remaining = self.config.recovery_window.saturating_sub(elapsed);
        tokio::select! {
            _ = context.cancellation().cancelled() => return Err(MemberDiskServiceError::Cancelled),
            _ = tokio::time::sleep(remaining) => {}
        }
        self.disable_allocation(disk).await
    }

    async fn has_references(&self, disk: &DiskUuid) -> Result<bool, MemberDiskServiceError> {
        self.virtual_disks
            .has_references(disk)
            .await
            .map_err(MemberDiskServiceError::VirtualDisks)
    }

    async fn evacuate(
        &self,
        disk: &DiskUuid,
        context: &CommandContext<DiskUuid>,
    ) -> Result<Step, MemberDiskServiceError> {
        self.virtual_disks
            .evacuate(disk, context.cancellation())
            .await
            .map_err(|error| {
                if context.cancellation().is_cancelled() {
                    MemberDiskServiceError::Cancelled
                } else {
                    MemberDiskServiceError::VirtualDisks(error)
                }
            })?;
        Ok(Step::Transitioned("evacuate all BG references"))
    }

    async fn remove(&self, disk: &DiskUuid) -> Result<Step, MemberDiskServiceError> {
        self.commit(disk, MemberDiskMutation::Remove).await?;
        Ok(Step::Transitioned("remove MemberDisk"))
    }

    async fn open_and_serve(
        &self,
        disk: &DiskUuid,
        context: &CommandContext<DiskUuid>,
    ) -> Result<Step, MemberDiskServiceError> {
        self.pool_nodes
            .open_disk(disk, context.cancellation())
            .await
            .map_err(|error| {
                if context.cancellation().is_cancelled() {
                    MemberDiskServiceError::Cancelled
                } else {
                    MemberDiskServiceError::PoolNodes(error)
                }
            })?;
        self.pool_nodes
            .publish_disk_up(disk, context.cancellation())
            .await
            .map_err(|error| {
                if context.cancellation().is_cancelled() {
                    MemberDiskServiceError::Cancelled
                } else {
                    MemberDiskServiceError::PoolNodes(error)
                }
            })?;

        let mutation = if self.state(disk)?.0 == MemberDiskState::Removed {
            MemberDiskMutation::Rejoin
        } else {
            MemberDiskMutation::CompleteOnline
        };
        self.commit(disk, mutation).await?;
        Ok(Step::Transitioned("open disk and publish UP"))
    }

    fn reply_for(&self, disk: &DiskUuid) -> Result<MemberDiskReply, MemberDiskServiceError> {
        let (state, _) = self.state(disk)?;
        Ok(MemberDiskReply {
            disk: disk.clone(),
            state,
        })
    }
}

fn progress(state: MemberDiskState) -> u8 {
    match state {
        MemberDiskState::UpActive => 100,
        MemberDiskState::DownActive => 25,
        MemberDiskState::UpInactive => 50,
        MemberDiskState::DownInactive => 75,
        MemberDiskState::Removed => 100,
    }
}
