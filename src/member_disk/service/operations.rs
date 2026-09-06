use super::{MemberDiskService, MemberDiskServiceError};
use crate::member_disk::{
    DiskIoState, DiskUuid, EpochMillis, MemberDiskState, MemberDiskUpdate, UserDpRequest,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

const SET_DISK_DOWN_RETRY_DELAY: Duration = Duration::from_millis(100);

impl MemberDiskService {
    /// Stops disk IO on every serviceable Pool node, then commits effective
    /// DOWN to MemberDisk metadata.
    ///
    /// The DOWN fact is carried by the active event and is not copied into
    /// MemberDisk metadata. Failed broadcasts are retried here because DOWN
    /// convergence is a MemberDisk policy. A conflicting event cannot cancel this boundary.
    /// Graceful service drain waits for it; force-aborting the root task drops
    /// the Future together with every other in-flight operation.
    pub(super) async fn set_disk_down(
        &self,
        disk: &DiskUuid,
    ) -> Result<(), MemberDiskServiceError> {
        let no_preemption = CancellationToken::new();

        loop {
            let result = self
                .pool_nodes
                .broadcast(
                    &no_preemption,
                    UserDpRequest::SetDiskState {
                        disk: disk.clone(),
                        state: DiskIoState::Down,
                    },
                )
                .await;

            match result {
                Ok(()) => break,
                Err(_) => tokio::time::sleep(SET_DISK_DOWN_RETRY_DELAY).await,
            }
        }

        self.commit_change(disk, MemberDiskUpdate::ApplyDown)
            .await?;
        Ok(())
    }

    pub(super) async fn wait_then_disable(
        &self,
        disk: &DiskUuid,
        observed_at: EpochMillis,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskServiceError> {
        let deadline = observed_at
            .get()
            .saturating_add(u64::try_from(self.recovery_window.as_millis()).unwrap_or(u64::MAX));
        let remaining = deadline.saturating_sub(now().get());

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(remaining)) => {}
            _ = cancel.cancelled() => return Err(MemberDiskServiceError::Cancelled),
        }

        self.disable_allocation(disk).await
    }

    pub(super) async fn disable_allocation(
        &self,
        disk: &DiskUuid,
    ) -> Result<(), MemberDiskServiceError> {
        self.commit_change(disk, MemberDiskUpdate::DisableAllocation)
            .await?;
        Ok(())
    }

    pub(super) async fn evacuate_down_disk(
        &self,
        disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskServiceError> {
        self.virtual_disks.evacuate(cancel, disk).await?;
        self.commit_change(disk, MemberDiskUpdate::Remove).await?;
        Ok(())
    }

    pub(super) async fn evacuate_online_disk(
        &self,
        disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskServiceError> {
        self.virtual_disks.evacuate(cancel, disk).await?;
        self.set_disk_down(disk).await?;
        self.commit_change(disk, MemberDiskUpdate::Remove).await?;
        Ok(())
    }

    pub(super) async fn open_and_serve(
        &self,
        disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskServiceError> {
        let update = match self.get_member(disk).await?.state() {
            MemberDiskState::Removed => MemberDiskUpdate::Rejoin,
            _ => MemberDiskUpdate::CompleteOnline,
        };

        self.pool_nodes
            .broadcast(cancel, UserDpRequest::OpenDisk(disk.clone()))
            .await?;
        self.pool_nodes
            .broadcast(
                cancel,
                UserDpRequest::SetDiskState {
                    disk: disk.clone(),
                    state: DiskIoState::Up,
                },
            )
            .await?;
        self.commit_change(disk, update).await?;
        Ok(())
    }

    pub(super) async fn request_shrink(
        &self,
        disk: &DiskUuid,
    ) -> Result<(), MemberDiskServiceError> {
        self.commit_change(disk, MemberDiskUpdate::RequestShrink)
            .await?;
        Ok(())
    }
}

fn now() -> EpochMillis {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    EpochMillis::new(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
}
