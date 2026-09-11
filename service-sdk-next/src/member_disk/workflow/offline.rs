use super::{append, append_error, cancelled, changes};
use crate::member_disk::operations::{DiskOperation, DiskResult};
use crate::member_disk::{
    DiskIoState, MemberDiskMutation, MemberDiskService, MemberDiskServiceError,
};
use futures_util::future::join_all;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_util::sync::CancellationToken;

impl MemberDiskService {
    /// Complete batch-offline flow. DOWN publication is one mandatory network
    /// action. Long-running isolation remains independent per disk.
    pub(super) async fn offline(
        &self,
        disk_operations: Vec<DiskOperation>,
        observed_at: u64,
    ) -> Vec<(DiskOperation, DiskResult)> {
        let (finished, active) = self.skip_removed(disk_operations);
        if active.is_empty() {
            return finished;
        }

        self.progress_all(&active, 10, "closing IO for the batch");
        if let Err(error) = self.set_disks_down(&active).await {
            return append_error(finished, active, error);
        }

        self.progress_all(&active, 25, "DOWN committed; isolating disks");
        let isolation = active.into_iter().map(|operation| async move {
            let result = self.isolate_disk(&operation, observed_at).await;
            (operation, result)
        });
        append(finished, join_all(isolation).await)
    }

    async fn isolate_disk(&self, operation: &DiskOperation, observed_at: u64) -> DiskResult {
        let (_, shrinking) = self.state(&operation.disk)?;
        if !shrinking {
            self.operations
                .progress(operation, 35, "waiting for the recovery window");
            self.wait_recovery_window(observed_at, &operation.cancel)
                .await?;
        }

        cancelled(&operation.cancel)?;
        self.commit(vec![(
            operation.disk.clone(),
            MemberDiskMutation::DisableAllocation,
        )])
        .await?;

        self.operations
            .progress(operation, 60, "evacuating VirtualDisk references");
        self.evacuate(operation).await?;

        cancelled(&operation.cancel)?;
        self.commit(vec![(operation.disk.clone(), MemberDiskMutation::Remove)])
            .await?;
        Ok(self.state(&operation.disk)?.0)
    }

    pub(super) async fn evacuate(
        &self,
        operation: &DiskOperation,
    ) -> Result<(), MemberDiskServiceError> {
        self.virtual_disks
            .evacuate(&operation.disk, &operation.cancel)
            .await
            .map_err(|error| {
                if operation.cancel.is_cancelled() {
                    MemberDiskServiceError::Cancelled
                } else {
                    MemberDiskServiceError::VirtualDisks(error)
                }
            })
    }

    /// Mandatory safety boundary: stop IO on user_dp, then durably publish the
    /// same DOWN capability in MemberDisk. Cancellation cannot split it.
    pub(super) async fn set_disks_down(
        &self,
        disk_operations: &[DiskOperation],
    ) -> Result<(), MemberDiskServiceError> {
        let changes = changes(disk_operations, DiskIoState::Down);
        loop {
            match self.pool_nodes.push_disk_states(changes.clone()).await {
                Ok(()) => break,
                Err(_) => tokio::time::sleep(self.config.mandatory_retry_delay).await,
            }
        }
        self.commit_all(
            disk_operations,
            MemberDiskMutation::SetIo(DiskIoState::Down),
        )
        .await
    }

    async fn wait_recovery_window(
        &self,
        observed_at: u64,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskServiceError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let elapsed = Duration::from_millis(now.saturating_sub(observed_at));
        let remaining = self.config.recovery_window.saturating_sub(elapsed);
        tokio::select! {
            _ = cancel.cancelled() => Err(MemberDiskServiceError::Cancelled),
            _ = tokio::time::sleep(remaining) => Ok(()),
        }
    }
}
