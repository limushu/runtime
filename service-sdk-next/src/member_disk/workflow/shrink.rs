use super::{append_error, append_states, partition_cancelled};
use crate::member_disk::operations::{DiskResult, OperationPermit};
use crate::member_disk::{MemberDiskMutation, MemberDiskService};
use futures_util::future::join_all;

impl MemberDiskService {
    /// Planned removal stays readable as a linear business flow. The only
    /// concurrency is the explicit `join_all` around independent evacuations.
    pub(super) async fn shrink(
        &self,
        permits: Vec<OperationPermit>,
    ) -> Vec<(OperationPermit, DiskResult)> {
        let (mut finished, active) = self.skip_removed(permits);
        if active.is_empty() {
            return finished;
        }

        // The intent is durable before this workflow may yield to a DOWN event.
        if let Err(error) = self
            .commit_all(&active, MemberDiskMutation::RequestShrink)
            .await
        {
            return append_error(finished, active, error);
        }

        let (cancelled, active) = partition_cancelled(active);
        finished.extend(cancelled);
        if active.is_empty() {
            return finished;
        }

        self.progress_all(&active, 20, "disabling new BLK allocation");
        if let Err(error) = self
            .commit_all(&active, MemberDiskMutation::DisableAllocation)
            .await
        {
            return append_error(finished, active, error);
        }

        self.progress_all(&active, 45, "evacuating VirtualDisk references");
        let evacuations = active.into_iter().map(|permit| async move {
            let result = self.evacuate(&permit).await;
            (permit, result)
        });
        let mut evacuated = Vec::new();
        for (permit, result) in join_all(evacuations).await {
            match result {
                Ok(()) => evacuated.push(permit),
                Err(error) => finished.push((permit, Err(error))),
            }
        }
        if evacuated.is_empty() {
            return finished;
        }

        self.progress_all(&evacuated, 75, "closing IO for evacuated disks");
        if let Err(error) = self.set_disks_down(&evacuated).await {
            return append_error(finished, evacuated, error);
        }
        if let Err(error) = self
            .commit_all(&evacuated, MemberDiskMutation::Remove)
            .await
        {
            return append_error(finished, evacuated, error);
        }

        self.progress_all(&evacuated, 100, "shrink completed");
        append_states(self, finished, evacuated)
    }
}
