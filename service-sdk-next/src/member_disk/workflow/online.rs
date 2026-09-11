use super::{append_error, append_states, changes, partition_cancelled};
use crate::member_disk::operations::{DiskOperation, DiskResult};
use crate::member_disk::{
    DiskIoState, DiskOpenResult, DiskUuid, MemberDiskMutation, MemberDiskService,
    MemberDiskServiceError, MemberDiskState, PortError,
};
use std::collections::HashMap;

impl MemberDiskService {
    /// Two-phase batch online: open once, then publish UP only for disks that
    /// opened successfully. No per-disk network loop is hidden in this method.
    pub(super) async fn online(
        &self,
        disk_operations: Vec<DiskOperation>,
    ) -> Vec<(DiskOperation, DiskResult)> {
        let (mut finished, active) = self.prepare_online(disk_operations);
        if active.is_empty() {
            return finished;
        }

        self.progress_all(&active, 20, "opening disks on Pool nodes");
        let disks = active
            .iter()
            .map(|operation| operation.disk.clone())
            .collect();
        let open_results = match self.pool_nodes.open_disks(disks).await {
            Ok(results) => index_open_results(results),
            Err(error) => {
                return append_error(finished, active, MemberDiskServiceError::PoolNodes(error));
            }
        };

        let mut opened = Vec::new();
        for operation in active {
            match open_results.get(&operation.disk) {
                Some(Ok(())) if operation.cancel.is_cancelled() => {
                    finished.push((operation, Err(MemberDiskServiceError::Cancelled)))
                }
                Some(Ok(())) => opened.push(operation),
                Some(Err(error)) => finished.push((
                    operation,
                    Err(MemberDiskServiceError::PoolNodes(error.clone())),
                )),
                None => {
                    let error = MemberDiskServiceError::PoolNodes(PortError(format!(
                        "PoolNodes omitted the open result for {}",
                        operation.disk
                    )));
                    finished.push((operation, Err(error)));
                }
            }
        }
        if opened.is_empty() {
            return finished;
        }

        self.progress_all(&opened, 60, "publishing UP for opened disks");
        if let Err(error) = self.push_up(&opened).await {
            return append_error(finished, opened, error);
        }

        let (cancelled, opened) = partition_cancelled(opened);
        finished.extend(cancelled);
        let mutations = opened
            .iter()
            .map(|operation| {
                let mutation = match self.state(&operation.disk) {
                    Ok((MemberDiskState::Removed, _)) => MemberDiskMutation::Rejoin,
                    _ => MemberDiskMutation::CompleteOnline,
                };
                (operation.disk.clone(), mutation)
            })
            .collect();
        if let Err(error) = self.commit(mutations).await {
            return append_error(finished, opened, error);
        }

        self.progress_all(&opened, 100, "online completed");
        append_states(self, finished, opened)
    }

    fn prepare_online(
        &self,
        disk_operations: Vec<DiskOperation>,
    ) -> (Vec<(DiskOperation, DiskResult)>, Vec<DiskOperation>) {
        let mut finished = Vec::new();
        let mut active = Vec::new();
        for operation in disk_operations {
            match self.state(&operation.disk) {
                Ok((MemberDiskState::UpActive, false)) => {
                    finished.push((operation, Ok(MemberDiskState::UpActive)))
                }
                Ok((state, true)) if state != MemberDiskState::Removed => finished.push((
                    operation,
                    Err(MemberDiskServiceError::InvalidState(
                        "a disk with an accepted shrink intent must finish removal before rejoin"
                            .into(),
                    )),
                )),
                Ok(_) if operation.cancel.is_cancelled() => {
                    finished.push((operation, Err(MemberDiskServiceError::Cancelled)))
                }
                Ok(_) => active.push(operation),
                Err(error) => finished.push((operation, Err(error))),
            }
        }
        (finished, active)
    }

    async fn push_up(
        &self,
        disk_operations: &[DiskOperation],
    ) -> Result<(), MemberDiskServiceError> {
        self.pool_nodes
            .push_disk_states(changes(disk_operations, DiskIoState::Up))
            .await
            .map_err(MemberDiskServiceError::PoolNodes)
    }
}

fn index_open_results(results: Vec<DiskOpenResult>) -> HashMap<DiskUuid, Result<(), PortError>> {
    results
        .into_iter()
        .map(|result| (result.disk, result.result))
        .collect()
}
