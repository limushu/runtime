use super::{append_error, append_states, changes, partition_cancelled};
use crate::member_disk::operations::{DiskResult, OperationPermit};
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
        permits: Vec<OperationPermit>,
    ) -> Vec<(OperationPermit, DiskResult)> {
        let (mut finished, active) = self.prepare_online(permits);
        if active.is_empty() {
            return finished;
        }

        self.progress_all(&active, 20, "opening disks on Pool nodes");
        let disks = active.iter().map(|permit| permit.disk.clone()).collect();
        let open_results = match self.pool_nodes.open_disks(disks).await {
            Ok(results) => index_open_results(results),
            Err(error) => {
                return append_error(finished, active, MemberDiskServiceError::PoolNodes(error));
            }
        };

        let mut opened = Vec::new();
        for permit in active {
            match open_results.get(&permit.disk) {
                Some(Ok(())) if permit.cancel.is_cancelled() => {
                    finished.push((permit, Err(MemberDiskServiceError::Cancelled)))
                }
                Some(Ok(())) => opened.push(permit),
                Some(Err(error)) => finished.push((
                    permit,
                    Err(MemberDiskServiceError::PoolNodes(error.clone())),
                )),
                None => {
                    let error = MemberDiskServiceError::PoolNodes(PortError(format!(
                        "PoolNodes omitted the open result for {}",
                        permit.disk
                    )));
                    finished.push((permit, Err(error)));
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
            .map(|permit| {
                let mutation = match self.state(&permit.disk) {
                    Ok((MemberDiskState::Removed, _)) => MemberDiskMutation::Rejoin,
                    _ => MemberDiskMutation::CompleteOnline,
                };
                (permit.disk.clone(), mutation)
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
        permits: Vec<OperationPermit>,
    ) -> (Vec<(OperationPermit, DiskResult)>, Vec<OperationPermit>) {
        let mut finished = Vec::new();
        let mut active = Vec::new();
        for permit in permits {
            match self.state(&permit.disk) {
                Ok((MemberDiskState::UpActive, false)) => {
                    finished.push((permit, Ok(MemberDiskState::UpActive)))
                }
                Ok((state, true)) if state != MemberDiskState::Removed => finished.push((
                    permit,
                    Err(MemberDiskServiceError::InvalidState(
                        "a disk with an accepted shrink intent must finish removal before rejoin"
                            .into(),
                    )),
                )),
                Ok(_) if permit.cancel.is_cancelled() => {
                    finished.push((permit, Err(MemberDiskServiceError::Cancelled)))
                }
                Ok(_) => active.push(permit),
                Err(error) => finished.push((permit, Err(error))),
            }
        }
        (finished, active)
    }

    async fn push_up(&self, permits: &[OperationPermit]) -> Result<(), MemberDiskServiceError> {
        self.pool_nodes
            .push_disk_states(changes(permits, DiskIoState::Up))
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
