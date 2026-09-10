mod offline;
mod online;
mod shrink;

use super::operations::{DiskResult, OperationKind, OperationPermit, OperationWait, WaitReason};
use super::{
    DiskIoState, DiskStateChange, DiskUuid, MemberDiskMutation, MemberDiskOutcome, MemberDiskReply,
    MemberDiskService, MemberDiskServiceError, MemberDiskState,
};
use crate::service::CommandContext;
use futures_util::future::join_all;
use std::collections::{HashMap, HashSet};

impl MemberDiskService {
    pub(super) async fn run_disks(
        &self,
        operation: OperationKind,
        disks: Vec<DiskUuid>,
        observed_at: Option<u64>,
        context: &CommandContext<DiskUuid>,
    ) -> MemberDiskReply {
        let order = unique(disks);
        let mut pending = order.clone();
        let mut completed = HashMap::new();

        while !pending.is_empty() {
            let plan = self.operations.plan(
                &pending,
                operation,
                context.execution_id(),
                context.cancellation(),
            );
            context.milestone(format!(
                "{}: start {}, wait {}",
                operation.name(),
                plan.start.len(),
                plan.wait.len()
            ));

            let running = async {
                let finished = self.run_started(operation, plan.start, observed_at).await;
                finished
                    .into_iter()
                    .map(|(permit, result)| {
                        let disk = permit.disk.clone();
                        self.operations.finish(permit, result.clone());
                        (disk, result)
                    })
                    .collect::<Vec<_>>()
            };
            let (running, waiting) = tokio::join!(running, wait_for_operations(plan.wait));

            completed.extend(running);
            pending = Vec::new();
            for waited in waiting {
                match waited.reason {
                    WaitReason::Join => {
                        completed.insert(waited.disk, waited.result);
                    }
                    WaitReason::Retry => pending.push(waited.disk),
                }
            }
        }

        MemberDiskReply {
            outcomes: order
                .into_iter()
                .map(|disk| MemberDiskOutcome {
                    result: completed.remove(&disk).unwrap_or_else(|| {
                        Err(MemberDiskServiceError::InvalidState(format!(
                            "operation for {disk} produced no result"
                        )))
                    }),
                    disk,
                })
                .collect(),
        }
    }

    async fn run_started(
        &self,
        operation: OperationKind,
        permits: Vec<OperationPermit>,
        observed_at: Option<u64>,
    ) -> Vec<(OperationPermit, DiskResult)> {
        match operation {
            OperationKind::Offline => {
                self.offline(permits, observed_at.expect("offline carries observed_at"))
                    .await
            }
            OperationKind::Online => self.online(permits).await,
            OperationKind::Shrink => self.shrink(permits).await,
        }
    }

    pub(super) async fn commit_all(
        &self,
        permits: &[OperationPermit],
        mutation: MemberDiskMutation,
    ) -> Result<(), MemberDiskServiceError> {
        self.commit(
            permits
                .iter()
                .map(|permit| (permit.disk.clone(), mutation.clone()))
                .collect(),
        )
        .await
    }

    pub(super) fn skip_removed(
        &self,
        permits: Vec<OperationPermit>,
    ) -> (Vec<(OperationPermit, DiskResult)>, Vec<OperationPermit>) {
        let mut finished = Vec::new();
        let mut active = Vec::new();
        for permit in permits {
            match self.state(&permit.disk) {
                Ok((MemberDiskState::Removed, _)) => {
                    finished.push((permit, Ok(MemberDiskState::Removed)))
                }
                Ok(_) => active.push(permit),
                Err(error) => finished.push((permit, Err(error))),
            }
        }
        (finished, active)
    }

    pub(super) fn progress_all(
        &self,
        permits: &[OperationPermit],
        progress: u8,
        detail: &'static str,
    ) {
        for permit in permits {
            self.operations.progress(permit, progress, detail);
        }
    }
}

struct WaitedOperation {
    disk: DiskUuid,
    reason: WaitReason,
    result: DiskResult,
}

async fn wait_for_operations(wait: Vec<OperationWait>) -> Vec<WaitedOperation> {
    join_all(wait.into_iter().map(|wait| async move {
        let disk = wait.disk.clone();
        let reason = wait.reason;
        let result = wait.completed().await;
        WaitedOperation {
            disk,
            reason,
            result,
        }
    }))
    .await
}

fn unique(disks: Vec<DiskUuid>) -> Vec<DiskUuid> {
    let mut seen = HashSet::new();
    disks
        .into_iter()
        .filter(|disk| seen.insert(disk.clone()))
        .collect()
}

pub(super) fn changes(permits: &[OperationPermit], state: DiskIoState) -> Vec<DiskStateChange> {
    permits
        .iter()
        .map(|permit| DiskStateChange {
            disk: permit.disk.clone(),
            state,
        })
        .collect()
}

pub(super) fn partition_cancelled(
    permits: Vec<OperationPermit>,
) -> (Vec<(OperationPermit, DiskResult)>, Vec<OperationPermit>) {
    let mut cancelled = Vec::new();
    let mut active = Vec::new();
    for permit in permits {
        if permit.cancel.is_cancelled() {
            cancelled.push((permit, Err(MemberDiskServiceError::Cancelled)));
        } else {
            active.push(permit);
        }
    }
    (cancelled, active)
}

pub(super) fn append(
    mut left: Vec<(OperationPermit, DiskResult)>,
    right: Vec<(OperationPermit, DiskResult)>,
) -> Vec<(OperationPermit, DiskResult)> {
    left.extend(right);
    left
}

pub(super) fn append_error(
    finished: Vec<(OperationPermit, DiskResult)>,
    permits: Vec<OperationPermit>,
    error: MemberDiskServiceError,
) -> Vec<(OperationPermit, DiskResult)> {
    append(
        finished,
        permits
            .into_iter()
            .map(|permit| (permit, Err(error.clone())))
            .collect(),
    )
}

pub(super) fn append_states(
    service: &MemberDiskService,
    finished: Vec<(OperationPermit, DiskResult)>,
    permits: Vec<OperationPermit>,
) -> Vec<(OperationPermit, DiskResult)> {
    append(
        finished,
        permits
            .into_iter()
            .map(|permit| {
                let result = service.state(&permit.disk).map(|(state, _)| state);
                (permit, result)
            })
            .collect(),
    )
}

pub(super) fn cancelled(
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<(), MemberDiskServiceError> {
    if cancel.is_cancelled() {
        Err(MemberDiskServiceError::Cancelled)
    } else {
        Ok(())
    }
}
