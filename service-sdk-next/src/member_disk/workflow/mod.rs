mod offline;
mod online;
mod shrink;

use super::operations::{DiskOperation, DiskResult, OperationKind, OperationWait, WaitReason};
use super::{
    DiskIoState, DiskStateChange, DiskUuid, MemberDiskMutation, MemberDiskOutcome, MemberDiskReply,
    MemberDiskService, MemberDiskServiceError, MemberDiskState,
};
use crate::service::ExecutionContext;
use futures_util::future::join_all;
use std::collections::{HashMap, HashSet};

impl MemberDiskService {
    pub(super) async fn run_disks(
        &self,
        operation: OperationKind,
        disks: Vec<DiskUuid>,
        observed_at: Option<u64>,
        context: &ExecutionContext<()>,
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
                    .map(|(disk_operation, result)| {
                        let disk = disk_operation.disk.clone();
                        self.operations.finish(disk_operation, result.clone());
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
        disk_operations: Vec<DiskOperation>,
        observed_at: Option<u64>,
    ) -> Vec<(DiskOperation, DiskResult)> {
        match operation {
            OperationKind::Offline => {
                self.offline(
                    disk_operations,
                    observed_at.expect("offline carries observed_at"),
                )
                .await
            }
            OperationKind::Online => self.online(disk_operations).await,
            OperationKind::Shrink => self.shrink(disk_operations).await,
        }
    }

    pub(super) async fn commit_all(
        &self,
        disk_operations: &[DiskOperation],
        mutation: MemberDiskMutation,
    ) -> Result<(), MemberDiskServiceError> {
        self.commit(
            disk_operations
                .iter()
                .map(|operation| (operation.disk.clone(), mutation.clone()))
                .collect(),
        )
        .await
    }

    pub(super) fn skip_removed(
        &self,
        disk_operations: Vec<DiskOperation>,
    ) -> (Vec<(DiskOperation, DiskResult)>, Vec<DiskOperation>) {
        let mut finished = Vec::new();
        let mut active = Vec::new();
        for operation in disk_operations {
            match self.state(&operation.disk) {
                Ok((MemberDiskState::Removed, _)) => {
                    finished.push((operation, Ok(MemberDiskState::Removed)))
                }
                Ok(_) => active.push(operation),
                Err(error) => finished.push((operation, Err(error))),
            }
        }
        (finished, active)
    }

    pub(super) fn progress_all(
        &self,
        disk_operations: &[DiskOperation],
        progress: u8,
        detail: &'static str,
    ) {
        for operation in disk_operations {
            self.operations.progress(operation, progress, detail);
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

pub(super) fn changes(
    disk_operations: &[DiskOperation],
    state: DiskIoState,
) -> Vec<DiskStateChange> {
    disk_operations
        .iter()
        .map(|operation| DiskStateChange {
            disk: operation.disk.clone(),
            state,
        })
        .collect()
}

pub(super) fn partition_cancelled(
    disk_operations: Vec<DiskOperation>,
) -> (Vec<(DiskOperation, DiskResult)>, Vec<DiskOperation>) {
    let mut cancelled = Vec::new();
    let mut active = Vec::new();
    for operation in disk_operations {
        if operation.cancel.is_cancelled() {
            cancelled.push((operation, Err(MemberDiskServiceError::Cancelled)));
        } else {
            active.push(operation);
        }
    }
    (cancelled, active)
}

pub(super) fn append(
    mut left: Vec<(DiskOperation, DiskResult)>,
    right: Vec<(DiskOperation, DiskResult)>,
) -> Vec<(DiskOperation, DiskResult)> {
    left.extend(right);
    left
}

pub(super) fn append_error(
    finished: Vec<(DiskOperation, DiskResult)>,
    disk_operations: Vec<DiskOperation>,
    error: MemberDiskServiceError,
) -> Vec<(DiskOperation, DiskResult)> {
    append(
        finished,
        disk_operations
            .into_iter()
            .map(|operation| (operation, Err(error.clone())))
            .collect(),
    )
}

pub(super) fn append_states(
    service: &MemberDiskService,
    finished: Vec<(DiskOperation, DiskResult)>,
    disk_operations: Vec<DiskOperation>,
) -> Vec<(DiskOperation, DiskResult)> {
    append(
        finished,
        disk_operations
            .into_iter()
            .map(|operation| {
                let result = service.state(&operation.disk).map(|(state, _)| state);
                (operation, result)
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
