//! Read the business sequence here. No queues, Action enum or completion callbacks.
use super::*;
use crate::vd::VdService;
use std::time::Duration;

/// Activity adapter: typed calls only. All mutable state belongs to domains.
#[derive(Clone)]
pub(crate) struct Activities {
    pub disk: DiskService,
    pub vd: VdService,
    pub recovery_window: Duration,
}
use crate::runtime::workflow::WorkflowContext;

#[derive(Debug, Clone)]
pub(crate) enum DiskWorkflow {
    Unknown {
        disk: DiskUuid,
        kind: CommandKind,
    },
    Offline {
        disk: DiskUuid,
        observed_at: EpochMillis,
    },
    Online(DiskUuid),
    Shrink(DiskUuid),
    Allocate(usize),
    Release(Vec<AllocatedBlock>),
}

impl DiskWorkflow {
    pub fn disk(&self) -> Option<&DiskUuid> {
        match self {
            Self::Offline { disk, .. }
            | Self::Online(disk)
            | Self::Shrink(disk)
            | Self::Unknown { disk, .. } => Some(disk),
            _ => None,
        }
    }
    pub fn kind(&self) -> CommandKind {
        match self {
            Self::Unknown { kind, .. } => *kind,
            Self::Offline { .. } | Self::Online(_) => CommandKind::DiskEvents,
            Self::Shrink(_) => CommandKind::Shrink,
            Self::Allocate(_) => CommandKind::Allocate,
            Self::Release(_) => CommandKind::Release,
        }
    }
    pub fn conflicts(&self, other: &Self) -> bool {
        if matches!(self, Self::Unknown { .. }) || matches!(other, Self::Unknown { .. }) {
            return false;
        }
        match (self.disk(), other.disk()) {
            (Some(a), Some(b)) => a == b,
            _ => false,
        }
    }
    pub fn preempts(&self, other: &Self) -> bool {
        self.disk().is_some()
            && self.conflicts(other)
            && std::mem::discriminant(self) != std::mem::discriminant(other)
    }
}

pub(crate) async fn run(
    input: DiskWorkflow,
    a: Activities,
    ctx: WorkflowContext,
) -> Result<ProcessReply, MemberDiskError> {
    ctx.checkpoint::<MemberDiskError>().await?;
    match input {
        DiskWorkflow::Unknown { disk, .. } => Err(MemberDiskError::UnknownDisk(disk)),
        DiskWorkflow::Offline { disk, observed_at } => {
            offline(&a, &ctx, &disk, observed_at).await?;
            disk_reply(&a, disk)
        }
        DiskWorkflow::Online(disk) => {
            if a.disk.get(&disk)?.state() != MemberDiskState::UpActive {
                ctx.activity(
                    "bring-online",
                    a.disk.bring_online(&disk, ctx.cancellation()),
                )
                .await?;
            }
            disk_reply(&a, disk)
        }
        DiskWorkflow::Shrink(disk) => {
            retire(&a, &ctx, &disk).await?;
            disk_reply(&a, disk)
        }
        DiskWorkflow::Allocate(count) => ctx
            .activity("allocate", a.disk.allocate(count, ctx.cancellation()))
            .await
            .map(ProcessReply::Allocated),
        DiskWorkflow::Release(blocks) => ctx
            .activity("release", a.disk.release(blocks, ctx.cancellation()))
            .await
            .map(|()| ProcessReply::Released),
    }
}

async fn offline(
    a: &Activities,
    ctx: &WorkflowContext,
    disk: &DiskUuid,
    observed_at: EpochMillis,
) -> Result<(), MemberDiskError> {
    use MemberDiskState::*;
    if a.disk.get(disk)?.state() == Removed {
        return Ok(());
    }
    if matches!(a.disk.get(disk)?.state(), UpActive | UpInactive) {
        ctx.activity("close-io", a.disk.close_io(disk, ctx.cancellation()))
            .await?;
    }
    if a.disk.get(disk)?.state() == DownActive {
        let deadline = observed_at.saturating_add(a.recovery_window.as_millis() as u64);
        ctx.activity(
            "recovery-window",
            ctx.sleep::<MemberDiskError>(std::time::Duration::from_millis(
                deadline.saturating_sub(now_millis()),
            )),
        )
        .await?;
    }
    retire(a, ctx, disk).await
}

async fn retire(
    a: &Activities,
    ctx: &WorkflowContext,
    disk: &DiskUuid,
) -> Result<(), MemberDiskError> {
    use MemberDiskState::*;
    if a.disk.get(disk)?.state() == Removed {
        return Ok(());
    }
    if matches!(a.disk.get(disk)?.state(), UpActive | DownActive) {
        ctx.activity(
            "disable-allocation",
            a.disk.disable_allocation(disk, ctx.cancellation()),
        )
        .await?;
    }
    let ticket = ctx
        .activity(
            "prepare-drain",
            a.disk.prepare_drain(disk, ctx.cancellation()),
        )
        .await?;
    let proof = ctx
        .activity("evacuate", a.vd.evacuate(ticket, ctx.cancellation()))
        .await?;
    ctx.activity("remove", a.disk.remove(proof, ctx.cancellation()))
        .await
}

fn disk_reply(a: &Activities, disk: DiskUuid) -> Result<ProcessReply, MemberDiskError> {
    let member = a.disk.get(&disk)?;
    Ok(ProcessReply::Disks(vec![DiskOutcome {
        disk,
        result: Ok(member),
    }]))
}
