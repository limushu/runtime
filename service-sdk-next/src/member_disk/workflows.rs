//! Read the business sequence here. No queues, Action enum or completion callbacks.
use super::{activities::Activities, *};
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
            _ => true,
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
) -> Result<MemberDiskReply, MemberDiskError> {
    ctx.checkpoint::<MemberDiskError>().await?;
    match input {
        DiskWorkflow::Unknown { disk, .. } => Err(MemberDiskError::UnknownDisk(disk)),
        DiskWorkflow::Offline { disk, observed_at } => {
            offline(&a, &ctx, &disk, observed_at).await?;
            disk_reply(&a, disk)
        }
        DiskWorkflow::Online(disk) => {
            if a.get(&disk)?.state() != MemberDiskState::UpActive {
                ctx.activity("bring-online", a.bring_online(&disk, &ctx))
                    .await?;
            }
            disk_reply(&a, disk)
        }
        DiskWorkflow::Shrink(disk) => {
            retire(&a, &ctx, &disk).await?;
            disk_reply(&a, disk)
        }
        DiskWorkflow::Allocate(count) => space(&a, &ctx, a.allocation_plan(count)?, true).await,
        DiskWorkflow::Release(blocks) => space(&a, &ctx, a.release_plan(blocks)?, false).await,
    }
}

async fn offline(
    a: &Activities,
    ctx: &WorkflowContext,
    disk: &DiskUuid,
    observed_at: EpochMillis,
) -> Result<(), MemberDiskError> {
    use MemberDiskState::*;
    if a.get(disk)?.state() == Removed {
        return Ok(());
    }
    if matches!(a.get(disk)?.state(), UpActive | UpInactive) {
        ctx.activity("close-io", a.close_io(disk)).await?;
    }
    if a.get(disk)?.state() == DownActive {
        let deadline = observed_at.saturating_add(a.recovery_window().as_millis() as u64);
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
    if a.get(disk)?.state() == Removed {
        return Ok(());
    }
    if matches!(a.get(disk)?.state(), UpActive | DownActive) {
        ctx.activity("disable-allocation", a.disable_allocation(disk, ctx))
            .await?;
    }
    ctx.activity("evacuate", a.evacuate(disk, ctx)).await?;
    ctx.activity("remove", a.remove(disk, ctx)).await
}

fn disk_reply(a: &Activities, disk: DiskUuid) -> Result<MemberDiskReply, MemberDiskError> {
    let member = a.get(&disk)?;
    Ok(MemberDiskReply::Disks(vec![DiskOutcome {
        disk,
        result: Ok(member),
    }]))
}

async fn space(
    a: &Activities,
    ctx: &WorkflowContext,
    plan: Vec<MemberDiskCommit>,
    allocating: bool,
) -> Result<MemberDiskReply, MemberDiskError> {
    let mut committed = Vec::new();
    for commit in plan {
        let result = ctx
            .activity(
                if allocating { "allocate" } else { "release" },
                a.commit(&commit.disk, commit.change.clone(), ctx, false),
            )
            .await;
        if let Err(cause) = result {
            return if committed.is_empty() {
                Err(cause)
            } else {
                Err(MemberDiskError::PartialSpace {
                    allocated: allocating,
                    committed,
                    cause: Box::new(cause),
                })
            };
        }
        let blocks = match commit.change {
            MemberDiskChange::Allocate(b) | MemberDiskChange::Release(b) => b,
            _ => unreachable!(),
        };
        committed.extend(blocks.into_iter().map(|block| AllocatedBlock {
            disk: commit.disk.clone(),
            block,
        }));
    }
    Ok(if allocating {
        MemberDiskReply::Allocated(committed)
    } else {
        MemberDiskReply::Released
    })
}
