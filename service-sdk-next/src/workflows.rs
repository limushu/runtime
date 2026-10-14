//! Application definitions. Only this module knows disk lifecycle sequencing.
//! New kinds of workflow do not require changing ProcessManager.
use crate::{member_disk::*, pool::PoolHandle, process_manager::*};
use crate::{runtime::CallError, vd::VdClient};

async fn state(disks: &DiskClient, disk: &DiskUuid) -> Result<MemberDiskState, WorkflowError> {
    disks
        .get(disk.clone())
        .await
        .map(|d| d.state())
        .map_err(|e| match e {
            CallError::Business(e) => e.into(),
            other => WorkflowError::Failed(other.to_string()),
        })
}

async fn retire(
    ctx: &mut WorkflowContext,
    disks: &DiskClient,
    vd: &VdClient,
    disk: DiskUuid,
) -> Result<(), WorkflowError> {
    match state(disks, &disk).await? {
        MemberDiskState::Removed => return Ok(()),
        MemberDiskState::UpActive | MemberDiskState::DownActive => {
            disks.disable_allocation(ctx, disk.clone()).await?
        }
        _ => {}
    }
    let ticket = disks.prepare_drain(ctx, disk).await?;
    let proof = vd.evacuate(ctx, ticket).await?;
    disks.remove(ctx, proof).await?;
    Ok(())
}

pub fn disk_object(disk: &DiskUuid) -> ObjectClaim {
    ObjectClaim::exclusive(ObjectKey::new("disk", disk.to_string()))
}
pub fn disk_collection() -> ObjectClaim {
    ObjectClaim::exclusive(ObjectKey::collection("disk"))
}

pub fn offline(pool: &PoolHandle, disk: DiskUuid) -> WorkflowTask {
    let disks = pool.disks.clone();
    let vd = pool.virtual_disks.clone();
    let deadline = tokio::time::Instant::now() + pool.recovery_window;
    WorkflowTask::new(
        "disk.offline",
        vec![disk_object(&disk)],
        move |mut ctx| async move {
            match state(&disks, &disk).await? {
                MemberDiskState::Removed => return Ok(()),
                MemberDiskState::UpActive | MemberDiskState::UpInactive => {
                    disks.close_io(&mut ctx, disk.clone()).await?
                }
                _ => {}
            }
            if state(&disks, &disk).await? == MemberDiskState::DownActive {
                ctx.sleep(deadline.saturating_duration_since(tokio::time::Instant::now()))
                    .await?;
            }
            retire(&mut ctx, &disks, &vd, disk).await
        },
    )
}
pub fn online(pool: &PoolHandle, disk: DiskUuid) -> WorkflowTask {
    let disks = pool.disks.clone();
    WorkflowTask::new(
        "disk.online",
        vec![disk_object(&disk)],
        move |mut ctx| async move {
            if state(&disks, &disk).await? != MemberDiskState::UpActive {
                disks.bring_online(&mut ctx, disk).await?;
            }
            Ok(())
        },
    )
}
pub fn shrink(pool: &PoolHandle, disk: DiskUuid) -> WorkflowTask {
    let disks = pool.disks.clone();
    let vd = pool.virtual_disks.clone();
    WorkflowTask::new(
        "disk.shrink",
        vec![disk_object(&disk)],
        move |mut ctx| async move { retire(&mut ctx, &disks, &vd, disk).await },
    )
}
