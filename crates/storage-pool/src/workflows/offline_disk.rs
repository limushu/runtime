use super::{Domains, disk_result};
use crate::domains::disk::service::DiskCommand;
use crate::domains::vd::service::VdCommand;
use crate::support::object;
use crate::task_center::{OperationKind, PoolOperation, PoolWorkflow};
use crate::*;
use async_trait::async_trait;
use workflow_foundation::{ObjectKey, Workflow, WorkflowContext};

pub struct OfflineDiskWorkflow {
    pub(crate) domains: Domains,
    pub(crate) id: DiskId,
    pub(crate) node: NodeId,
}

impl PoolWorkflow for OfflineDiskWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::OfflineDisk(self.id.clone()),
            objects: self.objects(),
        }
    }
}

#[async_trait]
impl Workflow for OfflineDiskWorkflow {
    type Output = MemberDisk;
    const NAME: &'static str = "storage.offline-disk";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![
            object("disk", self.id.as_str()),
            object("node", self.node.as_str()),
        ]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<MemberDisk> {
        ctx.report_progress(Self::NAME, 0, None);
        ctx.activity(&self.domains.disk, DiskCommand::Inspect(self.id.clone()))
            .await?;
        // Conservative validity transition first: cancellation may leave an Up
        // disk with Invalid data, never a fake successful repair or freed BLKs.
        ctx.activity(&self.domains.vd, VdCommand::Invalidate(self.id.clone()))
            .await?;
        ctx.activity(
            &self.domains.disk,
            DiskCommand::SetIo(self.id.clone(), IoState::Down),
        )
        .await?;
        // Skeleton only: the VD domain will wait for rebuild and confirm that
        // committed BG mappings no longer reference this member disk.
        ctx.activity(&self.domains.vd, VdCommand::WaitRebuild(self.id.clone()))
            .await?;
        // Never remove membership if rebuilding fails or has not completed.
        disk_result(
            ctx.activity(&self.domains.disk, DiskCommand::Remove(self.id))
                .await?,
        )
    }
}
