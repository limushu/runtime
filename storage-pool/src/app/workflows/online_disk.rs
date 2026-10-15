use super::{Domains, disk_result};
use crate::{
    DiskSpec, IoState, MemberDisk, Outcome,
    app::coordination::{OperationKind, PoolOperation, PoolWorkflow},
    domain::{disk::DiskCommand, node::NodeCommand, object, reject},
};
use async_trait::async_trait;
use workflow_foundation::{ObjectKey, Workflow, WorkflowContext};

pub struct OnlineDiskWorkflow {
    pub(crate) domains: Domains,
    pub(crate) spec: DiskSpec,
}

impl PoolWorkflow for OnlineDiskWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::OnlineDisk(self.spec.id.clone()),
            objects: self.objects(),
        }
    }
}

#[async_trait]
impl Workflow for OnlineDiskWorkflow {
    type Output = MemberDisk;
    const NAME: &'static str = "storage.online-disk";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![
            object("disk", self.spec.id.as_str()),
            object("node", self.spec.node.as_str()),
        ]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<MemberDisk> {
        ctx.report_progress(Self::NAME, 0, None);
        let node = ctx
            .activity(
                &self.domains.node,
                NodeCommand::Inspect(self.spec.node.clone()),
            )
            .await?;
        if !node.serving() {
            return Err(reject("disk online requires a loaded, connected pool node"));
        }
        let id = self.spec.id.clone();
        // These are steps of ONE business operation, never separate public runs.
        // Ensure membership is idempotent; Open alone does not publish IO Up.
        ctx.activity(&self.domains.disk, DiskCommand::Add(self.spec))
            .await?;
        ctx.activity(&self.domains.disk, DiskCommand::Open(id.clone()))
            .await?;
        disk_result(
            ctx.activity(&self.domains.disk, DiskCommand::SetIo(id, IoState::Up))
                .await?,
        )
    }
}
