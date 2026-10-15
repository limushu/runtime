use super::{Domains, batch_disks, node_objects};
use crate::{
    DiskSpec, Failure, IoState, NodeTransitionRequest, Outcome, PoolNode,
    app::coordination::{OperationKind, PoolOperation, PoolWorkflow},
    domain::{disk::DiskCommand, node::NodeCommand, reject, vd::VdCommand},
};
use async_trait::async_trait;
use workflow_foundation::{ObjectKey, Workflow, WorkflowContext};

pub struct OfflineNodeWorkflow {
    pub(crate) domains: Domains,
    pub(crate) request: NodeTransitionRequest,
    pub(crate) specs: Vec<DiskSpec>,
}

impl PoolWorkflow for OfflineNodeWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::OfflineNode {
                id: self.request.node.clone(),
                sequence: self.request.sequence,
            },
            objects: self.objects(),
        }
    }
}

#[async_trait]
impl Workflow for OfflineNodeWorkflow {
    type Output = PoolNode;
    const NAME: &'static str = "storage.offline-node";
    fn objects(&self) -> Vec<ObjectKey> {
        node_objects(&self.request, &self.specs)
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<PoolNode> {
        ctx.report_progress(Self::NAME, 0, None);
        let node = ctx
            .activity(
                &self.domains.node,
                NodeCommand::Observe {
                    id: self.request.node,
                    sequence: self.request.sequence,
                    connected: false,
                },
            )
            .await?;
        if node.observation != self.request.sequence || node.connected {
            return Err(reject("node offline superseded by newer observation"));
        }
        ctx.report_progress("inspect-member-disks", 1, Some(7));
        let disks = batch_disks(
            ctx.activity(
                &self.domains.disk,
                DiskCommand::InspectMany(self.specs.into_iter().map(|s| s.id).collect()),
            )
            .await?,
        )?;
        let ids: Vec<_> = disks.into_iter().map(|d| d.id).collect();
        ctx.report_progress("invalidate-replicas", 2, Some(7));
        ctx.activity(&self.domains.vd, VdCommand::InvalidateMany(ids.clone()))
            .await?;
        ctx.report_progress("close-disk-io", 3, Some(7));
        ctx.activity(
            &self.domains.disk,
            DiskCommand::SetIoMany(ids.clone(), IoState::Down),
        )
        .await?;
        ctx.report_progress("wait-rebuild", 4, Some(7));
        ctx.activity(&self.domains.vd, VdCommand::WaitRebuildMany(ids.clone()))
            .await?;
        ctx.report_progress("remove-members", 5, Some(7));
        ctx.activity(&self.domains.disk, DiskCommand::RemoveMany(ids))
            .await?;
        ctx.report_progress("power-off-unimplemented", 6, Some(7));
        // Physical power-off is not implemented. Never report that it occurred.
        Err(Failure::Failed("node power-off is not implemented".into()))
    }
}
