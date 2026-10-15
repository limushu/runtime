use super::{Domains, node_objects};
use crate::{
    DiskSpec, IoState, NodeTransitionRequest, Outcome, PoolNode,
    app::coordination::{OperationKind, PoolOperation, PoolWorkflow},
    domain::{disk::DiskCommand, node::NodeCommand, reject},
};
use async_trait::async_trait;
use workflow_foundation::{ObjectKey, Workflow, WorkflowContext};

pub struct OnlineNodeWorkflow {
    pub(crate) domains: Domains,
    pub(crate) request: NodeTransitionRequest,
    pub(crate) specs: Vec<DiskSpec>,
}

impl PoolWorkflow for OnlineNodeWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::OnlineNode {
                id: self.request.node.clone(),
                sequence: self.request.sequence,
            },
            objects: self.objects(),
        }
    }
}

#[async_trait]
impl Workflow for OnlineNodeWorkflow {
    type Output = PoolNode;
    const NAME: &'static str = "storage.online-node";
    fn objects(&self) -> Vec<ObjectKey> {
        node_objects(&self.request, &self.specs)
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<PoolNode> {
        ctx.report_progress(Self::NAME, 0, None);
        let node = ctx
            .activity(
                &self.domains.node,
                NodeCommand::Observe {
                    id: self.request.node.clone(),
                    sequence: self.request.sequence,
                    connected: true,
                },
            )
            .await?;
        if node.observation != self.request.sequence || !node.connected {
            return Err(reject("node online superseded by newer observation"));
        }
        let node = ctx
            .activity(&self.domains.node, NodeCommand::Load(self.request.node))
            .await?;
        let ids: Vec<_> = self.specs.iter().map(|s| s.id.clone()).collect();
        for spec in self.specs {
            ctx.activity(&self.domains.disk, DiskCommand::Add(spec.clone()))
                .await?;
            ctx.activity(&self.domains.disk, DiskCommand::Open(spec.id))
                .await?;
        }
        ctx.activity(&self.domains.disk, DiskCommand::SetIoMany(ids, IoState::Up))
            .await?;
        Ok(node)
    }
}
