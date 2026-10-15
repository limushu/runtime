use super::Domains;
use crate::domains::node::service::NodeCommand;
use crate::support::object;
use crate::task_center::{OperationKind, PoolOperation, PoolWorkflow};
use crate::*;
use async_trait::async_trait;
use workflow_foundation::{ObjectKey, Workflow, WorkflowContext};

pub struct LoadNodeWorkflow {
    pub(crate) domains: Domains,
    pub(crate) id: NodeId,
}

impl PoolWorkflow for LoadNodeWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::LoadNode(self.id.clone()),
            objects: self.objects(),
        }
    }
}

#[async_trait]
impl Workflow for LoadNodeWorkflow {
    type Output = PoolNode;
    const NAME: &'static str = "storage.load-node";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("node", self.id.as_str())]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<PoolNode> {
        ctx.report_progress(Self::NAME, 0, None);
        ctx.activity(&self.domains.node, NodeCommand::Load(self.id))
            .await
    }
}
