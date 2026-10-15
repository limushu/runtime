use super::Domains;
use crate::{
    NodeId, Outcome, PoolNode,
    app::coordination::{OperationKind, PoolOperation, PoolWorkflow},
    domain::{node::NodeCommand, object},
};
use async_trait::async_trait;
use workflow_foundation::{ObjectKey, Workflow, WorkflowContext};

pub struct ObserveNodeWorkflow {
    pub(crate) domains: Domains,
    pub(crate) id: NodeId,
    pub(crate) sequence: u64,
    pub(crate) connected: bool,
}

impl PoolWorkflow for ObserveNodeWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::ObserveNode {
                id: self.id.clone(),
                sequence: self.sequence,
                connected: self.connected,
            },
            objects: self.objects(),
        }
    }
}

#[async_trait]
impl Workflow for ObserveNodeWorkflow {
    type Output = PoolNode;
    const NAME: &'static str = "storage.observe-node";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("node", self.id.as_str())]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<PoolNode> {
        ctx.report_progress(Self::NAME, 0, None);
        ctx.activity(
            &self.domains.node,
            NodeCommand::Observe {
                id: self.id,
                sequence: self.sequence,
                connected: self.connected,
            },
        )
        .await
    }
}
