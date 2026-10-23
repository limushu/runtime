use super::{model::*, service::*};
use crate::support::object;
use async_trait::async_trait;
use workflow_foundation::{ActivityContext, Outcome, ResourceAccess, ServiceHandler};

pub(crate) struct NodeHost(pub std::sync::Arc<NodeService>);

#[async_trait]
impl ServiceHandler for NodeHost {
    type Request = NodeCommand;
    type Reply = PoolNode;
    type Query = Option<NodeId>;
    type View = Vec<PoolNode>;
    fn activity_name(request: &NodeCommand) -> &'static str {
        match request {
            NodeCommand::Attach { .. } => "node.attach",
            NodeCommand::Detach(_) => "node.detach",
            NodeCommand::Unload(_) => "node.unload",
            NodeCommand::Inspect(_) => "node.inspect",
            NodeCommand::Observe { .. } => "node.observe",
            NodeCommand::Load(_) => "node.load",
        }
    }
    fn resources(request: &NodeCommand) -> Vec<ResourceAccess> {
        let id = match request {
            NodeCommand::Inspect(id) => {
                return vec![ResourceAccess::read(object("node", id.as_str()))];
            }
            NodeCommand::Unload(id)
            | NodeCommand::Attach { id, .. }
            | NodeCommand::Detach(id)
            | NodeCommand::Observe { id, .. }
            | NodeCommand::Load(id) => id,
        };
        vec![ResourceAccess::write(object("node", id.as_str()))]
    }

    async fn execute(&self, request: Self::Request, ctx: ActivityContext) -> Outcome<Self::Reply> {
        self.0.execute(request, ctx.control().cancellation()).await
    }
    fn query(&self, query: Self::Query) -> Outcome<Self::View> {
        self.0.query(query)
    }
}
