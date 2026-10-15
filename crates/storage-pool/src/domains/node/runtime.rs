use super::{model::*, service::*};
use crate::support::object;
use async_trait::async_trait;
use workflow_foundation::{ActivityContext, Outcome, ResourceAccess, ServiceHandler};

pub(crate) struct NodeHost(pub NodeService);

#[async_trait]
impl ServiceHandler for NodeHost {
    type Request = NodeCommand;
    type Reply = PoolNode;
    type Query = Option<NodeId>;
    type View = Vec<PoolNode>;
    fn resources(request: &NodeCommand) -> Vec<ResourceAccess> {
        let id = match request {
            NodeCommand::Inspect(id) => {
                return vec![ResourceAccess::read(object("node", id.as_str()))];
            }
            NodeCommand::Observe { id, .. } | NodeCommand::Load(id) => id,
        };
        vec![ResourceAccess::write(object("node", id.as_str()))]
    }

    async fn execute(&self, request: Self::Request, ctx: ActivityContext) -> Outcome<Self::Reply> {
        self.0.execute(request, ctx.cancellation()).await
    }
    fn query(&self, query: Self::Query) -> Outcome<Self::View> {
        self.0.query(query)
    }
}
