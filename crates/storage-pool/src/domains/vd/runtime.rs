use super::{model::*, service::*};
use crate::support::object;
use async_trait::async_trait;
use workflow_foundation::{ActivityContext, Outcome, ResourceAccess, ServiceHandler};

pub(crate) struct VdHost(pub VdService);

#[async_trait]
impl ServiceHandler for VdHost {
    type Request = VdCommand;
    type Reply = VdReply;
    type Query = Option<VolumeId>;
    type View = Vec<VirtualDisk>;
    fn resources(request: &VdCommand) -> Vec<ResourceAccess> {
        if matches!(request, VdCommand::RequireAbsent(_)) {
            return vec![ResourceAccess::read(object("vd", "reference-index"))];
        }
        // Protect the reverse-reference index including newly created VDs. A
        // fixed known set is safer than discovering/expanding locks during IO.
        vec![ResourceAccess::write(object("vd", "reference-index"))]
    }

    async fn execute(&self, request: Self::Request, ctx: ActivityContext) -> Outcome<Self::Reply> {
        self.0.execute(request, ctx.cancellation()).await
    }
    fn query(&self, query: Self::Query) -> Outcome<Self::View> {
        self.0.query(query)
    }
}
