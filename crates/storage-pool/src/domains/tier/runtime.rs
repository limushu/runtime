use super::*;
use crate::{Outcome, support::object};
use async_trait::async_trait;
use workflow_foundation::{ActivityContext, ResourceAccess, ServiceHandler};

pub(crate) struct TierHost(pub TierService);
#[async_trait]
impl ServiceHandler for TierHost {
    type Request = TierCommand;
    type Reply = TierReply;
    type Query = Option<TierId>;
    type View = Vec<Tier>;
    fn activity_name(request: &TierCommand) -> &'static str {
        match request {
            TierCommand::Refresh => "tier.restore-metadata",
            TierCommand::Create(_) => "tier.create",
            TierCommand::Inspect(_) => "tier.inspect",
            TierCommand::Delete(_) => "tier.delete",
        }
    }
    fn resources(request: &TierCommand) -> Vec<ResourceAccess> {
        if matches!(request, TierCommand::Refresh) {
            return vec![ResourceAccess::write(object("tier", "catalog"))];
        }
        let id = match request {
            TierCommand::Refresh => unreachable!(),
            TierCommand::Create(id) | TierCommand::Inspect(id) | TierCommand::Delete(id) => id,
        };
        let key = object("tier", id.as_str());
        vec![if matches!(request, TierCommand::Inspect(_)) {
            ResourceAccess::read(key)
        } else {
            ResourceAccess::write(key)
        }]
    }
    async fn execute(&self, request: TierCommand, _: ActivityContext) -> Outcome<TierReply> {
        self.0.execute(request).await
    }
    fn query(&self, query: Option<TierId>) -> Outcome<Vec<Tier>> {
        self.0.query(query)
    }
}
