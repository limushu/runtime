use super::{model::*, service::*};
use crate::support::object;
use async_trait::async_trait;
use workflow_foundation::{ActivityContext, Outcome, ResourceAccess, ServiceHandler};

pub(crate) struct DiskHost(pub std::sync::Arc<DiskService>);

#[async_trait]
impl ServiceHandler for DiskHost {
    type Request = DiskCommand;
    type Reply = DiskReply;
    type Query = Option<DiskId>;
    type View = Vec<MemberDisk>;
    fn activity_name(request: &DiskCommand) -> &'static str {
        match request {
            DiskCommand::RequestShrink(_) => "disk.request-shrink",
            DiskCommand::DisableAllocation(_) => "disk.disable-allocation",
            DiskCommand::RequireEmptyMany(_) => "disk.require-empty-many",
            DiskCommand::RemoveEmptyMany(_) => "disk.remove-empty-many",
            DiskCommand::RequireTierEmpty(_) => "disk.require-tier-empty",
            DiskCommand::RemoveEmpty(_) => "disk.remove-empty",
            DiskCommand::Release { .. } => "disk.release-allocation",
            DiskCommand::Inspect(_) => "disk.inspect",
            DiskCommand::InspectMany(_) => "disk.inspect-many",
            DiskCommand::SetIoMany(_, _) => "disk.set-io-many",
            DiskCommand::RemoveMany(_) => "disk.remove-many",
            DiskCommand::Add(_) => "disk.add",
            DiskCommand::Open(_) => "disk.open",
            DiskCommand::SetIo(_, _) => "disk.set-io",
            DiskCommand::Allocate(_, _) => "disk.allocate",
            DiskCommand::Remove(_) => "disk.remove",
        }
    }
    fn resources(request: &DiskCommand) -> Vec<ResourceAccess> {
        if let DiskCommand::RequireTierEmpty(tier) = request {
            return vec![ResourceAccess::read(object("tier", tier.as_str()))];
        }
        // Includes the short read-model refresh before each command. Disjoint
        // disk batches keep executing concurrently.
        request
            .disks()
            .iter()
            .map(|id| ResourceAccess::write(object("disk", id.as_str())))
            .collect()
    }

    async fn execute(&self, request: Self::Request, ctx: ActivityContext) -> Outcome<Self::Reply> {
        self.0.execute(request, ctx.cancellation()).await
    }
    fn query(&self, query: Self::Query) -> Outcome<Self::View> {
        self.0.query(query)
    }
}
