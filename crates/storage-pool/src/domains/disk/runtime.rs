use super::service::*;
use crate::support::object;
use async_trait::async_trait;
use workflow_foundation::{ActivityContext, Outcome, ResourceAccess, ServiceHandler};

pub(crate) struct DiskHost(pub std::sync::Arc<DiskService>);

#[async_trait]
impl ServiceHandler for DiskHost {
    type Request = DiskCommand;
    type Reply = DiskReply;
    type Query = ();
    type View = ();
    fn activity_name(request: &DiskCommand) -> &'static str {
        match request {
            DiskCommand::ShrinkIntent(..) => "disk.request-shrink",
            DiskCommand::DisableAllocation(..) => "disk.disable-allocation",
            DiskCommand::ClearShrink(..) => "disk.clear-shrink-intent",
            DiskCommand::RequireEmpty(..) => "disk.require-empty",
            DiskCommand::FinalizeShrink(..) => "disk.down-and-remove",
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
        disk_resources(request)
    }

    async fn execute(&self, request: Self::Request, ctx: ActivityContext) -> Outcome<Self::Reply> {
        self.0.execute(request, ctx.control().cancellation()).await
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}

fn disk_resources(request: &DiskCommand) -> Vec<ResourceAccess> {
    if let DiskCommand::RequireTierEmpty(tier) = request {
        return vec![ResourceAccess::read(object("tier", tier.as_str()))];
    }
    if let DiskCommand::InspectMany(ids)
    | DiskCommand::SetIoMany(ids, _)
    | DiskCommand::RemoveMany(ids) = request
    {
        return ids
            .iter()
            .map(|id| {
                let key = object("disk", id.as_str());
                if matches!(request, DiskCommand::InspectMany(_)) {
                    ResourceAccess::read(key)
                } else {
                    ResourceAccess::write(key)
                }
            })
            .collect();
    }
    let id = match request {
        DiskCommand::RequireTierEmpty(_) => unreachable!(),
        DiskCommand::ShrinkIntent(id, _)
        | DiskCommand::DisableAllocation(id, _)
        | DiskCommand::ClearShrink(id, _)
        | DiskCommand::RequireEmpty(id)
        | DiskCommand::FinalizeShrink(id, _) => id,
        DiskCommand::Release { disk, .. } => disk,
        DiskCommand::Inspect(id) => {
            return vec![ResourceAccess::read(object("disk", id.as_str()))];
        }
        DiskCommand::InspectMany(_) | DiskCommand::SetIoMany(_, _) | DiskCommand::RemoveMany(_) => {
            unreachable!()
        }
        DiskCommand::Add(s) => &s.id,
        DiskCommand::Open(id)
        | DiskCommand::SetIo(id, _)
        | DiskCommand::Allocate(id, _)
        | DiskCommand::Remove(id) => id,
        DiskCommand::RemoveEmpty(id) => id,
    };
    vec![ResourceAccess::write(object("disk", id.as_str()))]
}
