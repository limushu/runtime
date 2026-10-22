use super::{model::*, service::*};
use crate::support::object;
use async_trait::async_trait;
use workflow_foundation::{ActivityContext, Outcome, ResourceAccess, ServiceHandler};

pub(crate) struct DiskHost(pub DiskService);

#[async_trait]
impl ServiceHandler for DiskHost {
    type Request = DiskCommand;
    type Reply = DiskReply;
    type Query = Option<DiskId>;
    type View = Vec<MemberDisk>;
    fn activity_name(request: &DiskCommand) -> &'static str {
        match request {
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
            DiskCommand::Inspect(id) => {
                return vec![ResourceAccess::read(object("disk", id.as_str()))];
            }
            DiskCommand::InspectMany(_)
            | DiskCommand::SetIoMany(_, _)
            | DiskCommand::RemoveMany(_) => unreachable!(),
            DiskCommand::Add(s) => &s.id,
            DiskCommand::Open(id)
            | DiskCommand::SetIo(id, _)
            | DiskCommand::Allocate(id, _)
            | DiskCommand::Remove(id) => id,
        };
        vec![ResourceAccess::write(object("disk", id.as_str()))]
    }

    async fn execute(&self, request: Self::Request, ctx: ActivityContext) -> Outcome<Self::Reply> {
        self.0.execute(request, ctx.cancellation()).await
    }
    fn query(&self, query: Self::Query) -> Outcome<Self::View> {
        self.0.query(query)
    }
}
