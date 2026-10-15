//! Application workflows, one business process per module.
//! Construct through PoolHandle so callers cannot omit the required conflict set.
use crate::domains::disk::service::{DiskCommand, DiskReply};
use crate::domains::node::service::NodeCommand;
use crate::domains::vd::service::{VdCommand, VdReply};
use crate::support::object;
use crate::*;
use workflow_foundation::{ActivityClient, ObjectKey};

mod create_volume;
mod load_node;
mod observe_node;
mod offline_disk;
mod offline_node;
mod online_disk;
mod online_node;

pub use create_volume::CreateVolumeWorkflow;
pub use load_node::LoadNodeWorkflow;
pub use observe_node::ObserveNodeWorkflow;
pub use offline_disk::OfflineDiskWorkflow;
pub use offline_node::OfflineNodeWorkflow;
pub use online_disk::OnlineDiskWorkflow;
pub use online_node::OnlineNodeWorkflow;

#[derive(Clone)]
pub(crate) struct Domains {
    pub disk: ActivityClient<DiskCommand, DiskReply, Option<DiskId>, Vec<MemberDisk>>,
    pub node: ActivityClient<NodeCommand, PoolNode, Option<NodeId>, Vec<PoolNode>>,
    pub vd: ActivityClient<VdCommand, VdReply, Option<VolumeId>, Vec<VirtualDisk>>,
}

fn disk_result(reply: DiskReply) -> Outcome<MemberDisk> {
    match reply {
        DiskReply::Disk(disk) => Ok(disk),
        _ => Err(Failure::Uncertain("unexpected disk reply".into())),
    }
}

fn node_objects(request: &NodeTransitionRequest, specs: &[DiskSpec]) -> Vec<ObjectKey> {
    let mut keys = vec![object("node", request.node.as_str())];
    keys.extend(specs.iter().map(|s| object("disk", s.id.as_str())));
    keys
}

fn batch_disks(reply: DiskReply) -> Outcome<Vec<MemberDisk>> {
    match reply {
        DiskReply::Disks(disks) => Ok(disks),
        _ => Err(Failure::Uncertain("unexpected batch disk reply".into())),
    }
}
