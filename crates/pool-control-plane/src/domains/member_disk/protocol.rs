use super::model::{MemberDiskPatch, MemberDiskSnapshot, MemberDiskSpec, PhysicalState};
use crate::kernel::{BlkId, MemberDiskId};
use control_runtime::ServiceRequest;

#[derive(Debug, Clone)]
pub(crate) enum MemberDiskCommand {
    Create(MemberDiskSpec),
    Update {
        disk: MemberDiskId,
        patch: MemberDiskPatch,
    },
    Delete(MemberDiskId),
    ApplyPhysical {
        disk: MemberDiskId,
        state: PhysicalState,
    },
    Allocate(MemberDiskId),
    Release {
        disk: MemberDiskId,
        blk: BlkId,
    },
    Get(MemberDiskId),
    List,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MemberDiskResponse {
    Snapshot(MemberDiskSnapshot),
    Snapshots(Vec<MemberDiskSnapshot>),
    Allocated(BlkId),
    Deleted,
    Released,
}

impl ServiceRequest for MemberDiskCommand {
    type Response = MemberDiskResponse;
}
