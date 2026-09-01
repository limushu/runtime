use super::model::{MemberDisk, PhysicalState};
use crate::kernel::{BlkId, MemberDiskId};
use control_runtime::ServiceRequest;

#[derive(Debug, Clone)]
pub(crate) enum MemberDiskCommand {
    Create(MemberDisk),
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
    Disk(MemberDisk),
    Disks(Vec<MemberDisk>),
    Allocated(BlkId),
    Deleted,
    Released,
}

impl ServiceRequest for MemberDiskCommand {
    type Response = MemberDiskResponse;
}
