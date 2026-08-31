use crate::kernel::MemberDiskId;
use control_runtime::ServiceRequest;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskIoAvailability {
    Up,
    Down,
}

#[derive(Debug, Clone)]
pub(crate) enum PoolNodeCommand {
    PublishMemberDisk {
        disk: MemberDiskId,
        availability: MemberDiskIoAvailability,
    },
    PublishedCount,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PoolNodeResponse {
    Published,
    PublishedCount(usize),
}

impl ServiceRequest for PoolNodeCommand {
    type Response = PoolNodeResponse;
}
