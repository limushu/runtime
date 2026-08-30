use crate::kernel::MemberDiskId;
use control_runtime::{ServiceId, ServiceRequest};

pub const SERVICE_ID: &str = "pool.node";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskIoAvailability {
    Up,
    Down,
}

#[derive(Debug, Clone)]
pub enum PoolNodeRequest {
    PublishMemberDisk {
        disk: MemberDiskId,
        availability: MemberDiskIoAvailability,
    },
    PublishedCount,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolNodeReply {
    Published,
    PublishedCount(usize),
}

impl ServiceRequest for PoolNodeRequest {
    type Response = PoolNodeReply;

    fn service_id() -> ServiceId {
        ServiceId::new(SERVICE_ID)
    }
}
