use crate::kernel::MemberDiskId;
use control_runtime::{ServiceId, ServiceRequest};

pub const SERVICE_ID: &str = "pool.virtual-disk";

#[derive(Debug, Clone)]
pub enum VirtualDiskRequest {
    EvacuateMemberDisk(MemberDiskId),
    Stats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct VirtualDiskStats {
    pub started: usize,
    pub completed: usize,
    pub cancelled: usize,
    pub bg_completed: usize,
    pub stable_stops: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VirtualDiskReply {
    Evacuated { bg_count: usize },
    DrainStopped { committed: usize, skipped: usize },
    Stats(VirtualDiskStats),
}

impl ServiceRequest for VirtualDiskRequest {
    type Response = VirtualDiskReply;

    fn service_id() -> ServiceId {
        ServiceId::new(SERVICE_ID)
    }
}
