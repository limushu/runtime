use crate::kernel::MemberDiskId;
use control_runtime::{ServiceId, ServiceRequest};

pub const SERVICE_ID: &str = "pool.member-disk";

/// Compound MemberDisk state: physical IO availability × allocation service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskState {
    /// Up + Active: serves IO and accepts new BLK allocation.
    Ua,
    /// Down + Active: unavailable for IO; allocation shutdown has not settled.
    Da,
    /// Down + Inactive: unavailable and no longer accepts allocation.
    Di,
    /// Up + Inactive: media recovered while evacuation was being reversed.
    Ui,
    /// No longer a member of the Pool.
    Removed,
}

impl MemberDiskState {
    pub const fn io_available(self) -> bool {
        matches!(self, Self::Ua | Self::Ui)
    }

    pub const fn allocation_active(self) -> bool {
        matches!(self, Self::Ua | Self::Da)
    }
}

#[derive(Debug, Clone)]
pub enum MemberDiskRequest {
    Offline(MemberDiskId),
    Online(MemberDiskId),
    Get(MemberDiskId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskReply {
    pub disk: MemberDiskId,
    pub state: MemberDiskState,
}

impl ServiceRequest for MemberDiskRequest {
    type Response = MemberDiskReply;

    fn service_id() -> ServiceId {
        ServiceId::new(SERVICE_ID)
    }
}
