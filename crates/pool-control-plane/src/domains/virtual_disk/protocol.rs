use crate::kernel::MemberDiskId;
use control_runtime::ServiceRequest;

#[derive(Debug, Clone)]
pub(crate) enum VirtualDiskCommand {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvacuationResult {
    pub bg_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum VirtualDiskResponse {
    Evacuated(EvacuationResult),
    Stats(VirtualDiskStats),
}

impl ServiceRequest for VirtualDiskCommand {
    type Response = VirtualDiskResponse;
}
