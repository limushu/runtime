use crate::domain::{PoolId, TierId, node::NodeId, vd::VolumeId};
use std::collections::BTreeSet;
use workflow_foundation::{Failure, Outcome};

identity!(DiskId);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoState {
    Down,
    Up,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationState {
    Inactive,
    Active,
}
/// MemberDisk is the disk domain's aggregate, not a workflow status record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDisk {
    pub id: DiskId,
    pub pool: PoolId,
    pub node: NodeId,
    pub tier: TierId,
    /// Logical BLK count; physical partition/byte geometry is deliberately absent.
    pub capacity_blks: u32,
    pub io: IoState,
    pub allocation: AllocationState,
    pub allocated: BTreeSet<u32>,
    pub revision: u64,
}
impl MemberDisk {
    pub fn allocatable(&self) -> bool {
        self.io == IoState::Up && self.allocation == AllocationState::Active
    }
}

#[derive(Debug, Clone)]
pub struct DiskSpec {
    pub id: DiskId,
    pub node: NodeId,
    pub tier: TierId,
    pub capacity_blks: u32,
}

/// Only DiskService can manufacture allocation evidence; never accepted from users.
pub(crate) struct Allocation {
    pub pool: PoolId,
    pub volume: VolumeId,
    pub disk: DiskId,
    pub node: NodeId,
    pub tier: TierId,
    pub blk: u32,
}
