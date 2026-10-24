use crate::domains::{PoolId, TierId, node::NodeId, vd::VolumeId};
use std::collections::BTreeSet;

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
    /// Persisted administrative intent; independent of a running task.
    pub shrink_requested: bool,
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

impl MemberDisk {
    pub(crate) fn new(pool: PoolId, spec: DiskSpec) -> crate::domains::DomainResult<Self> {
        if spec.capacity_blks == 0 {
            return Err(crate::domains::DomainError("disk has zero capacity".into()));
        }
        Ok(Self {
            id: spec.id,
            pool,
            node: spec.node,
            tier: spec.tier,
            capacity_blks: spec.capacity_blks,
            io: IoState::Down,
            allocation: AllocationState::Active,
            shrink_requested: false,
            allocated: BTreeSet::new(),
            revision: 0,
        })
    }
    pub(crate) fn require_identity(&self, spec: &DiskSpec) -> crate::domains::DomainResult<()> {
        if self.node != spec.node
            || self.tier != spec.tier
            || self.capacity_blks != spec.capacity_blks
        {
            return Err(crate::domains::DomainError(
                "existing disk identity has different topology or capacity".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn next_free_block(&self) -> crate::domains::DomainResult<u32> {
        if !self.allocatable() {
            return Err(crate::domains::DomainError("disk cannot allocate".into()));
        }
        (0..self.capacity_blks)
            .find(|b| !self.allocated.contains(b))
            .ok_or_else(|| crate::domains::DomainError("disk is full".into()))
    }
}
