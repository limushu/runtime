use std::collections::BTreeSet;
use workflow_foundation::{Failure, ObjectKey, Outcome};

macro_rules! identity {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);
        impl $name {
            pub fn new(value: impl Into<String>) -> Outcome<Self> {
                let value = value.into();
                if value.trim().is_empty() || value.len() > 128 || value.trim() != value {
                    return Err(Failure::Rejected(
                        "identity must be 1..128 bytes without edge whitespace".into(),
                    ));
                }
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}
identity!(PoolId);
identity!(NodeId);
identity!(DiskId);
identity!(VolumeId);
identity!(TierId);

pub(crate) fn object(kind: &str, id: &str) -> ObjectKey {
    ObjectKey::new(kind, id)
}
pub(crate) fn reject(message: &str) -> Failure {
    Failure::Rejected(message.into())
}

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataState {
    Valid,
    Invalid,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolNode {
    pub id: NodeId,
    pub pool: PoolId,
    pub connected: bool,
    pub observation: u64,
    /// Confirmed for the current connectivity epoch, never inferred from Connected.
    pub loaded: bool,
}
impl PoolNode {
    pub fn serving(&self) -> bool {
        self.connected && self.loaded
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BgEntry {
    pub disk: DiskId,
    pub blk: u32,
    pub data: DataState,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockGroup {
    /// Local to its owning VD.
    pub index: u32,
    pub entries: Vec<BgEntry>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualDisk {
    pub id: VolumeId,
    pub pool: PoolId,
    pub replicas: usize,
    pub groups: Vec<BlockGroup>,
    pub revision: u64,
}
impl VirtualDisk {
    pub(crate) fn invalidate(&mut self, disk: &DiskId) {
        let mut changed = false;
        for entry in self.groups.iter_mut().flat_map(|g| &mut g.entries) {
            if &entry.disk == disk && entry.data != DataState::Invalid {
                entry.data = DataState::Invalid;
                changed = true;
            }
        }
        if changed {
            self.revision += 1;
        }
    }
}

/// First vertical slice: one mirrored BG, one BLK per replica, distinct nodes.
#[derive(Debug, Clone)]
pub struct CreateVolume {
    pub id: VolumeId,
    pub disks: Vec<DiskId>,
}
impl CreateVolume {
    pub(crate) fn validate(&self) -> Outcome<()> {
        let unique: BTreeSet<_> = self.disks.iter().collect();
        if !(2..=3).contains(&self.disks.len()) || unique.len() != self.disks.len() {
            return Err(reject("a mirrored BG requires 2 or 3 distinct disks"));
        }
        Ok(())
    }
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
