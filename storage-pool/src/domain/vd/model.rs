use crate::domain::{PoolId, disk::DiskId};
use workflow_foundation::{Failure, Outcome};

identity!(VolumeId);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataState {
    Valid,
    Invalid,
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
