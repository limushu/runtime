use crate::domains::{PoolId, disk::DiskId};

identity!(VolumeId);

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DataState {
    Valid,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BgEntry {
    pub disk: DiskId,
    pub blk: u32,
    pub data: DataState,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BlockGroup {
    /// Local to its owning VD.
    pub index: u32,
    pub entries: Vec<BgEntry>,
}
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

impl VirtualDisk {
    pub(crate) fn from_allocations(
        pool: PoolId,
        id: VolumeId,
        allocations: Vec<crate::domains::disk::Allocation>,
    ) -> crate::domains::DomainResult<Self> {
        use std::collections::BTreeSet;
        let nodes: BTreeSet<_> = allocations.iter().map(|a| &a.node).collect();
        let tiers: BTreeSet<_> = allocations.iter().map(|a| &a.tier).collect();
        let disks: BTreeSet<_> = allocations.iter().map(|a| &a.disk).collect();
        if tiers.len() != 1
            || !(2..=3).contains(&allocations.len())
            || nodes.len() != allocations.len()
            || disks.len() != allocations.len()
            || allocations.iter().any(|a| a.pool != pool || a.volume != id)
        {
            return Err(crate::domains::DomainError(
                "invalid allocation evidence or replica placement".into(),
            ));
        }
        let entries: Vec<_> = allocations
            .into_iter()
            .map(|a| BgEntry {
                disk: a.disk,
                blk: a.blk,
                data: DataState::Valid,
            })
            .collect();
        Ok(Self {
            id,
            pool,
            replicas: entries.len(),
            groups: vec![BlockGroup { index: 0, entries }],
            revision: 0,
        })
    }
}
