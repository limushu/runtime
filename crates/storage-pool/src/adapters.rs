//! Explicit simulators, not persistence or real user_dp implementations.
use crate::support::reject;
use crate::{ports::*, *};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct MemoryMetadata {
    state: Mutex<MemoryState>,
}
#[derive(Default)]
struct MemoryState {
    tiers: BTreeMap<(PoolId, TierId), Tier>,
    retired_tiers: BTreeSet<(PoolId, TierId)>,
    retired_disks: BTreeSet<(PoolId, DiskId)>,
    retired_volumes: BTreeSet<(PoolId, VolumeId)>,
    allocations: BTreeMap<(PoolId, DiskId, u32), VolumeId>,
    released: BTreeSet<(PoolId, DiskId, u32, VolumeId)>,
    log: Vec<(PoolId, MetadataChange)>,
    disks: BTreeMap<(PoolId, DiskId), MemberDisk>,
    volumes: BTreeMap<(PoolId, VolumeId), VirtualDisk>,
}
impl MemoryMetadata {
    pub fn committed(&self) -> Vec<(PoolId, MetadataChange)> {
        self.state.lock().unwrap().log.clone()
    }
}
#[async_trait]
impl MetadataStore for MemoryMetadata {
    async fn commit(&self, pool: &PoolId, change: MetadataChange) -> Outcome<()> {
        let mut state = self.state.lock().unwrap();
        match &change {
            MetadataChange::CreateTier(tier) => {
                let key = (pool.clone(), tier.id.clone());
                if tier.pool != *pool
                    || state.tiers.contains_key(&key)
                    || state.retired_tiers.contains(&key)
                {
                    return Err(reject("tier identity already used or wrong pool"));
                }
                state.tiers.insert(key, tier.clone());
            }
            MetadataChange::DeleteTier {
                id,
                expected_revision,
            } => {
                let key = (pool.clone(), id.clone());
                let tier = state
                    .tiers
                    .get(&key)
                    .ok_or_else(|| reject("unknown tier"))?;
                if tier.revision != *expected_revision
                    || state
                        .disks
                        .values()
                        .any(|d| d.pool == *pool && d.tier == *id)
                {
                    return Err(reject("tier is referenced or revision is stale"));
                }
                state.tiers.remove(&key);
                state.retired_tiers.insert(key);
            }
            MetadataChange::RemoveDisk {
                disk,
                expected_revision,
            } => {
                let key = (pool.clone(), disk.clone());
                let member = state
                    .disks
                    .get(&key)
                    .ok_or_else(|| reject("unknown disk"))?;
                if member.revision != *expected_revision
                    || member.io != IoState::Down
                    || !member.allocated.is_empty()
                    || state.volumes.values().any(|v| {
                        v.pool == *pool
                            && v.groups
                                .iter()
                                .flat_map(|g| &g.entries)
                                .any(|e| e.disk == *disk)
                    })
                {
                    return Err(reject("disk is in use or revision is stale"));
                }
                state.disks.remove(&key);
                state.retired_disks.insert(key);
            }
            MetadataChange::DeleteVolume {
                volume,
                expected_revision,
            } => {
                let key = (pool.clone(), volume.clone());
                let current = state
                    .volumes
                    .get(&key)
                    .ok_or_else(|| reject("unknown volume"))?;
                if current.revision != *expected_revision {
                    return Err(reject("stale volume revision"));
                }
                state.volumes.remove(&key);
                state.retired_volumes.insert(key);
            }
            MetadataChange::ReleaseAllocation {
                disk,
                expected_revision,
                volume,
                blk,
            } => {
                let receipt = (pool.clone(), disk.clone(), *blk, volume.clone());
                if state.released.contains(&receipt) {
                    return Ok(());
                }
                let key = (pool.clone(), disk.clone(), *blk);
                if !state
                    .retired_volumes
                    .contains(&(pool.clone(), volume.clone()))
                    || state.allocations.get(&key) != Some(volume)
                {
                    return Err(reject(
                        "allocation owner or volume deletion proof does not match",
                    ));
                }
                let member = state
                    .disks
                    .get_mut(&(pool.clone(), disk.clone()))
                    .ok_or_else(|| reject("unknown disk"))?;
                if member.revision != *expected_revision || !member.allocated.remove(blk) {
                    return Err(reject("stale allocation release"));
                }
                member.revision += 1;
                state.allocations.remove(&key);
                state.released.insert(receipt);
            }
            MetadataChange::AddDisk(disk) => {
                if disk.pool != *pool
                    || state.disks.keys().any(|(_, id)| *id == disk.id)
                    || state
                        .retired_disks
                        .contains(&(pool.clone(), disk.id.clone()))
                {
                    return Err(reject("disk identity already owned or wrong pool"));
                }
                state
                    .disks
                    .insert((pool.clone(), disk.id.clone()), disk.clone());
            }
            MetadataChange::DiskIo {
                disk,
                expected_revision,
                io,
            } => {
                let disk = state
                    .disks
                    .get_mut(&(pool.clone(), disk.clone()))
                    .ok_or_else(|| reject("unknown disk"))?;
                if disk.revision != *expected_revision {
                    return Err(reject("stale disk revision"));
                }
                disk.io = *io;
                disk.revision += 1;
            }
            MetadataChange::Allocate {
                disk,
                expected_revision,
                blk,
                volume,
            } => {
                let key = (pool.clone(), disk.clone(), *blk);
                let disk = state
                    .disks
                    .get_mut(&(pool.clone(), disk.clone()))
                    .ok_or_else(|| reject("unknown disk"))?;
                if disk.revision != *expected_revision
                    || !disk.allocatable()
                    || *blk >= disk.capacity_blks
                    || disk.allocated.contains(blk)
                {
                    return Err(reject("invalid allocation or stale revision"));
                }
                disk.allocated.insert(*blk);
                disk.revision += 1;
                state.allocations.insert(key, volume.clone());
            }
            MetadataChange::CreateVolume(volume) => {
                let key = (pool.clone(), volume.id.clone());
                if volume.pool != *pool
                    || state.volumes.contains_key(&key)
                    || state.retired_volumes.contains(&key)
                {
                    return Err(reject("volume exists or wrong pool"));
                }
                state.volumes.insert(key, volume.clone());
            }
            MetadataChange::InvalidateDisk { disk } => {
                for ((owner, _), volume) in &mut state.volumes {
                    if owner == pool {
                        volume.invalidate(disk);
                    }
                }
            }
        }
        state.log.push((pool.clone(), change));
        Ok(())
    }
}

#[derive(Default)]
pub struct SimulatedDataPlane;
fn ready(cancel: &CancellationToken) -> Outcome<()> {
    if cancel.is_cancelled() {
        Err(Failure::Cancelled)
    } else {
        Ok(())
    }
}
#[async_trait]
impl DataPlane for SimulatedDataPlane {
    async fn unload_pool(&self, _: &PoolId, _: &NodeId, cancel: &CancellationToken) -> Outcome<()> {
        ready(cancel)
    }
    async fn close_disk(&self, _: &PoolId, _: &DiskId, cancel: &CancellationToken) -> Outcome<()> {
        ready(cancel)
    }
    async fn delete_volume(
        &self,
        _: &PoolId,
        _: &VolumeId,
        cancel: &CancellationToken,
    ) -> Outcome<()> {
        ready(cancel)
    }
    async fn open_disk(&self, _: &PoolId, _: &DiskId, cancel: &CancellationToken) -> Outcome<()> {
        ready(cancel)
    }
    async fn load_pool(&self, _: &PoolId, _: &NodeId, cancel: &CancellationToken) -> Outcome<()> {
        ready(cancel)
    }
    async fn set_disk_io(
        &self,
        _: &PoolId,
        _: &DiskId,
        _: IoState,
        cancel: &CancellationToken,
    ) -> Outcome<()> {
        ready(cancel)
    }
    async fn initialize_bg(
        &self,
        _: &PoolId,
        _: &VolumeId,
        _: &[BgEntry],
        cancel: &CancellationToken,
    ) -> Outcome<()> {
        ready(cancel)
    }
}

mod shrink;
pub use shrink::{SimulatedBalance, SimulatedShrinkStorage};
