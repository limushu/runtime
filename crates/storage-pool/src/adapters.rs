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
#[derive(Clone, Default)]
struct MemoryState {
    tiers: BTreeMap<(PoolId, TierId), Tier>,
    retired_tiers: BTreeSet<(PoolId, TierId)>,
    retired_disks: BTreeSet<(PoolId, DiskId)>,
    retired_volumes: BTreeSet<(PoolId, VolumeId)>,
    deleted_volumes: BTreeMap<(PoolId, VolumeId), VirtualDisk>,
    allocations: BTreeMap<(PoolId, DiskId, u32), VolumeId>,
    released: BTreeSet<(PoolId, DiskId, u32, VolumeId)>,
    log: Vec<(PoolId, MetadataChange)>,
    disks: BTreeMap<(PoolId, DiskId), MemberDisk>,
    volumes: BTreeMap<(PoolId, VolumeId), VirtualDisk>,
}
impl MemoryMetadata {
    /// Simulator of the C side's committed BG migration. All changes below use
    /// the same metadata consumed by Disk/VD; this is not a second shrink catalogue.
    pub fn evacuate(&self, pool: &PoolId, targets: &[DiskId]) -> Outcome<()> {
        let mut guard = self.state.lock().unwrap();
        let mut next = guard.clone();
        let volumes: Vec<_> = next
            .volumes
            .values()
            .filter(|v| &v.pool == pool)
            .cloned()
            .collect();
        for mut volume in volumes {
            let mut changed = false;
            for group in &mut volume.groups {
                for index in 0..group.entries.len() {
                    let old = group.entries[index].clone();
                    if !targets.contains(&old.disk) {
                        continue;
                    }
                    let source = next
                        .disks
                        .get(&(pool.clone(), old.disk.clone()))
                        .ok_or_else(|| reject("missing source disk"))?
                        .clone();
                    if source.allocation != AllocationState::Inactive {
                        return Err(reject("source allocation is still enabled"));
                    }
                    let nodes: BTreeSet<_> = group
                        .entries
                        .iter()
                        .enumerate()
                        .filter(|(i, _)| *i != index)
                        .filter_map(|(_, e)| {
                            next.disks
                                .get(&(pool.clone(), e.disk.clone()))
                                .map(|d| d.node.clone())
                        })
                        .collect();
                    let destination = next
                        .disks
                        .values()
                        .find(|d| {
                            d.pool == *pool
                                && d.tier == source.tier
                                && !targets.contains(&d.id)
                                && !nodes.contains(&d.node)
                                && d.allocatable()
                                && d.allocated.len() < d.capacity_blks as usize
                        })
                        .cloned()
                        .ok_or_else(|| reject("no valid destination for migration"))?;
                    let blk = (0..destination.capacity_blks)
                        .find(|b| !destination.allocated.contains(b))
                        .unwrap();
                    if next
                        .allocations
                        .get(&(pool.clone(), old.disk.clone(), old.blk))
                        != Some(&volume.id)
                    {
                        return Err(reject("source allocation owner mismatch"));
                    }
                    let disk = next
                        .disks
                        .get_mut(&(pool.clone(), old.disk.clone()))
                        .unwrap();
                    disk.allocated.remove(&old.blk);
                    disk.revision += 1;
                    let disk = next
                        .disks
                        .get_mut(&(pool.clone(), destination.id.clone()))
                        .unwrap();
                    disk.allocated.insert(blk);
                    disk.revision += 1;
                    next.allocations.remove(&(pool.clone(), old.disk, old.blk));
                    next.allocations.insert(
                        (pool.clone(), destination.id.clone(), blk),
                        volume.id.clone(),
                    );
                    group.entries[index] = BgEntry {
                        disk: destination.id,
                        blk,
                        data: old.data,
                    };
                    changed = true;
                }
            }
            if changed {
                volume.revision += 1;
                next.volumes
                    .insert((pool.clone(), volume.id.clone()), volume);
            }
        }
        if next
            .disks
            .values()
            .any(|d| d.pool == *pool && targets.contains(&d.id) && !d.allocated.is_empty())
        {
            return Err(reject(
                "unreferenced allocations still require reconciliation",
            ));
        }
        *guard = next;
        Ok(())
    }
    pub fn committed(&self) -> Vec<(PoolId, MetadataChange)> {
        self.state.lock().unwrap().log.clone()
    }
}
#[async_trait]
impl MetadataStore for MemoryMetadata {
    async fn disks(&self, pool: &PoolId) -> Outcome<Option<DiskMetadata>> {
        let state = self.state.lock().unwrap();
        Ok(Some(DiskMetadata {
            disks: state
                .disks
                .values()
                .filter(|d| &d.pool == pool)
                .cloned()
                .collect(),
            allocations: state
                .allocations
                .iter()
                .filter(|((p, _, _), _)| p == pool)
                .map(|((_, disk, blk), volume)| (disk.clone(), *blk, volume.clone()))
                .collect(),
            retired: state
                .retired_disks
                .iter()
                .filter(|(p, _)| p == pool)
                .map(|(_, d)| d.clone())
                .collect(),
        }))
    }
    async fn volumes(&self, pool: &PoolId) -> Outcome<Option<VdMetadata>> {
        let state = self.state.lock().unwrap();
        Ok(Some(VdMetadata {
            volumes: state
                .volumes
                .values()
                .filter(|v| &v.pool == pool)
                .cloned()
                .collect(),
            deleted: state
                .deleted_volumes
                .values()
                .filter(|v| &v.pool == pool)
                .cloned()
                .collect(),
        }))
    }
    async fn commit(&self, pool: &PoolId, change: MetadataChange) -> Outcome<()> {
        let mut state = self.state.lock().unwrap();
        match &change {
            MetadataChange::RequestShrink { disks }
            | MetadataChange::DisableAllocation { disks }
            | MetadataChange::RemoveDisks { disks } => {
                let mut seen = BTreeSet::new();
                for (id, revision) in disks {
                    let disk = state
                        .disks
                        .get(&(pool.clone(), id.clone()))
                        .ok_or_else(|| reject("unknown disk"))?;
                    if !seen.insert(id) || disk.revision != *revision {
                        return Err(reject("duplicate disk or stale revision"));
                    }
                    if matches!(change, MetadataChange::DisableAllocation { .. })
                        && !disk.shrink_requested
                    {
                        return Err(reject(
                            "allocation disable requires persisted shrink intent",
                        ));
                    }
                    if matches!(change, MetadataChange::RemoveDisks { .. })
                        && (disk.io != IoState::Down
                            || !disk.allocated.is_empty()
                            || state.volumes.values().any(|v| {
                                v.pool == *pool
                                    && v.groups
                                        .iter()
                                        .flat_map(|g| &g.entries)
                                        .any(|e| e.disk == *id)
                            }))
                    {
                        return Err(reject("disk is still in use"));
                    }
                }
                for (id, _) in disks {
                    let key = (pool.clone(), id.clone());
                    if matches!(change, MetadataChange::RemoveDisks { .. }) {
                        state.disks.remove(&key);
                        state.retired_disks.insert(key);
                    } else {
                        let disk = state.disks.get_mut(&key).unwrap();
                        if matches!(change, MetadataChange::RequestShrink { .. }) {
                            disk.shrink_requested = true;
                        } else {
                            disk.allocation = AllocationState::Inactive;
                        }
                        disk.revision += 1;
                    }
                }
            }
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
                let volume = state.volumes.remove(&key).unwrap();
                state.deleted_volumes.insert(key.clone(), volume);
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

mod balance;
pub use balance::SimulatedBalance;
