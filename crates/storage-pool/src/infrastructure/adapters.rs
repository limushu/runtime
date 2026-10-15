//! Explicit simulators, not persistence or real user_dp implementations.
use crate::{
    domain::{ports::*, reject},
    *,
};
use async_trait::async_trait;
use std::{collections::BTreeMap, sync::Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
pub struct MemoryMetadata {
    state: Mutex<MemoryState>,
}
#[derive(Default)]
struct MemoryState {
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
            MetadataChange::AddDisk(disk) => {
                if disk.pool != *pool || state.disks.keys().any(|(_, id)| *id == disk.id) {
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
                ..
            } => {
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
            }
            MetadataChange::CreateVolume(volume) => {
                let key = (pool.clone(), volume.id.clone());
                if volume.pool != *pool || state.volumes.contains_key(&key) {
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
