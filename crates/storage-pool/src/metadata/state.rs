//! Business invariants shared by every KV adapter. Apply only to a private transaction view.
use crate::{ports::*, support::reject, *};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Default, Clone)]
pub(super) struct State {
    pub tiers: BTreeMap<(PoolId, TierId), Tier>,
    pub retired_tiers: BTreeSet<(PoolId, TierId)>,
    pub retired_disks: BTreeSet<(PoolId, DiskId)>,
    pub retired_volumes: BTreeSet<(PoolId, VolumeId)>,
    pub allocations: BTreeMap<(PoolId, DiskId, u32), VolumeId>,
    pub released: BTreeSet<(PoolId, DiskId, u32, VolumeId)>,
    pub disks: BTreeMap<(PoolId, DiskId), MemberDisk>,
    pub volumes: BTreeMap<(PoolId, VolumeId), VirtualDisk>,
    pub deleted_volumes: BTreeMap<(PoolId, VolumeId), VirtualDisk>,
}
impl State {
    pub fn apply(&mut self, pool: &PoolId, change: &MetadataChange) -> Outcome<()> {
        match &change {
            MetadataChange::ShrinkIntent {
                disk,
                expected_revision,
                job,
            }
            | MetadataChange::DisableAllocation {
                disk,
                expected_revision,
                job,
            }
            | MetadataChange::ClearShrink {
                disk,
                expected_revision,
                job,
            }
            | MetadataChange::MarkRemoved {
                disk,
                expected_revision,
                job,
            } => {
                let key = (pool.clone(), disk.clone());
                let mut member = self
                    .disks
                    .get(&key)
                    .cloned()
                    .ok_or_else(|| reject("unknown disk"))?;
                if member.revision != *expected_revision
                    || member.membership == DiskMembership::Removed
                {
                    return Err(reject("stale or removed disk"));
                }
                match &change {
                    MetadataChange::ShrinkIntent { .. } => {
                        if member.shrink.is_some_and(|existing| existing != *job) {
                            return Err(reject("disk has another shrink intent"));
                        }
                        member.shrink = Some(*job);
                    }
                    _ if member.shrink != Some(*job) => {
                        return Err(reject("shrink intent does not match"));
                    }
                    MetadataChange::DisableAllocation { .. } => {
                        member.allocation = AllocationState::Inactive
                    }
                    MetadataChange::ClearShrink { .. } => {
                        member.shrink = None;
                        member.allocation = AllocationState::Inactive;
                    }
                    MetadataChange::MarkRemoved { .. } => {
                        if member.io != IoState::Down
                            || member.allocation != AllocationState::Inactive
                            || !member.allocated.is_empty()
                            || self.volumes.values().any(|v| {
                                v.pool == *pool
                                    && v.groups
                                        .iter()
                                        .flat_map(|g| &g.entries)
                                        .any(|e| e.disk == *disk)
                            })
                        {
                            return Err(reject(
                                "removed requires down, inactive and no references or allocations",
                            ));
                        }
                        member.membership = DiskMembership::Removed;
                        self.retired_disks.insert(key.clone());
                    }
                    _ => unreachable!(),
                }
                member.revision += 1;
                self.disks.insert(key, member);
            }
            MetadataChange::Remap {
                volume,
                expected_revision,
                group,
                source,
                target,
                source_revision,
                target_revision,
            } => {
                let volume_key = (pool.clone(), volume.clone());
                let mut vd = self
                    .volumes
                    .get(&volume_key)
                    .cloned()
                    .ok_or_else(|| reject("unknown volume"))?;
                let mut from = self
                    .disks
                    .get(&(pool.clone(), source.disk.clone()))
                    .cloned()
                    .ok_or_else(|| reject("unknown source"))?;
                let mut to = self
                    .disks
                    .get(&(pool.clone(), target.disk.clone()))
                    .cloned()
                    .ok_or_else(|| reject("unknown target"))?;
                let bg = vd
                    .groups
                    .iter_mut()
                    .find(|g| g.index == *group)
                    .ok_or_else(|| reject("unknown block group"))?;
                let slot = bg
                    .entries
                    .iter()
                    .position(|e| e == source)
                    .ok_or_else(|| reject("source placement changed"))?;
                if vd.revision != *expected_revision
                    || from.revision != *source_revision
                    || to.revision != *target_revision
                    || !to.allocatable()
                    || from.id == to.id
                    || from.tier != to.tier
                    || target.data != DataState::Valid
                    || target.blk >= to.capacity_blks
                    || to.allocated.contains(&target.blk)
                    || !from.allocated.contains(&source.blk)
                    || self
                        .allocations
                        .get(&(pool.clone(), source.disk.clone(), source.blk))
                        != Some(volume)
                    || bg.entries.iter().enumerate().any(|(i, e)| {
                        i != slot
                            && self
                                .disks
                                .get(&(pool.clone(), e.disk.clone()))
                                .is_none_or(|d| d.node == to.node)
                    })
                {
                    return Err(reject("invalid remap or stale placement revision"));
                }
                bg.entries[slot] = target.clone();
                vd.revision += 1;
                from.allocated.remove(&source.blk);
                from.revision += 1;
                to.allocated.insert(target.blk);
                to.revision += 1;
                self.allocations
                    .remove(&(pool.clone(), source.disk.clone(), source.blk));
                self.allocations.insert(
                    (pool.clone(), target.disk.clone(), target.blk),
                    volume.clone(),
                );
                self.disks.insert((pool.clone(), from.id.clone()), from);
                self.disks.insert((pool.clone(), to.id.clone()), to);
                self.volumes.insert(volume_key, vd);
            }
            MetadataChange::CreateTier(tier) => {
                let key = (pool.clone(), tier.id.clone());
                if tier.pool != *pool
                    || self.tiers.contains_key(&key)
                    || self.retired_tiers.contains(&key)
                {
                    return Err(reject("tier identity already used or wrong pool"));
                }
                self.tiers.insert(key, tier.clone());
            }
            MetadataChange::DeleteTier {
                id,
                expected_revision,
            } => {
                let key = (pool.clone(), id.clone());
                let tier = self.tiers.get(&key).ok_or_else(|| reject("unknown tier"))?;
                if tier.revision != *expected_revision
                    || self.disks.values().any(|d| {
                        d.pool == *pool && d.tier == *id && d.membership == DiskMembership::Member
                    })
                {
                    return Err(reject("tier is referenced or revision is stale"));
                }
                self.tiers.remove(&key);
                self.retired_tiers.insert(key);
            }
            MetadataChange::RemoveDisk {
                disk,
                expected_revision,
            } => {
                let key = (pool.clone(), disk.clone());
                let member = self.disks.get(&key).ok_or_else(|| reject("unknown disk"))?;
                if member.revision != *expected_revision
                    || member.shrink.is_some()
                    || member.io != IoState::Down
                    || !member.allocated.is_empty()
                    || self.volumes.values().any(|v| {
                        v.pool == *pool
                            && v.groups
                                .iter()
                                .flat_map(|g| &g.entries)
                                .any(|e| e.disk == *disk)
                    })
                {
                    return Err(reject("disk is in use or revision is stale"));
                }
                self.disks.remove(&key);
                self.retired_disks.insert(key);
            }
            MetadataChange::DeleteVolume {
                volume,
                expected_revision,
            } => {
                let key = (pool.clone(), volume.clone());
                let current = self
                    .volumes
                    .get(&key)
                    .ok_or_else(|| reject("unknown volume"))?;
                if current.revision != *expected_revision {
                    return Err(reject("stale volume revision"));
                }
                self.deleted_volumes.insert(key.clone(), current.clone());
                self.volumes.remove(&key);
                self.retired_volumes.insert(key);
            }
            MetadataChange::ReleaseAllocation {
                disk,
                expected_revision,
                volume,
                blk,
            } => {
                let receipt = (pool.clone(), disk.clone(), *blk, volume.clone());
                if self.released.contains(&receipt) {
                    return Ok(());
                }
                let key = (pool.clone(), disk.clone(), *blk);
                if !self
                    .retired_volumes
                    .contains(&(pool.clone(), volume.clone()))
                    || self.allocations.get(&key) != Some(volume)
                {
                    return Err(reject(
                        "allocation owner or volume deletion proof does not match",
                    ));
                }
                let member = self
                    .disks
                    .get_mut(&(pool.clone(), disk.clone()))
                    .ok_or_else(|| reject("unknown disk"))?;
                if member.revision != *expected_revision || !member.allocated.remove(blk) {
                    return Err(reject("stale allocation release"));
                }
                member.revision += 1;
                self.allocations.remove(&key);
                self.released.insert(receipt);
            }
            MetadataChange::AddDisk(disk) => {
                if disk.pool != *pool
                    || self
                        .disks
                        .values()
                        .any(|d| d.id == disk.id && d.membership == DiskMembership::Member)
                    || self
                        .retired_disks
                        .contains(&(pool.clone(), disk.id.clone()))
                {
                    return Err(reject("disk identity already owned or wrong pool"));
                }
                self.disks
                    .insert((pool.clone(), disk.id.clone()), disk.clone());
            }
            MetadataChange::DiskIo {
                disk,
                expected_revision,
                io,
            } => {
                let disk = self
                    .disks
                    .get_mut(&(pool.clone(), disk.clone()))
                    .ok_or_else(|| reject("unknown disk"))?;
                if disk.revision != *expected_revision || disk.membership == DiskMembership::Removed
                {
                    return Err(reject("stale or removed disk revision"));
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
                let disk = self
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
                self.allocations.insert(key, volume.clone());
            }
            MetadataChange::CreateVolume(volume) => {
                let key = (pool.clone(), volume.id.clone());
                if volume.pool != *pool
                    || self.volumes.contains_key(&key)
                    || self.retired_volumes.contains(&key)
                {
                    return Err(reject("volume exists or wrong pool"));
                }
                self.volumes.insert(key, volume.clone());
            }
            MetadataChange::InvalidateDisk { disk } => {
                for ((owner, _), volume) in &mut self.volumes {
                    if owner == pool {
                        volume.invalidate(disk);
                    }
                }
            }
        }
        Ok(())
    }
}
