use crate::domains::{
    PoolId,
    disk::{Allocation, AllocationState, DiskId, DiskMembership, DiskSpec, IoState, MemberDisk},
    vd::VolumeId,
};
use crate::{ports::*, support::reject};
use std::{collections::BTreeMap, sync::Arc};
use tokio_util::sync::CancellationToken;
use workflow_foundation::{Failure, Outcome};

#[derive(Clone)]
pub(crate) enum DiskCommand {
    ShrinkIntent(DiskId, crate::domains::balance::EvacuationId),
    DisableAllocation(DiskId, crate::domains::balance::EvacuationId),
    ClearShrink(DiskId, crate::domains::balance::EvacuationId),
    RequireEmpty(DiskId),
    FinalizeShrink(DiskId, crate::domains::balance::EvacuationId),
    RequireTierEmpty(crate::TierId),
    RemoveEmpty(DiskId),
    Release {
        disk: DiskId,
        volume: VolumeId,
        blk: u32,
    },
    Inspect(DiskId),
    InspectMany(Vec<DiskId>),
    SetIoMany(Vec<DiskId>, IoState),
    RemoveMany(Vec<DiskId>),
    Add(DiskSpec),
    Open(DiskId),
    SetIo(DiskId, IoState),
    Allocate(DiskId, VolumeId),
    /// Placeholder: remove membership only after VD confirms rebuild completion.
    Remove(DiskId),
}
pub(crate) enum DiskReply {
    Checked,
    Disk(MemberDisk),
    Allocated(Allocation),
    Disks(Vec<MemberDisk>),
}
enum ShrinkMutation {
    Intent,
    Disable,
    Clear,
}
pub(crate) struct DiskService {
    pool: PoolId,
    metadata: Arc<dyn MetadataStore>,
    data_plane: Arc<dyn DataPlane>,
}
impl DiskService {
    pub fn new(
        pool: PoolId,
        metadata: Arc<dyn MetadataStore>,
        data_plane: Arc<dyn DataPlane>,
    ) -> Self {
        Self {
            pool,
            metadata,
            data_plane,
        }
    }
    pub async fn execute(
        &self,
        request: DiskCommand,
        cancel: &CancellationToken,
    ) -> Outcome<DiskReply> {
        let snapshot = self.metadata.snapshot(&self.pool).await?;
        let shrink_mutation = match &request {
            DiskCommand::ShrinkIntent(..) => Some(ShrinkMutation::Intent),
            DiskCommand::DisableAllocation(..) => Some(ShrinkMutation::Disable),
            DiskCommand::ClearShrink(..) => Some(ShrinkMutation::Clear),
            _ => None,
        };
        match request {
            DiskCommand::ShrinkIntent(id, job)
            | DiskCommand::DisableAllocation(id, job)
            | DiskCommand::ClearShrink(id, job) => {
                let mut disk = snapshot.disk(&id)?;
                if disk.membership == DiskMembership::Removed {
                    return Err(reject("disk is removed"));
                }
                let change = match shrink_mutation {
                    Some(ShrinkMutation::Intent) => {
                        if disk.shrink == Some(job) {
                            return Ok(DiskReply::Disk(disk));
                        }
                        MetadataChange::ShrinkIntent {
                            disk: id.clone(),
                            expected_revision: disk.revision,
                            job,
                        }
                    }
                    Some(ShrinkMutation::Disable) => {
                        if disk.shrink == Some(job) && disk.allocation == AllocationState::Inactive
                        {
                            return Ok(DiskReply::Disk(disk));
                        }
                        MetadataChange::DisableAllocation {
                            disk: id.clone(),
                            expected_revision: disk.revision,
                            job,
                        }
                    }
                    _ => MetadataChange::ClearShrink {
                        disk: id.clone(),
                        expected_revision: disk.revision,
                        job,
                    },
                };
                self.metadata.commit(&self.pool, change.clone()).await?;
                match change {
                    MetadataChange::ShrinkIntent { .. } => disk.shrink = Some(job),
                    MetadataChange::DisableAllocation { .. } => {
                        disk.allocation = AllocationState::Inactive
                    }
                    _ => {
                        disk.shrink = None;
                        disk.allocation = AllocationState::Inactive;
                    }
                }
                disk.revision += 1;
                Ok(DiskReply::Disk(disk))
            }
            DiskCommand::RequireEmpty(id) => {
                if !snapshot.disk(&id)?.allocated.is_empty() {
                    return Err(reject("disk still has allocations"));
                }
                Ok(DiskReply::Checked)
            }
            DiskCommand::FinalizeShrink(id, job) => {
                let mut disk = snapshot.disk(&id)?;
                if disk.membership == DiskMembership::Removed && disk.shrink == Some(job) {
                    return Ok(DiskReply::Disk(disk));
                }
                if disk.shrink != Some(job)
                    || disk.allocation != AllocationState::Inactive
                    || !disk.allocated.is_empty()
                {
                    return Err(reject("shrink intent or allocation verification failed"));
                }
                let snapshot = self.metadata.snapshot(&self.pool).await?;
                if snapshot
                    .volumes
                    .iter()
                    .flat_map(|v| &v.groups)
                    .flat_map(|g| &g.entries)
                    .any(|e| e.disk == id)
                {
                    return Err(reject("disk still has committed references"));
                }
                // One short, non-pausable finalization boundary: topo cannot reopen the
                // disk between confirmed Down and Removed. Never drop an issued IO future.
                self.data_plane
                    .set_disk_io(&self.pool, &id, IoState::Down, cancel)
                    .await?;
                self.metadata
                    .commit(
                        &self.pool,
                        MetadataChange::DiskIo {
                            disk: id.clone(),
                            expected_revision: disk.revision,
                            io: IoState::Down,
                        },
                    )
                    .await
                    .map_err(|e| {
                        Failure::Uncertain(format!(
                            "shrink IO down but metadata not confirmed: {e}"
                        ))
                    })?;
                disk.io = IoState::Down;
                disk.revision += 1;
                self.data_plane.close_disk(&self.pool, &id, cancel).await?;
                self.metadata
                    .commit(
                        &self.pool,
                        MetadataChange::MarkRemoved {
                            disk: id.clone(),
                            expected_revision: disk.revision,
                            job,
                        },
                    )
                    .await
                    .map_err(|e| {
                        Failure::Uncertain(format!(
                            "shrink disk closed but Removed not confirmed: {e}"
                        ))
                    })?;
                disk.membership = DiskMembership::Removed;
                disk.revision += 1;
                Ok(DiskReply::Disk(disk))
            }
            DiskCommand::RequireTierEmpty(tier) => {
                if snapshot
                    .disks
                    .iter()
                    .any(|disk| disk.tier == tier && disk.membership == DiskMembership::Member)
                {
                    return Err(reject("tier still has member disks"));
                }
                Ok(DiskReply::Checked)
            }
            DiskCommand::Release {
                disk: id,
                volume,
                blk,
            } => {
                if snapshot
                    .released
                    .iter()
                    .any(|a| a.disk == id && a.blk == blk && a.volume == volume)
                {
                    return Ok(DiskReply::Checked);
                }
                let mut disk = snapshot.disk(&id)?;
                if !snapshot
                    .allocations
                    .iter()
                    .any(|a| a.disk == id && a.blk == blk && a.volume == volume)
                {
                    return Err(reject("allocation belongs to another volume"));
                }
                if !disk.allocated.contains(&blk) {
                    return Err(reject("allocation is missing"));
                }
                self.metadata
                    .commit(
                        &self.pool,
                        MetadataChange::ReleaseAllocation {
                            disk: id.clone(),
                            expected_revision: disk.revision,
                            volume,
                            blk,
                        },
                    )
                    .await?;
                disk.allocated.remove(&blk);
                disk.revision += 1;
                Ok(DiskReply::Checked)
            }
            DiskCommand::RemoveEmpty(id) => {
                if snapshot.retired_disks.contains(&id) {
                    return Ok(DiskReply::Checked);
                }
                let disk = snapshot.disk(&id)?;
                if disk.shrink.is_some() {
                    return Err(reject("disk has a shrink intent; use shrink finalization"));
                }
                if !disk.allocated.is_empty() || disk.io != IoState::Down {
                    return Err(reject("disk must be down and have no allocations"));
                }
                self.data_plane.close_disk(&self.pool, &id, cancel).await?;
                self.metadata
                    .commit(
                        &self.pool,
                        MetadataChange::RemoveDisk {
                            disk: id.clone(),
                            expected_revision: disk.revision,
                        },
                    )
                    .await
                    .map_err(|e| {
                        Failure::Uncertain(format!("disk closed but removal not confirmed: {e}"))
                    })?;
                Ok(DiskReply::Checked)
            }
            DiskCommand::Inspect(id) => Ok(DiskReply::Disk(snapshot.disk(&id)?)),
            DiskCommand::InspectMany(ids) => {
                // Routing also includes reserved disks whose Add never committed.
                Ok(DiskReply::Disks(
                    ids.iter()
                        .filter_map(|id| snapshot.disks.iter().find(|d| &d.id == id).cloned())
                        .collect(),
                ))
            }
            DiskCommand::RemoveMany(_) => Err(Failure::Failed(
                "member disk removal is not implemented".into(),
            )),
            DiskCommand::SetIoMany(ids, io) => {
                let mut unique = std::collections::BTreeSet::new();
                let mut disks = Vec::new();
                for id in &ids {
                    if !unique.insert(id.clone()) {
                        return Err(reject("duplicate disk in IO batch"));
                    }
                    let disk = snapshot.disk(id)?;
                    if disk.membership == DiskMembership::Removed {
                        return Err(reject("IO batch contains a removed disk"));
                    }
                    disks.push(disk);
                }
                let results = self
                    .data_plane
                    .set_disks_io(&self.pool, &ids, io, cancel)
                    .await
                    .map_err(|error| {
                        Failure::Uncertain(format!("batch IO not confirmed: {error}"))
                    })?;
                let returned: std::collections::BTreeSet<_> =
                    results.iter().map(|(id, _)| id.clone()).collect();
                if results.len() != ids.len() || returned != unique {
                    return Err(Failure::Uncertain(
                        "invalid batch IO response identities".into(),
                    ));
                }
                let results: BTreeMap<_, _> = results.into_iter().collect();
                let mut failure = None;
                for disk in &mut disks {
                    match &results[&disk.id] {
                        Ok(()) => {
                            let committed = self
                                .metadata
                                .commit(
                                    &self.pool,
                                    MetadataChange::DiskIo {
                                        disk: disk.id.clone(),
                                        expected_revision: disk.revision,
                                        io,
                                    },
                                )
                                .await;
                            if let Err(error) = committed {
                                failure = Some(Failure::Uncertain(format!(
                                    "batch IO applied but metadata not confirmed: {error}"
                                )));
                            } else {
                                disk.io = io;
                                disk.revision += 1;
                            }
                        }
                        Err(error) => {
                            if failure
                                .as_ref()
                                .is_none_or(|e| !matches!(e, Failure::Uncertain(_)))
                            {
                                failure = Some(error.clone());
                            }
                        }
                    }
                }
                match failure {
                    Some(error) => Err(error),
                    None => Ok(DiskReply::Disks(disks)),
                }
            }
            DiskCommand::Add(spec) => {
                if snapshot.retired_disks.contains(&spec.id) {
                    return Err(reject("disk membership identity is retired"));
                }
                if let Some(existing) = snapshot.disks.iter().find(|d| d.id == spec.id).cloned() {
                    existing.require_identity(&spec)?;
                    return Ok(DiskReply::Disk(existing));
                }
                let disk = MemberDisk::new(self.pool.clone(), spec)?;
                self.metadata
                    .commit(&self.pool, MetadataChange::AddDisk(disk.clone()))
                    .await?;
                Ok(DiskReply::Disk(disk))
            }
            DiskCommand::Open(id) => {
                let disk = snapshot.disk(&id)?;
                if disk.membership == DiskMembership::Removed {
                    return Err(reject("disk is removed"));
                }
                self.data_plane.open_disk(&self.pool, &id, cancel).await?;
                Ok(DiskReply::Disk(disk))
            }
            DiskCommand::SetIo(id, io) => {
                let mut disk = snapshot.disk(&id)?;
                if disk.membership == DiskMembership::Removed {
                    return Err(reject("disk is removed"));
                }
                // Reassert the external effect even for repeated metadata state.
                self.data_plane
                    .set_disk_io(&self.pool, &id, io, cancel)
                    .await?;
                self.metadata
                    .commit(
                        &self.pool,
                        MetadataChange::DiskIo {
                            disk: id.clone(),
                            expected_revision: disk.revision,
                            io,
                        },
                    )
                    .await
                    .map_err(|e| {
                        Failure::Uncertain(format!("IO applied but metadata not confirmed: {e}"))
                    })?;
                disk.io = io;
                disk.revision += 1;
                Ok(DiskReply::Disk(disk))
            }
            DiskCommand::Remove(id) => {
                // TODO: validate no-reference evidence and commit membership removal.
                Err(Failure::Failed(format!(
                    "member disk removal is not implemented for disk {}",
                    id.as_str()
                )))
            }
            DiskCommand::Allocate(id, volume) => {
                let mut disk = snapshot.disk(&id)?;
                let blk = disk.next_free_block()?;
                self.metadata
                    .commit(
                        &self.pool,
                        MetadataChange::Allocate {
                            disk: id.clone(),
                            expected_revision: disk.revision,
                            volume: volume.clone(),
                            blk,
                        },
                    )
                    .await?;
                disk.allocated.insert(blk);
                disk.revision += 1;
                let receipt = Allocation {
                    pool: self.pool.clone(),
                    volume,
                    disk: id.clone(),
                    node: disk.node.clone(),
                    tier: disk.tier.clone(),
                    blk,
                };
                Ok(DiskReply::Allocated(receipt))
            }
        }
    }
}
