use crate::domains::{
    PoolId,
    disk::{Allocation, DiskId},
    vd::{VirtualDisk, VolumeId},
};
use crate::{ports::*, support::reject};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;
use workflow_foundation::Outcome;

pub(crate) enum VdCommand {
    RequireDiskUnreferenced(DiskId),
    RequireDisksUnreferenced(Vec<DiskId>),
    Delete(VolumeId),
    RequireAbsent(VolumeId),
    Create {
        id: VolumeId,
        allocations: Vec<Allocation>,
    },
    Invalidate(DiskId),
    InvalidateMany(Vec<DiskId>),
    WaitRebuildMany(Vec<DiskId>),
    /// Placeholder: wait for rebuild and committed removal of old disk references.
    WaitRebuild(DiskId),
}
pub(crate) enum VdReply {
    Deleted(VirtualDisk),
    Checked,
    Created(VirtualDisk),
    Invalidated,
}
pub(crate) struct VdService {
    deleted: Mutex<BTreeMap<VolumeId, VirtualDisk>>,
    pool: PoolId,
    volumes: Mutex<BTreeMap<VolumeId, VirtualDisk>>,
    metadata: Arc<dyn MetadataStore>,
    data_plane: Arc<dyn DataPlane>,
}
impl VdService {
    pub fn new(
        pool: PoolId,
        metadata: Arc<dyn MetadataStore>,
        data_plane: Arc<dyn DataPlane>,
    ) -> Self {
        Self {
            deleted: Mutex::default(),
            pool,
            volumes: Mutex::new(BTreeMap::new()),
            metadata,
            data_plane,
        }
    }
    pub async fn execute(
        &self,
        request: VdCommand,
        cancel: &CancellationToken,
    ) -> Outcome<VdReply> {
        if let Some(state) = self.metadata.volumes(&self.pool).await? {
            *self.volumes.lock().unwrap() = state
                .volumes
                .into_iter()
                .map(|v| (v.id.clone(), v))
                .collect();
            self.deleted
                .lock()
                .unwrap()
                .extend(state.deleted.into_iter().map(|v| (v.id.clone(), v)));
        }
        match request {
            VdCommand::RequireDisksUnreferenced(ids) => {
                if self.volumes.lock().unwrap().values().any(|volume| {
                    volume
                        .groups
                        .iter()
                        .flat_map(|g| &g.entries)
                        .any(|entry| ids.contains(&entry.disk))
                }) {
                    return Err(reject("disk is still referenced by a volume"));
                }
                Ok(VdReply::Checked)
            }
            VdCommand::RequireDiskUnreferenced(id) => {
                if self.volumes.lock().unwrap().values().any(|volume| {
                    volume
                        .groups
                        .iter()
                        .flat_map(|g| &g.entries)
                        .any(|entry| entry.disk == id)
                }) {
                    return Err(reject("disk is still referenced by a volume"));
                }
                Ok(VdReply::Checked)
            }
            VdCommand::Delete(id) => {
                if let Some(volume) = self.deleted.lock().unwrap().get(&id).cloned() {
                    return Ok(VdReply::Deleted(volume));
                }
                let volume = self
                    .volumes
                    .lock()
                    .unwrap()
                    .get(&id)
                    .cloned()
                    .ok_or_else(|| reject("unknown volume"))?;
                self.data_plane
                    .delete_volume(&self.pool, &id, cancel)
                    .await?;
                self.metadata
                    .commit(
                        &self.pool,
                        MetadataChange::DeleteVolume {
                            volume: id.clone(),
                            expected_revision: volume.revision,
                        },
                    )
                    .await
                    .map_err(|e| {
                        crate::Failure::Uncertain(format!(
                            "volume stopped but deletion not confirmed: {e}"
                        ))
                    })?;
                self.volumes.lock().unwrap().remove(&id);
                self.deleted.lock().unwrap().insert(id, volume.clone());
                Ok(VdReply::Deleted(volume))
            }
            VdCommand::RequireAbsent(id) => {
                if self.volumes.lock().unwrap().contains_key(&id)
                    || self.deleted.lock().unwrap().contains_key(&id)
                {
                    Err(reject("volume already exists"))
                } else {
                    Ok(VdReply::Checked)
                }
            }
            VdCommand::Create { id, allocations } => {
                if self.volumes.lock().unwrap().contains_key(&id)
                    || self.deleted.lock().unwrap().contains_key(&id)
                {
                    return Err(reject("volume already exists"));
                }
                let volume =
                    VirtualDisk::from_allocations(self.pool.clone(), id.clone(), allocations)?;
                self.data_plane
                    .initialize_bg(&self.pool, &id, &volume.groups[0].entries, cancel)
                    .await?;
                // Failed publication leaks allocations safely. There is no free
                // capability until reference removal/reconciliation is designed.
                self.metadata
                    .commit(&self.pool, MetadataChange::CreateVolume(volume.clone()))
                    .await?;
                self.volumes.lock().unwrap().insert(id, volume.clone());
                Ok(VdReply::Created(volume))
            }
            VdCommand::WaitRebuild(disk) => {
                self.data_plane
                    .wait_rebuild(&self.pool, &[disk], cancel)
                    .await?;
                Ok(VdReply::Checked)
            }
            VdCommand::WaitRebuildMany(disks) => {
                self.data_plane
                    .wait_rebuild(&self.pool, &disks, cancel)
                    .await?;
                Ok(VdReply::Checked)
            }
            VdCommand::InvalidateMany(disks) => {
                for disk in disks {
                    // Every confirmed commit is published, even if a later item fails.
                    self.metadata
                        .commit(
                            &self.pool,
                            MetadataChange::InvalidateDisk { disk: disk.clone() },
                        )
                        .await?;
                    for volume in self.volumes.lock().unwrap().values_mut() {
                        volume.invalidate(&disk);
                    }
                }
                Ok(VdReply::Invalidated)
            }
            VdCommand::Invalidate(disk) => {
                self.metadata
                    .commit(
                        &self.pool,
                        MetadataChange::InvalidateDisk { disk: disk.clone() },
                    )
                    .await?;
                for volume in self.volumes.lock().unwrap().values_mut() {
                    volume.invalidate(&disk);
                }
                Ok(VdReply::Invalidated)
            }
        }
    }
    pub async fn read_one(&self, id: &VolumeId) -> Outcome<VirtualDisk> {
        match self.metadata.volumes(&self.pool).await? {
            Some(state) => state
                .volumes
                .into_iter()
                .chain(state.deleted)
                .find(|volume| &volume.id == id)
                .ok_or_else(|| reject("unknown volume")),
            None => self
                .query(Some(id.clone()))
                .map(|mut volumes| volumes.remove(0)),
        }
    }
    pub async fn read(&self) -> Outcome<Vec<VirtualDisk>> {
        match self.metadata.volumes(&self.pool).await? {
            Some(state) => Ok(state.volumes),
            None => self.query(None),
        }
    }
    pub async fn authoritative(&self) -> Outcome<VdMetadata> {
        self.metadata
            .volumes(&self.pool)
            .await?
            .ok_or_else(|| reject("shrink requires authoritative VD metadata reads"))
    }
    pub fn query(&self, id: Option<VolumeId>) -> Outcome<Vec<VirtualDisk>> {
        let volumes = self.volumes.lock().unwrap();
        match id {
            Some(id) => Ok(vec![
                volumes
                    .get(&id)
                    .cloned()
                    .or_else(|| self.deleted.lock().unwrap().get(&id).cloned())
                    .ok_or_else(|| reject("unknown volume"))?,
            ]),
            None => Ok(volumes.values().cloned().collect()),
        }
    }
}
