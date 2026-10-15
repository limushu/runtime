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
    Checked,
    Created(VirtualDisk),
    Invalidated,
}
pub(crate) struct VdService {
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
        match request {
            VdCommand::RequireAbsent(id) => {
                if self.volumes.lock().unwrap().contains_key(&id) {
                    Err(reject("volume already exists"))
                } else {
                    Ok(VdReply::Checked)
                }
            }
            VdCommand::Create { id, allocations } => {
                if self.volumes.lock().unwrap().contains_key(&id) {
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
    pub fn query(&self, id: Option<VolumeId>) -> Outcome<Vec<VirtualDisk>> {
        let volumes = self.volumes.lock().unwrap();
        match id {
            Some(id) => Ok(vec![
                volumes
                    .get(&id)
                    .cloned()
                    .ok_or_else(|| reject("unknown volume"))?,
            ]),
            None => Ok(volumes.values().cloned().collect()),
        }
    }
}
