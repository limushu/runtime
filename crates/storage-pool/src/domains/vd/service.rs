use crate::domains::{
    PoolId,
    disk::{Allocation, DiskId},
    vd::{VirtualDisk, VolumeId},
};
use crate::{ports::*, support::reject};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use workflow_foundation::Outcome;

pub(crate) enum VdCommand {
    RequireDiskUnreferenced(DiskId),
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
    pool: PoolId,
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
            metadata,
            data_plane,
        }
    }
    pub async fn execute(
        &self,
        request: VdCommand,
        cancel: &CancellationToken,
    ) -> Outcome<VdReply> {
        let snapshot = self.metadata.snapshot(&self.pool).await?;
        match request {
            VdCommand::RequireDiskUnreferenced(id) => {
                if snapshot.volumes.iter().any(|volume| {
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
                if let Some(volume) = snapshot
                    .deleted_volumes
                    .iter()
                    .find(|v| v.id == id)
                    .cloned()
                {
                    return Ok(VdReply::Deleted(volume));
                }
                let volume = snapshot.volume(&id)?;
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
                Ok(VdReply::Deleted(volume))
            }
            VdCommand::RequireAbsent(id) => {
                if snapshot.volumes.iter().any(|v| v.id == id)
                    || snapshot.deleted_volumes.iter().any(|v| v.id == id)
                {
                    Err(reject("volume already exists"))
                } else {
                    Ok(VdReply::Checked)
                }
            }
            VdCommand::Create { id, allocations } => {
                if snapshot.volumes.iter().any(|v| v.id == id)
                    || snapshot.deleted_volumes.iter().any(|v| v.id == id)
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
                Ok(VdReply::Invalidated)
            }
        }
    }
}
