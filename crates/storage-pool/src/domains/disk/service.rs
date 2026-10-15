use crate::domains::{
    PoolId,
    disk::{Allocation, DiskId, DiskSpec, IoState, MemberDisk},
    vd::VolumeId,
};
use crate::{ports::*, support::reject};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio_util::sync::CancellationToken;
use workflow_foundation::{Failure, Outcome};

pub(crate) enum DiskCommand {
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
    Disk(MemberDisk),
    Allocated(Allocation),
    Disks(Vec<MemberDisk>),
}
pub(crate) struct DiskService {
    pool: PoolId,
    disks: Mutex<BTreeMap<DiskId, MemberDisk>>,
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
            disks: Mutex::new(BTreeMap::new()),
            metadata,
            data_plane,
        }
    }
    fn get(&self, id: &DiskId) -> Outcome<MemberDisk> {
        self.disks
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| reject("unknown member disk"))
    }
    pub async fn execute(
        &self,
        request: DiskCommand,
        cancel: &CancellationToken,
    ) -> Outcome<DiskReply> {
        // Only short in-memory access uses Mutex. No guard crosses an await;
        // object conflicts are resolved by the Pool TaskCenter before execution.
        match request {
            DiskCommand::Inspect(id) => Ok(DiskReply::Disk(self.get(&id)?)),
            DiskCommand::InspectMany(ids) => {
                // Routing also includes reserved disks whose Add never committed.
                let disks = self.disks.lock().unwrap();
                Ok(DiskReply::Disks(
                    ids.iter().filter_map(|id| disks.get(id).cloned()).collect(),
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
                    disks.push(self.get(id)?);
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
                                self.disks
                                    .lock()
                                    .unwrap()
                                    .insert(disk.id.clone(), disk.clone());
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
                if let Some(existing) = self.disks.lock().unwrap().get(&spec.id).cloned() {
                    existing.require_identity(&spec)?;
                    return Ok(DiskReply::Disk(existing));
                }
                let disk = MemberDisk::new(self.pool.clone(), spec)?;
                self.metadata
                    .commit(&self.pool, MetadataChange::AddDisk(disk.clone()))
                    .await?;
                self.disks
                    .lock()
                    .unwrap()
                    .insert(disk.id.clone(), disk.clone());
                Ok(DiskReply::Disk(disk))
            }
            DiskCommand::Open(id) => {
                let disk = self.get(&id)?;
                self.data_plane.open_disk(&self.pool, &id, cancel).await?;
                Ok(DiskReply::Disk(disk))
            }
            DiskCommand::SetIo(id, io) => {
                let mut disk = self.get(&id)?;
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
                self.disks.lock().unwrap().insert(id, disk.clone());
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
                let mut disk = self.get(&id)?;
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
                self.disks.lock().unwrap().insert(id, disk);
                Ok(DiskReply::Allocated(receipt))
            }
        }
    }
    pub fn query(&self, id: Option<DiskId>) -> Outcome<Vec<MemberDisk>> {
        match id {
            Some(id) => Ok(vec![self.get(&id)?]),
            None => Ok(self.disks.lock().unwrap().values().cloned().collect()),
        }
    }
}
