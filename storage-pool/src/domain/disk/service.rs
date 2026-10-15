use crate::domain::{
    PoolId,
    disk::{Allocation, AllocationState, DiskId, DiskSpec, IoState, MemberDisk},
    object,
    ports::*,
    reject,
    vd::VolumeId,
};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use workflow_foundation::{ActivityContext, DomainService, ResourceAccess};
use workflow_foundation::{Failure, Outcome};

pub(crate) enum DiskCommand {
    Inspect(DiskId),
    Add(DiskSpec),
    Open(DiskId),
    SetIo(DiskId, IoState),
    Allocate(DiskId, VolumeId),
}
pub(crate) enum DiskReply {
    Disk(MemberDisk),
    Allocated(Allocation),
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
}
#[async_trait]
impl DomainService for DiskService {
    type Request = DiskCommand;
    type Reply = DiskReply;
    type Query = Option<DiskId>;
    type View = Vec<MemberDisk>;
    fn resources(request: &DiskCommand) -> Vec<ResourceAccess> {
        let id = match request {
            DiskCommand::Inspect(id) => {
                return vec![ResourceAccess::read(object("disk", id.as_str()))];
            }
            DiskCommand::Add(s) => &s.id,
            DiskCommand::Open(id) | DiskCommand::SetIo(id, _) | DiskCommand::Allocate(id, _) => id,
        };
        vec![ResourceAccess::write(object("disk", id.as_str()))]
    }
    async fn execute(&self, request: DiskCommand, ctx: ActivityContext) -> Outcome<DiskReply> {
        // Only short in-memory access uses Mutex. No guard crosses an await;
        // all object scheduling is the generic Coordinator's responsibility.
        match request {
            DiskCommand::Inspect(id) => Ok(DiskReply::Disk(self.get(&id)?)),
            DiskCommand::Add(spec) => {
                if spec.capacity_blks == 0 {
                    return Err(reject("disk has zero capacity"));
                }
                if let Some(existing) = self.disks.lock().unwrap().get(&spec.id).cloned() {
                    if existing.node != spec.node
                        || existing.tier != spec.tier
                        || existing.capacity_blks != spec.capacity_blks
                    {
                        return Err(reject(
                            "existing disk identity has different topology or capacity",
                        ));
                    }
                    return Ok(DiskReply::Disk(existing));
                }
                let disk = MemberDisk {
                    id: spec.id,
                    pool: self.pool.clone(),
                    node: spec.node,
                    tier: spec.tier,
                    capacity_blks: spec.capacity_blks,
                    io: IoState::Down,
                    allocation: AllocationState::Active,
                    allocated: Default::default(),
                    revision: 0,
                };
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
                self.data_plane
                    .open_disk(&self.pool, &id, ctx.cancellation())
                    .await?;
                Ok(DiskReply::Disk(disk))
            }
            DiskCommand::SetIo(id, io) => {
                let mut disk = self.get(&id)?;
                // Reassert the external effect even for repeated metadata state.
                self.data_plane
                    .set_disk_io(&self.pool, &id, io, ctx.cancellation())
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
            DiskCommand::Allocate(id, volume) => {
                let mut disk = self.get(&id)?;
                if !disk.allocatable() {
                    return Err(reject("disk cannot allocate"));
                }
                let blk = (0..disk.capacity_blks)
                    .find(|b| !disk.allocated.contains(b))
                    .ok_or_else(|| reject("disk is full"))?;
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
    fn query(&self, id: Option<DiskId>) -> Outcome<Vec<MemberDisk>> {
        match id {
            Some(id) => Ok(vec![self.get(&id)?]),
            None => Ok(self.disks.lock().unwrap().values().cloned().collect()),
        }
    }
}
