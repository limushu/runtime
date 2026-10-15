use crate::domain::{
    PoolId,
    disk::{Allocation, DiskId},
    object,
    ports::*,
    reject,
    vd::{BgEntry, BlockGroup, DataState, VirtualDisk, VolumeId},
};
use async_trait::async_trait;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use workflow_foundation::Outcome;
use workflow_foundation::{ActivityContext, DomainService, ResourceAccess};

pub(crate) enum VdCommand {
    RequireAbsent(VolumeId),
    Create {
        id: VolumeId,
        allocations: Vec<Allocation>,
    },
    Invalidate(DiskId),
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
}
#[async_trait]
impl DomainService for VdService {
    type Request = VdCommand;
    type Reply = VdReply;
    type Query = Option<VolumeId>;
    type View = Vec<VirtualDisk>;
    fn resources(request: &VdCommand) -> Vec<ResourceAccess> {
        if matches!(request, VdCommand::RequireAbsent(_)) {
            return vec![ResourceAccess::read(object("vd", "reference-index"))];
        }
        // Protect the reverse-reference index including newly created VDs. A
        // fixed known set is safer than discovering/expanding locks during IO.
        vec![ResourceAccess::write(object("vd", "reference-index"))]
    }
    async fn execute(&self, request: VdCommand, ctx: ActivityContext) -> Outcome<VdReply> {
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
                let nodes: BTreeSet<_> = allocations.iter().map(|a| &a.node).collect();
                let tiers: BTreeSet<_> = allocations.iter().map(|a| &a.tier).collect();
                let disks: BTreeSet<_> = allocations.iter().map(|a| &a.disk).collect();
                if tiers.len() != 1
                    || !(2..=3).contains(&allocations.len())
                    || nodes.len() != allocations.len()
                    || disks.len() != allocations.len()
                    || allocations
                        .iter()
                        .any(|a| a.pool != self.pool || a.volume != id)
                {
                    return Err(reject("invalid allocation evidence or replica placement"));
                }
                let entries: Vec<_> = allocations
                    .into_iter()
                    .map(|a| BgEntry {
                        disk: a.disk,
                        blk: a.blk,
                        data: DataState::Valid,
                    })
                    .collect();
                self.data_plane
                    .initialize_bg(&self.pool, &id, &entries, ctx.cancellation())
                    .await?;
                let volume = VirtualDisk {
                    id: id.clone(),
                    pool: self.pool.clone(),
                    replicas: entries.len(),
                    groups: vec![BlockGroup { index: 0, entries }],
                    revision: 0,
                };
                // Failed publication leaks allocations safely. There is no free
                // capability until reference removal/reconciliation is designed.
                self.metadata
                    .commit(&self.pool, MetadataChange::CreateVolume(volume.clone()))
                    .await?;
                self.volumes.lock().unwrap().insert(id, volume.clone());
                Ok(VdReply::Created(volume))
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
    fn query(&self, id: Option<VolumeId>) -> Outcome<Vec<VirtualDisk>> {
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
