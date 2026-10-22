use super::{Domains, disk_result};
use crate::{
    domains::{
        disk::service::DiskCommand,
        node::service::NodeCommand,
        tier::{TierCommand, TierReply},
        vd::service::{VdCommand, VdReply},
    },
    manager::{PoolShared, Registry},
    support::{object, reject},
    task_center::{OperationKind, PoolOperation, PoolWorkflow},
    *,
};
use async_trait::async_trait;
use std::sync::{Arc, Mutex, Weak};
use workflow_foundation::{ObjectKey, Workflow, WorkflowContext};

pub(crate) struct AttachNodeWorkflow {
    pub domains: Domains,
    pub id: NodeId,
    pub sequence: u64,
    pub connected: bool,
    pub reservation: crate::manager::NodeReservation,
}
impl PoolWorkflow for AttachNodeWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::AttachNode(self.id.clone()),
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for AttachNodeWorkflow {
    type Output = PoolNode;
    const NAME: &'static str = "storage.attach-node";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("node", self.id.as_str())]
    }
    async fn run(mut self, ctx: &mut WorkflowContext) -> Outcome<PoolNode> {
        self.reservation.begin();
        let result = ctx
            .activity(
                &self.domains.node,
                NodeCommand::Attach {
                    id: self.id,
                    sequence: self.sequence,
                    connected: self.connected,
                },
            )
            .await;
        self.reservation.settled(&result);
        result
    }
}

pub(crate) struct DetachNodeWorkflow {
    pub domains: Domains,
    pub id: NodeId,
    pub shared: Arc<PoolShared>,
    pub removal: crate::manager::RemovalGuard,
}
impl PoolWorkflow for DetachNodeWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::DetachNode(self.id.clone()),
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for DetachNodeWorkflow {
    type Output = ();
    const NAME: &'static str = "storage.detach-node";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("node", self.id.as_str())]
    }
    async fn run(mut self, ctx: &mut WorkflowContext) -> Outcome<()> {
        self.removal.begin();
        let result = ctx
            .activity(&self.domains.node, NodeCommand::Detach(self.id.clone()))
            .await;
        self.removal.settled(&result);
        result?;
        self.shared.node_claims.lock().unwrap().remove(&self.id);
        self.shared.nodes.lock().unwrap().remove(&self.id);
        Ok(())
    }
}

pub(crate) struct CreateTierWorkflow {
    pub domains: Domains,
    pub id: TierId,
}
impl PoolWorkflow for CreateTierWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::CreateTier(self.id.clone()),
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for CreateTierWorkflow {
    type Output = Tier;
    const NAME: &'static str = "storage.create-tier";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("tier", self.id.as_str())]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Tier> {
        match ctx
            .activity(&self.domains.tier, TierCommand::Create(self.id))
            .await?
        {
            TierReply::Tier(tier) => Ok(tier),
            _ => Err(Failure::Uncertain("unexpected tier reply".into())),
        }
    }
}
pub(crate) struct DeleteTierWorkflow {
    pub domains: Domains,
    pub id: TierId,
    pub removal: crate::manager::RemovalGuard,
}
impl PoolWorkflow for DeleteTierWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::DeleteTier(self.id.clone()),
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for DeleteTierWorkflow {
    type Output = ();
    const NAME: &'static str = "storage.delete-tier";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("tier", self.id.as_str())]
    }
    async fn run(mut self, ctx: &mut WorkflowContext) -> Outcome<()> {
        self.removal.begin();
        let result = async {
            ctx.activity(
                &self.domains.disk,
                DiskCommand::RequireTierEmpty(self.id.clone()),
            )
            .await?;
            ctx.activity(&self.domains.tier, TierCommand::Delete(self.id.clone()))
                .await?;
            Ok(())
        }
        .await;
        self.removal.settled(&result);
        result
    }
}

pub(crate) struct RemoveDiskWorkflow {
    pub domains: Domains,
    pub spec: DiskSpec,
    pub registry: Weak<Mutex<Registry>>,
    pub uuid: PoolUuid,
    pub routing: Arc<Mutex<std::collections::BTreeMap<DiskId, DiskSpec>>>,
    pub removal: crate::manager::RemovalGuard,
}
impl PoolWorkflow for RemoveDiskWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::RemoveDisk(self.spec.id.clone()),
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for RemoveDiskWorkflow {
    type Output = ();
    const NAME: &'static str = "storage.remove-disk";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![
            object("disk", self.spec.id.as_str()),
            object("node", self.spec.node.as_str()),
            object("tier", self.spec.tier.as_str()),
        ]
    }
    async fn run(mut self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let id = self.spec.id;
        self.removal.begin();
        let result = async {
            ctx.activity(
                &self.domains.vd,
                VdCommand::RequireDiskUnreferenced(id.clone()),
            )
            .await?;
            let disk = disk_result(
                ctx.activity(&self.domains.disk, DiskCommand::Inspect(id.clone()))
                    .await?,
            )?;
            if !disk.allocated.is_empty() {
                return Err(reject("disk still has allocations"));
            }
            ctx.activity(
                &self.domains.disk,
                DiskCommand::SetIo(id.clone(), IoState::Down),
            )
            .await?;
            ctx.activity(&self.domains.disk, DiskCommand::RemoveEmpty(id.clone()))
                .await?;
            if let Some(registry) = self.registry.upgrade() {
                let mut registry = registry.lock().unwrap();
                if registry
                    .disks
                    .get(&id)
                    .is_some_and(|entry| entry.owner == self.uuid)
                {
                    registry.disks.remove(&id);
                }
            }
            self.routing.lock().unwrap().remove(&id);
            Ok(())
        }
        .await;
        self.removal.settled(&result);
        result
    }
}

pub(crate) struct DeleteVolumeWorkflow {
    pub domains: Domains,
    pub volume: VirtualDisk,
    pub nodes: Vec<NodeId>,
}
impl PoolWorkflow for DeleteVolumeWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::DeleteVolume(self.volume.id.clone()),
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for DeleteVolumeWorkflow {
    type Output = ();
    const NAME: &'static str = "storage.delete-volume";
    fn objects(&self) -> Vec<ObjectKey> {
        let mut keys = vec![object("volume", self.volume.id.as_str())];
        keys.extend(
            self.volume
                .groups
                .iter()
                .flat_map(|g| &g.entries)
                .map(|e| object("disk", e.disk.as_str())),
        );
        keys.extend(self.nodes.iter().map(|node| object("node", node.as_str())));
        keys
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let volume = match ctx
            .activity(&self.domains.vd, VdCommand::Delete(self.volume.id))
            .await?
        {
            VdReply::Deleted(volume) => volume,
            _ => {
                return Err(Failure::Uncertain(
                    "unexpected volume deletion reply".into(),
                ));
            }
        };
        for entry in volume.groups.into_iter().flat_map(|g| g.entries) {
            ctx.activity(
                &self.domains.disk,
                DiskCommand::Release {
                    disk: entry.disk,
                    volume: volume.id.clone(),
                    blk: entry.blk,
                },
            )
            .await?;
        }
        Ok(())
    }
}
