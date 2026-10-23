use super::*;
use crate::support::reject;
use async_trait::async_trait;
use std::collections::BTreeMap;
use workflow_foundation::{ActivityClient, ActivityContext, ResourceAccess, ServiceHandler};

pub(crate) enum MembershipCommand {
    RetireDisk(DiskId),
    DetachNode(NodeId),
    RemoveDisk(DiskId),
    FinalizeShrink(DiskId, crate::domains::balance::EvacuationId),
    Restore,
    ApplyObservation(TopologyEvent),
}
pub(crate) enum MembershipReply {
    Updated,
    Disk(MemberDisk),
}
pub(crate) type MembershipClient = ActivityClient<MembershipCommand, MembershipReply, (), ()>;
pub(super) struct MembershipHost {
    pub shared: Arc<PoolShared>,
    pub metadata: Arc<dyn MetadataStore>,
    pub disks: Arc<DiskService>,
    pub nodes: Arc<NodeService>,
}
#[async_trait]
impl ServiceHandler for MembershipHost {
    type Request = MembershipCommand;
    type Reply = MembershipReply;
    type Query = ();
    type View = ();
    fn activity_name(request: &MembershipCommand) -> &'static str {
        match request {
            MembershipCommand::RetireDisk(_) => "pool.retire-disk",
            MembershipCommand::DetachNode(_) => "pool.detach-node",
            MembershipCommand::RemoveDisk(_) => "pool.remove-disk",
            MembershipCommand::FinalizeShrink(..) => "pool.finalize-shrink",
            MembershipCommand::Restore => "pool.restore-membership",
            MembershipCommand::ApplyObservation(_) => "pool.apply-node-observation",
        }
    }
    fn resources(request: &MembershipCommand) -> Vec<ResourceAccess> {
        let mut keys = vec![ResourceAccess::write(crate::support::object(
            "pool",
            "membership",
        ))];
        match request {
            MembershipCommand::FinalizeShrink(id, _)
            | MembershipCommand::RemoveDisk(id)
            | MembershipCommand::RetireDisk(id) => keys.push(ResourceAccess::write(
                crate::support::object("disk", id.as_str()),
            )),
            MembershipCommand::DetachNode(id) => keys.push(ResourceAccess::write(
                crate::support::object("node", id.as_str()),
            )),
            MembershipCommand::Restore => {}
            MembershipCommand::ApplyObservation(event) => {
                keys.push(ResourceAccess::write(event.key()))
            }
        }
        keys
    }
    async fn execute(
        &self,
        request: MembershipCommand,
        ctx: ActivityContext,
    ) -> Outcome<MembershipReply> {
        match request {
            MembershipCommand::RetireDisk(id) => {
                retire_disk(&self.shared, &self.shared.routing, &id)?
            }
            MembershipCommand::FinalizeShrink(id, job) => {
                let reply = self
                    .disks
                    .execute(
                        crate::domains::disk::service::DiskCommand::FinalizeShrink(id.clone(), job),
                        ctx.control().cancellation(),
                    )
                    .await?;
                let crate::domains::disk::service::DiskReply::Disk(disk) = reply else {
                    return Err(Failure::Uncertain("unexpected shrink reply".into()));
                };
                // Commit and directory release form one non-pausable completion boundary.
                retire_disk(&self.shared, &self.shared.routing, &id)?;
                return Ok(MembershipReply::Disk(disk));
            }
            MembershipCommand::RemoveDisk(id) => {
                self.disks
                    .execute(
                        crate::domains::disk::service::DiskCommand::RemoveEmpty(id.clone()),
                        ctx.control().cancellation(),
                    )
                    .await?;
                retire_disk(&self.shared, &self.shared.routing, &id)?;
            }
            MembershipCommand::DetachNode(id) => {
                self.nodes
                    .execute(
                        crate::domains::node::service::NodeCommand::Detach(id.clone()),
                        ctx.control().cancellation(),
                    )
                    .await?;
                self.shared.node_claims.lock().unwrap().remove(&id);
                self.shared.nodes.lock().unwrap().remove(&id);
            }
            MembershipCommand::Restore => {
                let pool = self.shared.descriptor.lock().unwrap().id.clone();
                let disks = self.metadata.snapshot(&pool).await?.disks;
                restore_disk_directory(&self.shared, &self.shared.routing, disks)?;
            }
            MembershipCommand::ApplyObservation(event) => {
                let TopologyChange::NodeConnectivity { node, connected } = &event.change else {
                    return Err(reject("load requires node connectivity snapshots"));
                };
                let sequence = self
                    .shared
                    .serial
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                    .max(event.sequence);
                self.shared.serial.fetch_max(
                    sequence.saturating_add(1),
                    std::sync::atomic::Ordering::SeqCst,
                );
                self.nodes
                    .execute(
                        crate::domains::node::service::NodeCommand::Observe {
                            id: node.clone(),
                            sequence,
                            connected: *connected,
                        },
                        ctx.control().cancellation(),
                    )
                    .await?;
                let descriptor = self.shared.descriptor.lock().unwrap().clone();
                let receipt = PoolEventReceipt::immediate(
                    descriptor.uuid,
                    self.shared.instance,
                    event.id,
                    EventDecision::Applied,
                );
                self.shared
                    .events
                    .lock()
                    .unwrap()
                    .versions
                    .insert(event.key(), (Arc::new(event), receipt));
            }
        }
        Ok(MembershipReply::Updated)
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
pub(crate) fn retire_disk(
    shared: &PoolShared,
    routing: &Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
    id: &DiskId,
) -> Outcome<()> {
    if let Some(registry) = shared.registry.upgrade() {
        let mut registry = registry.lock().unwrap();
        let uuid = shared.descriptor.lock().unwrap().uuid;
        if registry
            .disks
            .get(id)
            .is_some_and(|owner| owner.owner == uuid)
        {
            registry.disks.remove(id);
        }
    }
    routing.lock().unwrap().remove(id);
    shared.retiring_disks.lock().unwrap().insert(id.clone());
    Ok(())
}
pub(crate) fn restore_disk_directory(
    shared: &PoolShared,
    routing: &Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
    disks: Vec<MemberDisk>,
) -> Outcome<()> {
    let registry = shared.registry.upgrade().ok_or(Failure::Unavailable)?;
    let mut registry = registry.lock().unwrap();
    let uuid = shared.descriptor.lock().unwrap().uuid;
    for disk in &disks {
        if disk.membership == DiskMembership::Member
            && registry
                .disks
                .get(&disk.id)
                .is_some_and(|owner| owner.owner != uuid)
        {
            return Err(reject("restored disk is owned by another pool"));
        }
    }
    let mut directory = routing.lock().unwrap();
    directory.clear();
    for disk in disks {
        if disk.membership == DiskMembership::Removed {
            shared.retiring_disks.lock().unwrap().insert(disk.id);
            continue;
        }
        registry.disks.insert(
            disk.id.clone(),
            super::DiskOwnership {
                owner: uuid,
                claims: 0,
                confirmed: true,
                uncertain: false,
            },
        );
        shared.nodes.lock().unwrap().insert(disk.node.clone());
        shared
            .node_claims
            .lock()
            .unwrap()
            .entry(disk.node.clone())
            .or_default()
            .confirmed = true;
        directory.insert(
            disk.id.clone(),
            DiskSpec {
                id: disk.id,
                node: disk.node,
                tier: disk.tier,
                capacity_blks: disk.capacity_blks,
            },
        );
    }
    Ok(())
}
