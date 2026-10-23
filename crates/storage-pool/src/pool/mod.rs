mod admin;
mod bootstrap;
mod lifecycle;
mod membership;
mod operations;
mod ownership;
mod recovery;
mod shrink_disk;
mod state;
mod takeover;
use crate::domains::tier::{TierService, runtime::TierHost};
use crate::domains::{
    disk::{runtime::DiskHost, service::DiskService},
    node::{runtime::NodeHost, service::NodeService},
    vd::{runtime::VdHost, service::VdService},
};
use crate::manager::*;
use crate::{ports::*, support::reject, task_center::TaskCenter, workflows::*, *};
pub(crate) use admin::{AdminClient, AdminCommand};
pub(crate) use membership::{MembershipClient, MembershipCommand, MembershipReply};
pub(crate) use ownership::{DiskOwnership, DiskReservation, NodeReservation, RemovalGuard};
pub(crate) use state::PoolShared;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, Weak},
};
use workflow_foundation::{RuntimeSnapshot, WorkflowDriver};

/// A composition/lifecycle boundary, not an aggregate containing all domain data.
pub struct Pool {
    handle: PoolHandle,
}
/// Public application facade: no raw service mutation or arbitrary submit escape.
#[derive(Clone)]
pub struct PoolHandle {
    pub(crate) metadata: Arc<dyn MetadataStore>,
    pub(crate) balancer: Arc<dyn Balancer>,
    pub(crate) id: PoolId,
    pub(crate) task_center: TaskCenter,
    pub(crate) domains: Domains,
    pub(crate) shared: Arc<PoolShared>,
    pub(crate) admin: admin::AdminClient,
}
impl PoolHandle {
    pub fn id(&self) -> &PoolId {
        &self.id
    }
    pub fn tasks(&self) -> Vec<TaskSnapshot> {
        self.task_center.tasks()
    }
    pub fn task(&self, id: TaskId) -> Outcome<TaskSnapshot> {
        self.task_center.task(id)
    }
    pub fn observe_task(&self, id: TaskId) -> Outcome<TaskObserver> {
        self.task_center.observe_task(id)
    }
    pub fn suspend(&self, id: TaskId) -> Outcome<()> {
        self.task_center.suspend(id)
    }
    pub fn resume(&self, id: TaskId) -> Outcome<()> {
        self.task_center.resume(id)
    }
    pub fn recheck_task(&self, id: TaskId) -> Outcome<()> {
        self.task_center.recheck(id)
    }
    pub fn cancel(&self, id: TaskId) -> Outcome<()> {
        self.task_center.cancel(id)
    }
    pub async fn wait_suspended(&self, id: TaskId) -> Outcome<()> {
        self.task_center.wait_suspended(id).await
    }
    pub async fn suspend_task(&self, id: TaskId) -> Outcome<()> {
        self.task_center.suspend_task(id).await
    }
    pub async fn resume_task(&self, id: TaskId) -> Outcome<()> {
        self.descriptor().active()?;
        self.task_center.resume_task(id).await
    }
    pub async fn cancel_task(&self, id: TaskId) -> Outcome<()> {
        self.task_center.cancel_task(id).await
    }
    pub fn scheduling(&self) -> crate::task_center::SchedulingSnapshot {
        self.task_center.scheduling()
    }
    pub fn execution(&self) -> RuntimeSnapshot {
        self.task_center.snapshot()
    }
    pub async fn disks(&self) -> Outcome<Vec<MemberDisk>> {
        Ok(self
            .metadata
            .snapshot(&self.id)
            .await?
            .disks
            .into_iter()
            .filter(|d| d.membership == DiskMembership::Member)
            .collect())
    }
    pub async fn nodes(&self) -> Outcome<Vec<PoolNode>> {
        self.domains.node.query(None).await
    }
    pub async fn volumes(&self) -> Outcome<Vec<VirtualDisk>> {
        Ok(self.metadata.snapshot(&self.id).await?.volumes)
    }
    pub async fn observe_node(
        &self,
        id: NodeId,
        sequence: u64,
        connected: bool,
    ) -> Outcome<TaskRun<PoolNode>> {
        self.descriptor().active()?;
        if !self.shared.nodes.lock().unwrap().contains(&id) {
            return Err(reject("node is not attached to this pool"));
        }
        self.shared.serial.fetch_max(
            sequence.saturating_add(1),
            std::sync::atomic::Ordering::SeqCst,
        );
        self.task_center
            .submit(ObserveNodeWorkflow {
                domains: self.domains.clone(),
                id,
                sequence,
                connected,
            })
            .await
    }
    pub async fn load_node(&self, id: NodeId) -> Outcome<TaskRun<PoolNode>> {
        self.task_center
            .submit(LoadNodeWorkflow {
                domains: self.domains.clone(),
                id,
            })
            .await
    }
    pub async fn online_disk(&self, id: DiskId) -> Outcome<TaskRun<MemberDisk>> {
        let disk = self.disk(id).await?;
        if disk.membership == DiskMembership::Removed {
            return Err(reject("disk is removed"));
        }
        self.task_center
            .submit(OnlineDiskWorkflow {
                domains: self.domains.clone(),
                reservation: None,
                spec: DiskSpec {
                    id: disk.id.clone(),
                    node: disk.node.clone(),
                    tier: disk.tier.clone(),
                    capacity_blks: disk.capacity_blks,
                },
            })
            .await
    }
    pub async fn offline_disk(&self, id: DiskId) -> Outcome<TaskRun<MemberDisk>> {
        let disk = self.disk(id.clone()).await?;
        self.task_center
            .submit(OfflineDiskWorkflow {
                domains: self.domains.clone(),
                id,
                node: disk.node,
            })
            .await
    }
    pub async fn offline_node(&self, request: NodeTransitionRequest) -> Outcome<TaskRun<PoolNode>> {
        self.task_center
            .submit(OfflineNodeWorkflow {
                specs: self.shared.node_disks(&request.node),
                domains: self.domains.clone(),
                request,
            })
            .await
    }
    pub async fn online_node(&self, request: NodeTransitionRequest) -> Outcome<TaskRun<PoolNode>> {
        let specs = self.shared.node_disks(&request.node);
        let reservations = specs
            .iter()
            .map(|spec| self.reserve_disk(spec))
            .collect::<Outcome<Vec<_>>>()?;
        self.task_center
            .submit(OnlineNodeWorkflow {
                specs,
                reservations,
                domains: self.domains.clone(),
                request,
            })
            .await
    }
    pub async fn create_volume(&self, request: CreateVolume) -> Outcome<TaskRun<VirtualDisk>> {
        request.validate()?;
        // Placement identities are immutable in this slice. Future relocation
        // must add a versioned placement protocol, not trust this pre-read.
        let mut nodes = BTreeSet::new();
        let mut tiers = BTreeSet::new();
        for disk in &request.disks {
            let view = self.disk(disk.clone()).await?;
            nodes.insert(view.node);
            tiers.insert(view.tier);
        }
        if nodes.len() != request.disks.len() || tiers.len() != 1 {
            return Err(reject("replicas must be in one tier on distinct nodes"));
        }
        self.task_center
            .submit(CreateVolumeWorkflow {
                domains: self.domains.clone(),
                request,
                nodes: nodes.into_iter().collect(),
            })
            .await
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.handle.task_center.abort();
    }
}
