use crate::domains::tier::{TierService, runtime::TierHost};
use crate::domains::{
    disk::{runtime::DiskHost, service::DiskService},
    node::{runtime::NodeHost, service::NodeService},
    vd::{runtime::VdHost, service::VdService},
};
use crate::{ports::*, support::reject, task_center::TaskCenter, workflows::*, *};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex, Weak},
};
use workflow_foundation::{RuntimeSnapshot, TaskId, WorkflowDriver};

/// A composition/lifecycle boundary, not an aggregate containing all domain data.
pub struct Pool {
    handle: PoolHandle,
}
impl Pool {
    pub(crate) fn start(
        descriptor: PoolDescriptor,
        config: Config,
        metadata: Arc<dyn MetadataStore>,
        data_plane: Arc<dyn DataPlane>,
        registry: Weak<Mutex<crate::manager::Registry>>,
        task_ids: TaskIds,
        shrink: Option<(
            Arc<dyn crate::domains::shrink::ShrinkStorage>,
            Arc<dyn crate::domains::balance::BalanceBackend>,
        )>,
    ) -> Outcome<Self> {
        let id = descriptor.id.clone();
        let mut driver = WorkflowDriver::new(id.as_str(), config.clone())?;
        let uuid = descriptor
            .uuid
            .to_string()
            .parse::<uuid::Uuid>()
            .map_err(|e| reject(e.to_string()))?;
        driver = driver.with_task_ids(uuid.as_u128(), task_ids)?;
        let tier = driver.register(
            "tier",
            TierHost(TierService::new(id.clone(), metadata.clone())),
        )?;
        let disk = driver.register(
            "disk",
            DiskHost(DiskService::new(
                id.clone(),
                metadata.clone(),
                data_plane.clone(),
            )),
        )?;
        let node = driver.register(
            "node",
            NodeHost(NodeService::new(id.clone(), data_plane.clone())),
        )?;
        let vd = driver.register(
            "vd",
            VdHost(VdService::new(id.clone(), metadata, data_plane)),
        )?;
        let domains = Domains {
            disk,
            node,
            vd,
            tier,
        };
        let shared = Arc::new(crate::manager::PoolShared::new(
            descriptor, registry, &config,
        ));
        let admin = driver.register(
            "pool",
            crate::manager::admin::AdminHost {
                shared: shared.clone(),
                domains: domains.clone(),
            },
        )?;
        let shrink = shrink
            .map(|(storage, balance)| {
                ShrinkServices::register(&mut driver, id.clone(), storage, balance)
            })
            .transpose()?;
        let handle = PoolHandle {
            id,
            task_center: TaskCenter::start(driver, &config),
            domains,
            shared,
            admin,
            shrink,
        };
        Ok(Self { handle })
    }
    pub fn handle(&self) -> PoolHandle {
        self.handle.clone()
    }
    pub async fn shutdown(&mut self) -> Outcome<()> {
        self.handle.shutdown().await
    }
}

/// Public application facade: no raw service mutation or arbitrary submit escape.
#[derive(Clone)]
pub struct PoolHandle {
    pub(crate) id: PoolId,
    pub(crate) task_center: TaskCenter,
    pub(crate) domains: Domains,
    pub(crate) shared: Arc<crate::manager::PoolShared>,
    pub(crate) admin: crate::manager::admin::AdminClient,
    shrink: Option<ShrinkServices>,
}
impl PoolHandle {
    pub async fn shrink(&self, request: ShrinkRequest) -> Outcome<Run<()>> {
        self.descriptor().active()?;
        let services = self
            .shrink
            .clone()
            .ok_or_else(|| reject("shrink backends are not configured"))?;
        self.task_center
            .submit(ShrinkWorkflow::new(request, services)?)
            .await
    }
    /// A caller can query by pool without saving a TaskId. Includes uncertain tasks
    /// which still fence the pool's shrink slot and require reconciliation.
    pub fn current_shrink(&self) -> Option<TaskSnapshot> {
        self.tasks().into_iter().find(|task| {
            task.name == "storage.shrink"
                && match &task.state {
                    workflow_foundation::RunState::Completed => false,
                    workflow_foundation::RunState::Failed(error) => {
                        matches!(error, Failure::Uncertain(_))
                    }
                    _ => true,
                }
        })
    }
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
    pub fn scheduling(&self) -> crate::task_center::SchedulingSnapshot {
        self.task_center.scheduling()
    }
    pub fn execution(&self) -> RuntimeSnapshot {
        self.task_center.snapshot()
    }
    pub async fn disks(&self) -> Outcome<Vec<MemberDisk>> {
        self.domains.disk.query(None).await
    }
    pub async fn nodes(&self) -> Outcome<Vec<PoolNode>> {
        self.domains.node.query(None).await
    }
    pub async fn volumes(&self) -> Outcome<Vec<VirtualDisk>> {
        self.domains.vd.query(None).await
    }
    pub async fn observe_node(
        &self,
        id: NodeId,
        sequence: u64,
        connected: bool,
    ) -> Outcome<Run<PoolNode>> {
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
    pub async fn load_node(&self, id: NodeId) -> Outcome<Run<PoolNode>> {
        self.task_center
            .submit(LoadNodeWorkflow {
                domains: self.domains.clone(),
                id,
            })
            .await
    }
    pub async fn online_disk(&self, id: DiskId) -> Outcome<Run<MemberDisk>> {
        let disks = self.domains.disk.query(Some(id)).await?;
        let disk = &disks[0];
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
    pub async fn offline_disk(&self, id: DiskId) -> Outcome<Run<MemberDisk>> {
        let disk = self.domains.disk.query(Some(id.clone())).await?.remove(0);
        self.task_center
            .submit(OfflineDiskWorkflow {
                domains: self.domains.clone(),
                id,
                node: disk.node,
            })
            .await
    }
    pub async fn offline_node(&self, request: NodeTransitionRequest) -> Outcome<Run<PoolNode>> {
        self.task_center
            .submit(OfflineNodeWorkflow {
                specs: self.task_center.node_disks(&request.node),
                domains: self.domains.clone(),
                request,
            })
            .await
    }
    pub async fn online_node(&self, request: NodeTransitionRequest) -> Outcome<Run<PoolNode>> {
        let specs = self.task_center.node_disks(&request.node);
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
    pub async fn create_volume(&self, request: CreateVolume) -> Outcome<Run<VirtualDisk>> {
        request.validate()?;
        // Placement identities are immutable in this slice. Future relocation
        // must add a versioned placement protocol, not trust this pre-read.
        let mut nodes = BTreeSet::new();
        let mut tiers = BTreeSet::new();
        for disk in &request.disks {
            let view = self.domains.disk.query(Some(disk.clone())).await?;
            nodes.insert(view[0].node.clone());
            tiers.insert(view[0].tier.clone());
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
