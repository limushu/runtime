use crate::domains::{
    disk::{runtime::DiskHost, service::DiskService},
    node::{runtime::NodeHost, service::NodeService},
    vd::{runtime::VdHost, service::VdService},
};
use crate::{ports::*, support::reject, task_center::TaskCenter, workflows::*, *};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use workflow_foundation::{RuntimeSnapshot, WorkflowDriver, WorkflowId};

/// A composition/lifecycle boundary, not an aggregate containing all domain data.
pub struct Pool {
    handle: PoolHandle,
}
impl Pool {
    fn start(
        id: PoolId,
        config: Config,
        metadata: Arc<dyn MetadataStore>,
        data_plane: Arc<dyn DataPlane>,
    ) -> Outcome<Self> {
        let mut driver = WorkflowDriver::new(id.as_str(), config.clone())?;
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
        let handle = PoolHandle {
            id,
            task_center: TaskCenter::start(driver, &config),
            domains: Domains { disk, node, vd },
        };
        Ok(Self { handle })
    }
    pub fn handle(&self) -> PoolHandle {
        self.handle.clone()
    }
    pub async fn shutdown(&mut self) -> Outcome<()> {
        self.handle.task_center.shutdown().await
    }
}

/// Public application facade: no raw service mutation or arbitrary submit escape.
#[derive(Clone)]
pub struct PoolHandle {
    id: PoolId,
    task_center: TaskCenter,
    domains: Domains,
}
impl PoolHandle {
    pub fn id(&self) -> &PoolId {
        &self.id
    }
    pub fn tasks(&self) -> Vec<TaskSnapshot> {
        self.task_center.tasks()
    }
    pub fn task(&self, id: WorkflowId) -> Outcome<TaskSnapshot> {
        self.task_center.task(id)
    }
    pub fn observe_task(&self, id: WorkflowId) -> Outcome<TaskObserver> {
        self.task_center.observe_task(id)
    }
    pub fn suspend(&self, id: WorkflowId) -> Outcome<()> {
        self.task_center.suspend(id)
    }
    pub fn resume(&self, id: WorkflowId) -> Outcome<()> {
        self.task_center.resume(id)
    }
    pub fn cancel(&self, id: WorkflowId) -> Outcome<()> {
        self.task_center.cancel(id)
    }
    pub async fn wait_suspended(&self, id: WorkflowId) -> Outcome<()> {
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
        self.task_center
            .submit(OnlineNodeWorkflow {
                specs: self.task_center.node_disks(&request.node),
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

/// Global routing/lifetime plus ordinary physical-disk ownership. No workflow
/// arbitration here: that belongs to each Pool's concrete TaskCenter.
pub struct PoolManager {
    pools: BTreeMap<PoolId, Pool>,
    disk_owners: BTreeMap<DiskId, PoolId>,
    config: Config,
    metadata: Arc<dyn MetadataStore>,
    data_plane: Arc<dyn DataPlane>,
    stopped: bool,
}
impl PoolManager {
    pub fn new(
        config: Config,
        metadata: Arc<dyn MetadataStore>,
        data_plane: Arc<dyn DataPlane>,
    ) -> Self {
        Self {
            pools: BTreeMap::new(),
            disk_owners: BTreeMap::new(),
            config,
            metadata,
            data_plane,
            stopped: false,
        }
    }
    pub fn create_pool(&mut self, id: PoolId) -> Outcome<PoolHandle> {
        if self.stopped {
            return Err(Failure::Unavailable);
        }
        if self.pools.contains_key(&id) {
            return Err(reject(
                "pool identity already used; recreation is not recovery",
            ));
        }
        let pool = Pool::start(
            id.clone(),
            self.config.clone(),
            self.metadata.clone(),
            self.data_plane.clone(),
        )?;
        let handle = pool.handle();
        self.pools.insert(id, pool);
        Ok(handle)
    }
    pub fn pool(&self, id: &PoolId) -> Outcome<PoolHandle> {
        self.pools
            .get(id)
            .map(Pool::handle)
            .ok_or_else(|| reject("unknown pool"))
    }
    /// Routes a complete online operation, including initial membership if needed.
    pub async fn online_disk(&mut self, pool: &PoolId, spec: DiskSpec) -> Outcome<Run<MemberDisk>> {
        if self.stopped {
            return Err(Failure::Unavailable);
        }
        let handle = self.pool(pool)?;
        if spec.capacity_blks == 0 {
            return Err(reject("disk capacity must be positive"));
        }
        if self
            .disk_owners
            .get(&spec.id)
            .is_some_and(|owner| owner != pool)
        {
            return Err(reject("physical disk belongs to another pool"));
        }
        // Reserve before await. Cancellation or an uncertain commit must never
        // make the same physical disk available to a different Pool.
        handle.task_center.register_disk(&spec)?;
        self.disk_owners.insert(spec.id.clone(), pool.clone());
        handle
            .task_center
            .submit(OnlineDiskWorkflow {
                domains: handle.domains,
                spec,
            })
            .await
    }
    pub async fn shutdown_pool(&mut self, id: &PoolId) -> Outcome<()> {
        self.pools
            .get_mut(id)
            .ok_or_else(|| reject("unknown pool"))?
            .shutdown()
            .await
    }
    pub async fn shutdown(&mut self) -> Outcome<()> {
        self.stopped = true;
        let mut failure = None;
        for pool in self.pools.values_mut() {
            if let Err(e) = pool.shutdown().await {
                failure.get_or_insert(e);
            }
        }
        failure.map_or(Ok(()), Err)
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        self.handle.task_center.abort();
    }
}
