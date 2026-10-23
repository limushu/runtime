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
use workflow_foundation::{RuntimeSnapshot, WorkflowDriver, WorkflowId};

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
        balancer: Arc<dyn Balancer>,
        registry: Weak<Mutex<crate::manager::Registry>>,
        task_store: Option<(
            Arc<dyn crate::task_center::persistence::TaskStore>,
            crate::task_center::persistence::PersistenceConfig,
        )>,
    ) -> Outcome<Self> {
        let id = descriptor.id.clone();
        let authoritative_reads = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut driver = WorkflowDriver::new(id.as_str(), config.clone())?;
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
                authoritative_reads.clone(),
            )),
        )?;
        let node = driver.register(
            "node",
            NodeHost(NodeService::new(id.clone(), data_plane.clone())),
        )?;
        let vd = driver.register(
            "vd",
            VdHost(VdService::new(
                id.clone(),
                metadata.clone(),
                data_plane,
                authoritative_reads.clone(),
            )),
        )?;
        let balance = Arc::new(crate::domains::balance::service::BalanceService {
            pool: id.clone(),
            port: balancer,
        });
        let balance_client = driver.register(
            "balance",
            crate::domains::balance::runtime::BalanceHost(balance.clone()),
        )?;
        let domains = Domains {
            balance: balance_client,
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
        let task_center = match task_store {
            Some((store, persistence_config)) => TaskCenter::with_task_store(
                driver,
                &config,
                shared.descriptor.lock().unwrap().uuid,
                shared.instance,
                store,
                persistence_config,
            )?,
            None => TaskCenter::start(driver, &config),
        };
        let handle = PoolHandle {
            authoritative_reads,
            metadata,
            balance,
            id,
            task_center,
            domains,
            shared,
            admin,
        };
        if handle.task_center.has_task_store() {
            let domains = handle.domains.clone();
            let metadata = handle.metadata.clone();
            let balance = handle.balance.clone();
            let shared = handle.shared.clone();
            let routing = handle.task_center.disk_directory();
            handle.register_task_recovery("storage.shrink-disk", 1, move |task| {
                let domains = domains.clone();
                let metadata = metadata.clone();
                let balance = balance.clone();
                let shared = shared.clone();
                let routing = routing.clone();
                async move {
                    let resume = task
                        .resume
                        .ok_or_else(|| reject("missing shrink resume input"))?;
                    let request: ShrinkRequest =
                        serde_json::from_slice(&resume.input).map_err(|e| reject(e.to_string()))?;
                    Ok(ShrinkDiskWorkflow::new(
                        domains, request, metadata, balance, shared, routing, true,
                    ))
                }
            })?;
        }
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
    pub(crate) authoritative_reads: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) metadata: Arc<dyn MetadataStore>,
    pub(crate) balance: Arc<crate::domains::balance::service::BalanceService>,
    pub(crate) id: PoolId,
    pub(crate) task_center: TaskCenter,
    pub(crate) domains: Domains,
    pub(crate) shared: Arc<crate::manager::PoolShared>,
    pub(crate) admin: crate::manager::admin::AdminClient,
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
    pub fn recheck_task(&self, id: WorkflowId) -> Outcome<()> {
        self.task_center.recheck(id)
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
        if self
            .authoritative_reads
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(self
                .metadata
                .snapshot(&self.id)
                .await?
                .disks
                .into_iter()
                .filter(|d| d.membership == DiskMembership::Member)
                .collect());
        }
        self.domains.disk.query(None).await
    }
    pub async fn nodes(&self) -> Outcome<Vec<PoolNode>> {
        self.domains.node.query(None).await
    }
    pub async fn volumes(&self) -> Outcome<Vec<VirtualDisk>> {
        if self
            .authoritative_reads
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Ok(self.metadata.snapshot(&self.id).await?.volumes);
        }
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
