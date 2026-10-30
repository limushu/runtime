use crate::{support::reject, task_center::TaskCenter, workflows::*, *};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use workflow_foundation::{RuntimeSnapshot, TaskId, WorkflowDriver};

/// A composition/lifecycle boundary, not an aggregate containing all domain data.
pub struct Pool {
    handle: PoolHandle,
}
impl Pool {
    pub(crate) fn start(descriptor: PoolDescriptor, manager: &PoolManager) -> Outcome<Self> {
        Self::prepare(descriptor, manager, None)?.finish(None)
    }
    pub(crate) fn prepare(
        descriptor: PoolDescriptor,
        manager: &PoolManager,
        snapshot: Option<crate::domain_clients::DomainSnapshot>,
    ) -> Outcome<PreparedPool> {
        let id = descriptor.id.clone();
        let mut driver = WorkflowDriver::new(id.as_str(), manager.config.clone())?;
        let uuid = descriptor
            .uuid
            .to_string()
            .parse::<uuid::Uuid>()
            .map_err(|e| reject(e.to_string()))?;
        driver = driver.with_task_ids(uuid.as_u128(), manager.task_ids.clone())?;
        let shared = Arc::new(crate::manager::PoolShared::new(
            descriptor,
            Arc::downgrade(&manager.registry),
            &manager.config,
        ));
        let routing = Arc::new(Mutex::new(std::collections::BTreeMap::new()));
        if let Some(snapshot) = &snapshot {
            for node in &snapshot.nodes {
                shared.nodes.lock().unwrap().insert(node.id.clone());
                shared.node_claims.lock().unwrap().insert(
                    node.id.clone(),
                    crate::manager::NodeClaim {
                        confirmed: true,
                        ..Default::default()
                    },
                );
                shared.serial.fetch_max(
                    node.observation.saturating_add(1),
                    std::sync::atomic::Ordering::SeqCst,
                );
            }
            for disk in &snapshot.disks.disks {
                routing.lock().unwrap().insert(
                    disk.id.clone(),
                    DiskSpec {
                        id: disk.id.clone(),
                        node: disk.node.clone(),
                        tier: disk.tier.clone(),
                        capacity_blks: disk.capacity_blks,
                    },
                );
            }
        }
        let domains = Domains::register_with_snapshot(
            &mut driver,
            id,
            manager.metadata.clone(),
            manager.data_plane.clone(),
            manager.balance.clone(),
            snapshot,
        )?;
        let admin = driver.register(
            "pool",
            crate::manager::admin::AdminHost {
                shared: shared.clone(),
                domains: domains.clone(),
            },
        )?;
        Ok(PreparedPool {
            driver,
            domains,
            shared,
            admin,
            routing,
            config: manager.config.clone(),
            durable: manager.task_store.is_some(),
        })
    }
    pub fn handle(&self) -> PoolHandle {
        self.handle.clone()
    }
    pub async fn shutdown(&mut self) -> Outcome<()> {
        self.handle.shutdown().await
    }
}

pub(crate) struct PreparedPool {
    pub driver: WorkflowDriver,
    pub domains: Domains,
    pub shared: Arc<crate::manager::PoolShared>,
    pub routing: Arc<Mutex<std::collections::BTreeMap<DiskId, DiskSpec>>>,
    admin: crate::manager::admin::AdminClient,
    config: Config,
    durable: bool,
}
impl PreparedPool {
    pub fn finish(self, restored: Option<Vec<workflow_foundation::Restored>>) -> Outcome<Pool> {
        let task_center = match restored {
            Some(tasks) => TaskCenter::from_restored_with_routing(
                self.driver,
                &self.config,
                Arc::new(crate::task_center::PoolSchedulingPolicy),
                tasks,
                self.routing,
            )?,
            None => TaskCenter::start(self.driver, &self.config),
        };
        let id = self.shared.descriptor.lock().unwrap().id.clone();
        Ok(Pool {
            handle: PoolHandle {
                id,
                task_center,
                domains: self.domains,
                shared: self.shared,
                admin: self.admin,
                durable: self.durable,
            },
        })
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
    durable: bool,
}
impl PoolHandle {
    pub async fn shrink(&self, request: ShrinkRequest) -> Outcome<Run<()>> {
        self.descriptor().active()?;
        let request = ShrinkRequest::new(request.disks)?;
        let membership = self.reserve_removal(&request.disks)?;
        let mut workflow =
            ShrinkWorkflow::new(request, self.domains.clone())?.with_membership(membership);
        if self.durable {
            workflow = workflow.durable();
        }
        self.task_center.submit(workflow).await
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
    /// Persistent history across leader changes; querying never restarts a Task.
    pub async fn history(
        &self,
        query: workflow_foundation::persistence::TaskQuery,
    ) -> Outcome<workflow_foundation::persistence::TaskPage> {
        self.task_center.history(query).await
    }
    pub async fn history_task(
        &self,
        id: TaskId,
    ) -> Outcome<workflow_foundation::persistence::SavedTask> {
        self.task_center.history_task(id).await
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
        self.domains.disks().await
    }
    pub async fn nodes(&self) -> Outcome<Vec<PoolNode>> {
        self.domains.node.query(None).await
    }
    pub async fn volumes(&self) -> Outcome<Vec<VirtualDisk>> {
        self.domains.volumes().await
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
        let disk = self.domains.disk(&id).await?;
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
        let disk = self.domains.disk(&id).await?;
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
        // This pre-read builds the conflict set. Domain activities re-read and
        // validate authoritative placement under their revision checks.
        let mut nodes = BTreeSet::new();
        let mut tiers = BTreeSet::new();
        for disk in &request.disks {
            let view = self.domains.disk(disk).await?;
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
