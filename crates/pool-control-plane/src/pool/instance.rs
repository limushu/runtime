use super::model::{PoolLifecycle, PoolMetadata, PoolPatch, PoolSnapshot, PoolSpec};
use crate::domains::member_disk::model::MemberDisk;
use crate::domains::member_disk::MemberDiskService;
use crate::domains::pool_node::PoolNodeService;
use crate::domains::virtual_disk::VirtualDiskService;
use crate::kernel::PoolId;
use crate::ports::ControlPlaneStore;
use control_runtime::{
    ControlHandle, ManagedService, RuntimeConfig, RuntimeError, RuntimeResult, ServiceObserver,
    StateCell,
};
use std::sync::{Arc, Mutex};

struct PoolServiceHosts {
    member_disk: ManagedService,
    virtual_disk: ManagedService,
    pool_node: ManagedService,
}

impl PoolServiceHosts {
    async fn shutdown(self) -> RuntimeResult<()> {
        let member_disk = self.member_disk.shutdown().await;
        let virtual_disk = self.virtual_disk.shutdown().await;
        let pool_node = self.pool_node.shutdown().await;
        member_disk.and(virtual_disk).and(pool_node)
    }
}

/// One independently loaded Pool control-plane instance.
///
/// `Pool` is the business and lifecycle boundary. The generic runtime remains
/// an implementation detail of each contained service.
pub struct Pool {
    metadata: StateCell<PoolMetadata>,
    member_disks: MemberDiskService,
    virtual_disks: VirtualDiskService,
    pool_nodes: PoolNodeService,
    member_disk_control: ControlHandle,
    member_disk_observer: ServiceObserver,
    virtual_disk_observer: ServiceObserver,
    hosts: Mutex<Option<PoolServiceHosts>>,
    store: Arc<dyn ControlPlaneStore>,
}

impl Pool {
    pub(crate) async fn create(
        spec: PoolSpec,
        disks: Vec<MemberDisk>,
        store: Arc<dyn ControlPlaneStore>,
        config: RuntimeConfig,
    ) -> RuntimeResult<Arc<Self>> {
        if disks.iter().any(|disk| disk.pool() != &spec.id) {
            return Err(RuntimeError::Rejected(
                "all initial MemberDisks must belong to the created Pool".into(),
            ));
        }
        let mut metadata = PoolMetadata::creating(spec);
        store.save_pool(metadata.clone()).await?;
        for disk in &disks {
            store.save_member_disk(disk.clone()).await?;
        }
        metadata.lifecycle = PoolLifecycle::Active;
        metadata.revision += 1;
        store.save_pool(metadata.clone()).await?;
        Ok(Self::assemble(metadata, disks, store, config))
    }

    pub(crate) fn restore(
        metadata: PoolMetadata,
        disks: Vec<MemberDisk>,
        store: Arc<dyn ControlPlaneStore>,
        config: RuntimeConfig,
    ) -> Arc<Self> {
        Self::assemble(metadata, disks, store, config)
    }

    fn assemble(
        metadata: PoolMetadata,
        disks: Vec<MemberDisk>,
        store: Arc<dyn ControlPlaneStore>,
        config: RuntimeConfig,
    ) -> Arc<Self> {
        let pool_id = metadata.spec.id.clone();
        let (pool_nodes, pool_node_host) = PoolNodeService::spawn(&pool_id, config.clone());
        let (virtual_disks, virtual_disk_host) =
            VirtualDiskService::spawn(&pool_id, config.clone());
        let (member_disks, member_disk_host) = MemberDiskService::spawn(
            pool_id,
            disks,
            store.clone(),
            pool_nodes.clone(),
            virtual_disks.clone(),
            config,
        );
        let member_disk_control = member_disk_host.control.clone();
        let member_disk_observer = member_disk_host.observer.clone();
        let virtual_disk_observer = virtual_disk_host.observer.clone();
        Arc::new(Self {
            metadata: StateCell::new(metadata),
            member_disks,
            virtual_disks,
            pool_nodes,
            member_disk_control,
            member_disk_observer,
            virtual_disk_observer,
            hosts: Mutex::new(Some(PoolServiceHosts {
                member_disk: member_disk_host,
                virtual_disk: virtual_disk_host,
                pool_node: pool_node_host,
            })),
            store,
        })
    }

    pub fn id(&self) -> PoolId {
        self.metadata.read(|metadata| metadata.spec.id.clone())
    }

    pub fn metadata(&self) -> PoolMetadata {
        self.metadata.read(Clone::clone)
    }

    pub fn member_disks(&self) -> &MemberDiskService {
        &self.member_disks
    }

    pub fn virtual_disks(&self) -> &VirtualDiskService {
        &self.virtual_disks
    }

    pub fn pool_nodes(&self) -> &PoolNodeService {
        &self.pool_nodes
    }

    pub fn member_disk_control(&self) -> ControlHandle {
        self.member_disk_control.clone()
    }

    pub fn member_disk_observer(&self) -> ServiceObserver {
        self.member_disk_observer.clone()
    }

    pub fn virtual_disk_observer(&self) -> ServiceObserver {
        self.virtual_disk_observer.clone()
    }

    pub async fn snapshot(&self) -> RuntimeResult<PoolSnapshot> {
        Ok(PoolSnapshot {
            metadata: self.metadata(),
            member_disk_count: self.member_disks.list().await?.len(),
        })
    }

    pub async fn update(&self, patch: PoolPatch) -> RuntimeResult<PoolMetadata> {
        let mut next = self.metadata();
        next.apply(patch);
        self.store.save_pool(next.clone()).await?;
        self.metadata.update(|metadata| *metadata = next.clone());
        Ok(next)
    }

    pub(crate) async fn mark_draining(&self) -> RuntimeResult<()> {
        let mut next = self.metadata();
        next.lifecycle = PoolLifecycle::Draining;
        next.revision += 1;
        self.store.save_pool(next.clone()).await?;
        self.metadata.update(|metadata| *metadata = next);
        Ok(())
    }

    pub async fn shutdown(&self) -> RuntimeResult<()> {
        let hosts = self
            .hosts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        match hosts {
            Some(hosts) => hosts.shutdown().await,
            None => Ok(()),
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        // Dropping ManagedService aborts any root task still owned here.
        let _ = self
            .hosts
            .get_mut()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }
}
