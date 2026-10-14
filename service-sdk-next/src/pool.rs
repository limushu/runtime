//! Composition and routing. Each Pool has one independent Runtime root.
use crate::{member_disk::*, process_manager::ProcessManager, runtime::*, vd::VdService};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Notify;

#[cfg(test)]
mod tests;

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub recovery_window: Duration,
    pub retry_delay: Duration,
    pub max_retries: u32,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            recovery_window: Duration::from_secs(30),
            retry_delay: Duration::from_millis(100),
            max_retries: 3,
        }
    }
}

pub struct Pool {
    id: Arc<str>,
    disk: DiskService,
    vd: VdService,
    recovery_window: Duration,
    changed: Arc<Notify>,
}

impl Pool {
    /// Provider instances must already be scoped to this Pool. Local identity
    /// checks cannot namespace a remote database or data-plane connection.
    pub fn new(
        id: impl Into<Arc<str>>,
        disks: impl IntoIterator<Item = MemberDisk>,
        metadata: Arc<dyn MemberDiskMetadata>,
        nodes: Arc<dyn PoolNodes>,
        vd: Arc<dyn VirtualDisks>,
        config: PoolConfig,
    ) -> Result<Self, MemberDiskError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(MemberDiskError::InvalidRequest("Pool ID is empty"));
        }
        let changed = Arc::new(Notify::new());
        let disk = DiskService::new(
            id.clone(),
            disks,
            metadata,
            nodes,
            DiskConfig {
                retry_delay: config.retry_delay,
                max_retries: config.max_retries,
            },
            changed.clone(),
        )?;
        let vd = VdService::new(id.clone(), disk.identity(), vd, changed.clone());
        Ok(Self {
            id,
            disk,
            vd,
            recovery_window: config.recovery_window,
            changed,
        })
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn disks(&self) -> DiskService {
        self.disk.clone()
    }
    pub fn virtual_disks(&self) -> VdService {
        self.vd.clone()
    }
    pub fn start(self, config: ServiceConfig) -> ServiceInstance<ProcessManager> {
        spawn_service(
            ProcessManager::new(self.disk, self.vd, self.recovery_window, self.changed),
            config,
        )
    }
}

pub struct PoolHandle {
    pub client: ServiceClient<ProcessManager>,
    pub control: ServiceControl,
    pub observer: ServiceObserver<crate::process_manager::ProcessSnapshot>,
}

#[derive(Debug)]
pub enum PoolManagerError {
    DuplicatePool(String),
    UnknownPool(String),
    Control(ControlError),
    RootFailed(String),
}

/// Owns root task lifetimes; routes by explicit Pool identity. No disk/BG
/// workflow or resource policy is implemented here.
#[derive(Default)]
pub struct PoolManager {
    pools: BTreeMap<String, ServiceInstance<ProcessManager>>,
}

impl PoolManager {
    pub fn insert(
        &mut self,
        pool: Pool,
        config: ServiceConfig,
    ) -> Result<PoolHandle, PoolManagerError> {
        let id = pool.id().to_owned();
        if self.pools.contains_key(&id) {
            return Err(PoolManagerError::DuplicatePool(id));
        }
        self.pools.insert(id.clone(), pool.start(config));
        self.get(&id)
    }
    pub fn get(&self, id: &str) -> Result<PoolHandle, PoolManagerError> {
        let instance = self
            .pools
            .get(id)
            .ok_or_else(|| PoolManagerError::UnknownPool(id.into()))?;
        Ok(PoolHandle {
            client: instance.client.clone(),
            control: instance.control.clone(),
            observer: instance.observer.clone(),
        })
    }
    pub fn ids(&self) -> Vec<String> {
        self.pools.keys().cloned().collect()
    }

    /// Keep ownership while waiting, so cancellation of this management call
    /// does not implicitly abort the Pool or remove its route.
    pub async fn remove(&mut self, id: &str) -> Result<(), PoolManagerError> {
        let instance = self
            .pools
            .get_mut(id)
            .ok_or_else(|| PoolManagerError::UnknownPool(id.into()))?;
        if instance.observer.snapshot().lifecycle != Lifecycle::Stopped {
            instance
                .control
                .stop()
                .await
                .map_err(PoolManagerError::Control)?;
        }
        instance
            .task
            .join()
            .await
            .map_err(|e| PoolManagerError::RootFailed(e.to_string()))?;
        self.pools.remove(id);
        Ok(())
    }
}
