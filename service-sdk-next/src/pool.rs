//! Pool is a composition/lifetime boundary, not a shared Future driver.
use crate::{
    member_disk::*,
    process_manager::*,
    runtime::{activity::*, *},
    vd::{VdClient, VdService},
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
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
    id: String,
    scope: ExecutionScope,
    disk: DiskService,
    vd: VdService,
    recovery_window: Duration,
}
impl Pool {
    /// Providers must already be scoped to this Pool's external namespace.
    pub fn new(
        id: impl Into<String>,
        disks: impl IntoIterator<Item = MemberDisk>,
        metadata: Arc<dyn MemberDiskMetadata>,
        nodes: Arc<dyn PoolNodes>,
        vd: Arc<dyn VirtualDisks>,
        config: PoolConfig,
    ) -> Result<Self, MemberDiskError> {
        let id = id.into();
        if id.trim().is_empty() {
            return Err(MemberDiskError::InvalidRequest("empty Pool ID"));
        }
        let scope = ExecutionScope::default();
        let disk = DiskService::new(
            &id,
            scope.clone(),
            disks,
            metadata,
            nodes,
            DiskConfig {
                retry_delay: config.retry_delay,
                max_retries: config.max_retries,
            },
        )?;
        let vd = VdService::new(scope.clone(), vd);
        Ok(Self {
            id,
            scope,
            disk,
            vd,
            recovery_window: config.recovery_window,
        })
    }
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn start(self, config: ServiceConfig) -> PoolInstance {
        let disk = spawn_service(ActivityService::new(self.disk), config.clone());
        let vd = spawn_service(ActivityService::new(self.vd), config.clone());
        let workflows = spawn_service(ProcessManager::new(self.scope.clone()), config);
        let handle = PoolHandle {
            id: self.id,
            recovery_window: self.recovery_window,
            client: workflows.client.clone(),
            control: workflows.control.clone(),
            observer: workflows.observer.clone(),
            disks: DiskClient(ActivityClient::new(disk.client.clone(), self.scope.clone())),
            virtual_disks: VdClient(ActivityClient::new(vd.client.clone(), self.scope)),
            disk_control: disk.control.clone(),
            disk_observer: disk.observer.clone(),
            vd_control: vd.control.clone(),
            vd_observer: vd.observer.clone(),
        };
        PoolInstance {
            handle,
            workflows,
            disk,
            vd,
        }
    }
}
#[derive(Clone)]
pub struct PoolHandle {
    pub id: String,
    pub recovery_window: Duration,
    pub client: ServiceClient<ProcessManager>,
    /// Controls only workflow scheduling, NOT the domain runtimes.
    pub control: ServiceControl,
    pub observer: ServiceObserver<ProcessSnapshot>,
    pub disks: DiskClient,
    pub virtual_disks: VdClient,
    pub disk_control: ServiceControl,
    pub disk_observer: ServiceObserver<ActivitySnapshot>,
    pub vd_control: ServiceControl,
    pub vd_observer: ServiceObserver<ActivitySnapshot>,
}
pub struct PoolInstance {
    pub handle: PoolHandle,
    workflows: ServiceInstance<ProcessManager>,
    disk: ServiceInstance<ActivityService<DiskService>>,
    vd: ServiceInstance<ActivityService<VdService>>,
}
async fn stop<S: Service>(instance: &mut ServiceInstance<S>) -> Result<(), PoolManagerError> {
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
        .map_err(|e| PoolManagerError::RootFailed(e.to_string()))
}
impl PoolInstance {
    /// Keep the workers alive until all workflow activities settle. Safe to
    /// cancel this waiting future and retry; task ownership stays in Self.
    pub async fn shutdown(&mut self) -> Result<(), PoolManagerError> {
        // Paused workers may own already accepted requests. Let their drivers
        // observe cancellation/settle those requests before awaiting the manager.
        for control in [
            (&self.disk.control, self.disk.observer.snapshot().lifecycle),
            (&self.vd.control, self.vd.observer.snapshot().lifecycle),
        ] {
            if control.1 == Lifecycle::Paused {
                control
                    .0
                    .resume()
                    .await
                    .map_err(PoolManagerError::Control)?;
            }
        }
        stop(&mut self.workflows).await?;
        stop(&mut self.disk).await?;
        stop(&mut self.vd).await
    }
}
#[derive(Debug)]
pub enum PoolManagerError {
    DuplicatePool(String),
    UnknownPool(String),
    Control(ControlError),
    RootFailed(String),
}
#[derive(Default)]
pub struct PoolManager {
    pools: BTreeMap<String, PoolInstance>,
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
        self.pools
            .get(id)
            .map(|p| p.handle.clone())
            .ok_or_else(|| PoolManagerError::UnknownPool(id.into()))
    }
    pub fn ids(&self) -> Vec<String> {
        self.pools.keys().cloned().collect()
    }
    pub async fn remove(&mut self, id: &str) -> Result<(), PoolManagerError> {
        self.pools
            .get_mut(id)
            .ok_or_else(|| PoolManagerError::UnknownPool(id.into()))?
            .shutdown()
            .await?;
        self.pools.remove(id);
        Ok(())
    }
}
