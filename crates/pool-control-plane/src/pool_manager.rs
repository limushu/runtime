use crate::domains::member_disk::model::{DiskSharing, MemberDisk, PhysicalState};
use crate::kernel::{MemberDiskId, PhysicalDiskId, PoolId};
use crate::pool::{Pool, PoolMetadata, PoolPatch, PoolSnapshot, PoolSpec};
use crate::ports::ControlPlaneStore;
use control_runtime::{RuntimeConfig, RuntimeError, RuntimeResult};
use futures::future::join_all;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskFactResult {
    pub pool: PoolId,
    pub result: RuntimeResult<MemberDisk>,
}

#[derive(Clone)]
struct DiskOwner {
    pool: PoolId,
    member_disk: crate::kernel::MemberDiskId,
    sharing: DiskSharing,
}

/// Monitor-level owner of all independently loaded Pool instances.
pub struct PoolManager {
    pools: RwLock<HashMap<PoolId, Arc<Pool>>>,
    disk_owners: RwLock<HashMap<PhysicalDiskId, Vec<DiskOwner>>>,
    store: Arc<dyn ControlPlaneStore>,
    config: RuntimeConfig,
    lifecycle_gate: Mutex<()>,
}

impl PoolManager {
    pub fn new(store: Arc<dyn ControlPlaneStore>, config: RuntimeConfig) -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
            disk_owners: RwLock::new(HashMap::new()),
            store,
            config,
            lifecycle_gate: Mutex::new(()),
        }
    }

    pub async fn create_pool(
        &self,
        spec: PoolSpec,
        disks: Vec<MemberDisk>,
    ) -> RuntimeResult<Arc<Pool>> {
        let _guard = self.lifecycle_gate.lock().await;
        if self
            .pools
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(&spec.id)
        {
            return Err(RuntimeError::Rejected(format!(
                "pool {} is already loaded",
                spec.id
            )));
        }
        self.validate_disk_owners(&disks)?;
        let pool =
            Pool::create(spec, disks.clone(), self.store.clone(), self.config.clone()).await?;
        if let Err(error) = self.install(pool.clone(), &disks) {
            let pool_id = pool.id();
            let _ = pool.shutdown().await;
            let _ = self.store.delete_pool(&pool_id).await;
            return Err(error);
        }
        Ok(pool)
    }

    pub async fn restore_all(&self) -> RuntimeResult<Vec<PoolId>> {
        let _guard = self.lifecycle_gate.lock().await;
        let mut restored = Vec::new();
        for pool_id in self.store.list_pools().await? {
            if self
                .pools
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .contains_key(&pool_id)
            {
                continue;
            }
            let metadata = self
                .store
                .load_pool(&pool_id)
                .await?
                .ok_or_else(|| RuntimeError::InvalidState(format!("missing pool {pool_id}")))?;
            let disks = self.store.load_member_disks(&pool_id).await?;
            let pool = Pool::restore(
                metadata,
                disks.clone(),
                self.store.clone(),
                self.config.clone(),
            );
            self.install(pool, &disks)?;
            restored.push(pool_id);
        }
        Ok(restored)
    }

    fn install(&self, pool: Arc<Pool>, disks: &[MemberDisk]) -> RuntimeResult<()> {
        let pool_id = pool.id();
        let mut pools = self
            .pools
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if pools.contains_key(&pool_id) {
            return Err(RuntimeError::Rejected(format!(
                "pool {pool_id} was installed concurrently"
            )));
        }
        let mut owners = self
            .disk_owners
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self::validate_against(&owners, disks)?;
        pools.insert(pool_id.clone(), pool);
        for disk in disks {
            owners
                .entry(disk.physical_disk().clone())
                .or_default()
                .push(DiskOwner {
                    pool: pool_id.clone(),
                    member_disk: disk.id().clone(),
                    sharing: disk.sharing(),
                });
        }
        Ok(())
    }

    fn validate_disk_owners(&self, disks: &[MemberDisk]) -> RuntimeResult<()> {
        let owners = self
            .disk_owners
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        Self::validate_against(&owners, disks)
    }

    fn validate_against(
        current: &HashMap<PhysicalDiskId, Vec<DiskOwner>>,
        disks: &[MemberDisk],
    ) -> RuntimeResult<()> {
        let mut proposed = current.clone();
        for disk in disks {
            let owners = proposed.entry(disk.physical_disk().clone()).or_default();
            if owners
                .iter()
                .any(|owner| owner.pool == *disk.pool() && owner.member_disk == *disk.id())
            {
                return Err(RuntimeError::Rejected(format!(
                    "physical disk {} is already registered as {} in pool {}",
                    disk.physical_disk(),
                    disk.id(),
                    disk.pool()
                )));
            }
            let compatible = match disk.sharing() {
                DiskSharing::Exclusive => owners.is_empty(),
                DiskSharing::SharedCache => owners
                    .iter()
                    .all(|owner| owner.sharing == DiskSharing::SharedCache),
            };
            if !compatible {
                return Err(RuntimeError::Rejected(format!(
                    "physical disk {} cannot mix exclusive and shared-cache ownership",
                    disk.physical_disk()
                )));
            }
            owners.push(DiskOwner {
                pool: disk.pool().clone(),
                member_disk: disk.id().clone(),
                sharing: disk.sharing(),
            });
        }
        Ok(())
    }

    pub fn pool(&self, id: &PoolId) -> RuntimeResult<Arc<Pool>> {
        self.pools
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(id)
            .cloned()
            .ok_or_else(|| RuntimeError::InvalidState(format!("pool {id} is not loaded")))
    }

    pub async fn get_pool(&self, id: &PoolId) -> RuntimeResult<PoolSnapshot> {
        self.pool(id)?.snapshot().await
    }

    pub async fn update_pool(&self, id: &PoolId, patch: PoolPatch) -> RuntimeResult<PoolMetadata> {
        self.pool(id)?.update(patch).await
    }

    pub async fn create_member_disk(
        &self,
        pool_id: &PoolId,
        disk: MemberDisk,
    ) -> RuntimeResult<MemberDisk> {
        let _guard = self.lifecycle_gate.lock().await;
        if disk.pool() != pool_id {
            return Err(RuntimeError::Rejected(format!(
                "member disk {} belongs to pool {}, not {pool_id}",
                disk.id(),
                disk.pool()
            )));
        }
        self.validate_disk_owners(std::slice::from_ref(&disk))?;
        let created = self
            .pool(pool_id)?
            .member_disks()
            .create(disk.clone())
            .await?;
        self.disk_owners
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(disk.physical_disk().clone())
            .or_default()
            .push(DiskOwner {
                pool: disk.pool().clone(),
                member_disk: disk.id().clone(),
                sharing: disk.sharing(),
            });
        Ok(created)
    }

    pub async fn delete_member_disk(
        &self,
        pool_id: &PoolId,
        disk: MemberDiskId,
    ) -> RuntimeResult<()> {
        let _guard = self.lifecycle_gate.lock().await;
        let pool = self.pool(pool_id)?;
        let current = pool.member_disks().get(disk.clone()).await?;
        pool.member_disks().delete(disk.clone()).await?;
        let physical = current.physical_disk().clone();
        let mut index = self
            .disk_owners
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let remove_key = index.get_mut(&physical).is_some_and(|owners| {
            owners.retain(|owner| owner.pool != *pool_id || owner.member_disk != disk);
            owners.is_empty()
        });
        if remove_key {
            index.remove(&physical);
        }
        Ok(())
    }

    pub async fn delete_pool(&self, id: &PoolId) -> RuntimeResult<()> {
        let _guard = self.lifecycle_gate.lock().await;
        let pool = self.pool(id)?;
        pool.mark_draining().await?;
        pool.shutdown().await?;
        self.store.delete_pool(id).await?;
        self.pools
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(id);
        self.disk_owners
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|_, owners| {
                owners.retain(|owner| &owner.pool != id);
                !owners.is_empty()
            });
        Ok(())
    }

    /// Route one normalized DiskMap fact to every affected Pool. Ordinary
    /// MemberDisks have one owner; the vector also models the explicitly
    /// allowed shared-cache case without changing the routing contract.
    pub async fn route_disk_fact(
        &self,
        physical_disk: &PhysicalDiskId,
        state: PhysicalState,
    ) -> RuntimeResult<Vec<DiskFactResult>> {
        let targets = self
            .disk_owners
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(physical_disk)
            .cloned()
            .ok_or_else(|| {
                RuntimeError::InvalidState(format!(
                    "physical disk {physical_disk} has no Pool owner"
                ))
            })?;
        let calls = targets.into_iter().map(|owner| {
            let pool = self.pool(&owner.pool);
            async move {
                let result = match pool {
                    Ok(pool) => {
                        pool.member_disks()
                            .apply_physical(owner.member_disk, state)
                            .await
                    }
                    Err(error) => Err(error),
                };
                DiskFactResult {
                    pool: owner.pool,
                    result,
                }
            }
        });
        Ok(join_all(calls).await)
    }

    pub fn loaded_pool_ids(&self) -> Vec<PoolId> {
        self.pools
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .keys()
            .cloned()
            .collect()
    }
}
