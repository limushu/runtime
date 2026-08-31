use crate::domains::member_disk::model::MemberDiskRecord;
use crate::kernel::{MemberDiskId, PoolId};
use crate::pool::model::PoolMetadata;
use async_trait::async_trait;
use control_runtime::{RuntimeError, RuntimeResult};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Persistence port for decisions owned by the Pool control plane.
///
/// A production adapter maps these operations onto SDB. External topology
/// facts are intentionally absent: DiskMap and NodeMap remain their authority.
#[async_trait]
pub trait ControlPlaneStore: Send + Sync + 'static {
    async fn list_pools(&self) -> RuntimeResult<Vec<PoolId>>;
    async fn load_pool(&self, pool: &PoolId) -> RuntimeResult<Option<PoolMetadata>>;
    async fn save_pool(&self, metadata: PoolMetadata) -> RuntimeResult<()>;
    async fn delete_pool(&self, pool: &PoolId) -> RuntimeResult<()>;

    async fn load_member_disks(&self, pool: &PoolId) -> RuntimeResult<Vec<MemberDiskRecord>>;
    async fn save_member_disk(&self, record: MemberDiskRecord) -> RuntimeResult<()>;
    async fn delete_member_disk(&self, pool: &PoolId, disk: &MemberDiskId) -> RuntimeResult<()>;
}

#[derive(Debug, Default)]
struct MemoryState {
    pools: HashMap<PoolId, PoolMetadata>,
    member_disks: HashMap<(PoolId, MemberDiskId), MemberDiskRecord>,
}

/// Deterministic in-memory adapter used by the executable model and tests.
#[derive(Debug, Clone, Default)]
pub struct InMemoryControlPlaneStore {
    state: Arc<RwLock<MemoryState>>,
}

#[async_trait]
impl ControlPlaneStore for InMemoryControlPlaneStore {
    async fn list_pools(&self) -> RuntimeResult<Vec<PoolId>> {
        Ok(self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pools
            .keys()
            .cloned()
            .collect())
    }

    async fn load_pool(&self, pool: &PoolId) -> RuntimeResult<Option<PoolMetadata>> {
        Ok(self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pools
            .get(pool)
            .cloned())
    }

    async fn save_pool(&self, metadata: PoolMetadata) -> RuntimeResult<()> {
        self.state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pools
            .insert(metadata.spec.id.clone(), metadata);
        Ok(())
    }

    async fn delete_pool(&self, pool: &PoolId) -> RuntimeResult<()> {
        let mut state = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.pools.remove(pool);
        state.member_disks.retain(|(owner, _), _| owner != pool);
        Ok(())
    }

    async fn load_member_disks(&self, pool: &PoolId) -> RuntimeResult<Vec<MemberDiskRecord>> {
        Ok(self
            .state
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .member_disks
            .iter()
            .filter(|((owner, _), _)| owner == pool)
            .map(|(_, record)| record.clone())
            .collect())
    }

    async fn save_member_disk(&self, record: MemberDiskRecord) -> RuntimeResult<()> {
        let key = (record.pool().clone(), record.id().clone());
        self.state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .member_disks
            .insert(key, record);
        Ok(())
    }

    async fn delete_member_disk(&self, pool: &PoolId, disk: &MemberDiskId) -> RuntimeResult<()> {
        let removed = self
            .state
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .member_disks
            .remove(&(pool.clone(), disk.clone()));
        if removed.is_none() {
            return Err(RuntimeError::InvalidState(format!(
                "unknown member disk {disk} in pool {pool}"
            )));
        }
        Ok(())
    }
}
