use crate::{metadata::*, *};
use async_trait::async_trait;
use std::sync::Mutex;
/// In-memory KV with the same repository rules as persistent adapters. The log is test diagnostics.
pub struct MemoryMetadata {
    repository: crate::metadata::KvMetadata,
    log: Mutex<Vec<(PoolId, MetadataChange)>>,
}
impl Default for MemoryMetadata {
    fn default() -> Self {
        Self {
            repository: crate::metadata::KvMetadata::new(std::sync::Arc::new(
                crate::metadata::MemoryKv::default(),
            )),
            log: Mutex::default(),
        }
    }
}
impl MemoryMetadata {
    pub fn committed(&self) -> Vec<(PoolId, MetadataChange)> {
        self.log.lock().unwrap().clone()
    }
}
#[async_trait]
impl MetadataStore for MemoryMetadata {
    async fn snapshot(&self, pool: &PoolId) -> Outcome<MetadataSnapshot> {
        self.repository.snapshot(pool).await
    }
    async fn commit(&self, pool: &PoolId, change: MetadataChange) -> Outcome<()> {
        self.repository.commit(pool, change.clone()).await?;
        self.log.lock().unwrap().push((pool.clone(), change));
        Ok(())
    }
}
