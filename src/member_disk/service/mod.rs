mod client;
mod error;
mod operations;
mod reconcile;
mod runtime;

pub use client::MemberDiskClient;
pub use error::MemberDiskServiceError;

use super::{
    DiskUuid, MemberDisk, MemberDiskUpdate, MetadataService, PoolNodeService, VirtualDiskService,
};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

/// MemberDisk domain service.
///
/// The service owns the committed in-memory object directory. External events
/// update object facts or intent first; reconciliation then reads the object
/// and executes one matching business operation.
pub struct MemberDiskService {
    disks: Mutex<HashMap<DiskUuid, MemberDisk>>,
    metadata: Arc<dyn MetadataService>,
    pool_nodes: Arc<dyn PoolNodeService>,
    virtual_disks: Arc<dyn VirtualDiskService>,
    recovery_window: Duration,
}

impl MemberDiskService {
    pub fn new(
        disks: Vec<MemberDisk>,
        metadata: Arc<dyn MetadataService>,
        pool_nodes: Arc<dyn PoolNodeService>,
        virtual_disks: Arc<dyn VirtualDiskService>,
        recovery_window: Duration,
    ) -> Self {
        let disks = disks
            .into_iter()
            .map(|disk| (disk.uuid().clone(), disk))
            .collect();

        Self {
            disks: Mutex::new(disks),
            metadata,
            pool_nodes,
            virtual_disks,
            recovery_window,
        }
    }

    async fn get_member(&self, disk: &DiskUuid) -> Result<MemberDisk, MemberDiskServiceError> {
        self.disks
            .lock()
            .await
            .get(disk)
            .cloned()
            .ok_or_else(|| MemberDiskServiceError::UnknownDisk(disk.clone()))
    }

    /// Commits one completed MemberDisk change: validate -> SDB -> memory.
    async fn commit_change(
        &self,
        disk: &DiskUuid,
        change: MemberDiskUpdate,
    ) -> Result<bool, MemberDiskServiceError> {
        let mut disks = self.disks.lock().await;
        let member = disks
            .get_mut(disk)
            .ok_or_else(|| MemberDiskServiceError::UnknownDisk(disk.clone()))?;

        let changed = member.validate_update(&change)?;
        if !changed {
            return Ok(false);
        }

        self.metadata.update_member_disk(disk, &change).await?;
        member.apply_committed(&change);
        Ok(true)
    }
}
