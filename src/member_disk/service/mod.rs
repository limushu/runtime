mod allocation;
mod client;
mod error;
mod operations;
mod reconcile;
mod runtime;

pub use client::{
    Accepted, GetMemberDisk, MemberDiskClient, MemberDiskRuntime, WaitMemberDiskIdle,
};
pub use error::MemberDiskServiceError;

use super::{
    DiskUuid, MemberDisk, MemberDiskUpdate, MetadataService, PoolNodeService, VirtualDiskService,
};
use crate::runtime::ObjectTaskCoordinator;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

/// MemberDisk domain service.
///
/// The service owns the committed in-memory object directory. External events
/// update object facts or intent first; reconciliation then reads the object
/// and executes one matching business operation.
pub struct MemberDiskService {
    disks: Mutex<HashMap<DiskUuid, MemberDisk>>,
    mutation_gate: Mutex<()>,
    metadata: Arc<dyn MetadataService>,
    pool_nodes: Arc<dyn PoolNodeService>,
    virtual_disks: Arc<dyn VirtualDiskService>,
    recovery_window: Duration,
    object_tasks: ObjectTaskCoordinator<DiskUuid, super::MemberDiskEvent, MemberDiskServiceError>,
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
            mutation_gate: Mutex::new(()),
            metadata,
            pool_nodes,
            virtual_disks,
            recovery_window,
            object_tasks: ObjectTaskCoordinator::new(MemberDiskServiceError::Cancelled),
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
        let _mutation = self.mutation_gate.lock().await;
        let changed = self.get_member(disk).await?.validate_update(&change)?;
        if !changed {
            return Ok(false);
        }

        self.metadata.update_member_disk(disk, &change).await?;

        let mut disks = self.disks.lock().await;
        let member = disks
            .get_mut(disk)
            .expect("the mutation gate keeps a validated MemberDisk present");
        member.apply_committed(&change);
        Ok(true)
    }
}
