use crate::domains::member_disk::protocol::{MemberDiskReply, MemberDiskRequest, MemberDiskState};
use crate::domains::member_disk::MemberDiskService;
use crate::domains::pool_node::protocol::{PoolNodeReply, PoolNodeRequest};
use crate::domains::pool_node::PoolNodeService;
use crate::domains::virtual_disk::protocol::{VirtualDiskReply, VirtualDiskRequest};
use crate::domains::virtual_disk::VirtualDiskService;
use crate::kernel::MemberDiskId;
use control_runtime::{
    spawn_service, ControlHandle, ManagedService, Router, RuntimeConfig, RuntimeError,
    RuntimeResult, ServiceObserver,
};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskBatchItem {
    pub disk: MemberDiskId,
    pub result: RuntimeResult<MemberDiskState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskBatchReply {
    pub items: Vec<MemberDiskBatchItem>,
}

/// Owns all service containers for exactly one Pool.
pub struct PoolRuntime {
    router: Router,
    member_disk: ManagedService,
    virtual_disk: ManagedService,
    pool_node: ManagedService,
}

impl PoolRuntime {
    pub fn new(disks: impl IntoIterator<Item = MemberDiskId>) -> Self {
        Self::with_config(disks, RuntimeConfig::default())
    }

    pub fn with_config(
        disks: impl IntoIterator<Item = MemberDiskId>,
        config: RuntimeConfig,
    ) -> Self {
        let router = Router::new();
        let pool_node = spawn_service(Arc::new(PoolNodeService::new()), &router, config.clone());
        let virtual_disk =
            spawn_service(Arc::new(VirtualDiskService::new()), &router, config.clone());
        let member_disk = spawn_service(
            Arc::new(MemberDiskService::new(router.clone(), disks)),
            &router,
            config,
        );
        Self {
            router,
            member_disk,
            virtual_disk,
            pool_node,
        }
    }

    pub fn router(&self) -> Router {
        self.router.clone()
    }

    pub fn member_disk_observer(&self) -> ServiceObserver {
        self.member_disk.observer.clone()
    }

    pub fn virtual_disk_observer(&self) -> ServiceObserver {
        self.virtual_disk.observer.clone()
    }

    pub fn member_disk_control(&self) -> ControlHandle {
        self.member_disk.control.clone()
    }

    pub fn force_abort_member_disk(&self) {
        self.member_disk.force_abort();
    }

    pub async fn offline(&self, disk: MemberDiskId) -> RuntimeResult<MemberDiskReply> {
        let mut reply = self.offline_many(vec![disk.clone()]).await?;
        let result = reply
            .items
            .pop()
            .ok_or_else(|| RuntimeError::Internal("single-item batch returned no result".into()))?;
        result.result.map(|state| MemberDiskReply { disk, state })
    }

    pub async fn online(&self, disk: MemberDiskId) -> RuntimeResult<MemberDiskReply> {
        let mut reply = self.online_many(vec![disk.clone()]).await?;
        let result = reply
            .items
            .pop()
            .ok_or_else(|| RuntimeError::Internal("single-item batch returned no result".into()))?;
        result.result.map(|state| MemberDiskReply { disk, state })
    }

    pub async fn offline_many(
        &self,
        disks: Vec<MemberDiskId>,
    ) -> RuntimeResult<MemberDiskBatchReply> {
        let requests = disks.iter().cloned().map(MemberDiskRequest::Offline);
        let results = self
            .router
            .call_batch("offline member disks", requests)
            .await?;
        Ok(MemberDiskBatchReply {
            items: disks
                .into_iter()
                .zip(results)
                .map(|(disk, result)| MemberDiskBatchItem {
                    disk,
                    result: result.map(|reply| reply.state),
                })
                .collect(),
        })
    }

    pub async fn online_many(
        &self,
        disks: Vec<MemberDiskId>,
    ) -> RuntimeResult<MemberDiskBatchReply> {
        let requests = disks.iter().cloned().map(MemberDiskRequest::Online);
        let results = self
            .router
            .call_batch("online member disks", requests)
            .await?;
        Ok(MemberDiskBatchReply {
            items: disks
                .into_iter()
                .zip(results)
                .map(|(disk, result)| MemberDiskBatchItem {
                    disk,
                    result: result.map(|reply| reply.state),
                })
                .collect(),
        })
    }

    pub async fn member_disk(&self, disk: MemberDiskId) -> RuntimeResult<MemberDiskReply> {
        self.router
            .call_root("query member disk", MemberDiskRequest::Get(disk))
            .await
    }

    pub async fn virtual_disk_stats(&self) -> RuntimeResult<VirtualDiskReply> {
        self.router
            .call_root("query virtual disk", VirtualDiskRequest::Stats)
            .await
    }

    pub async fn node_published_count(&self) -> RuntimeResult<PoolNodeReply> {
        self.router
            .call_root("query pool node", PoolNodeRequest::PublishedCount)
            .await
    }

    pub async fn shutdown(self) -> RuntimeResult<()> {
        self.member_disk.shutdown().await?;
        self.virtual_disk.shutdown().await?;
        self.pool_node.shutdown().await
    }
}
