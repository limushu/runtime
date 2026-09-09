use super::{DiskUuid, MemberDiskMutation};
use async_trait::async_trait;
use std::fmt;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortError(pub String);

impl fmt::Display for PortError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for PortError {}

/// SDB-facing boundary. A successful return means the mutation is durable.
#[async_trait]
pub trait MemberDiskMetadata: Send + Sync + 'static {
    async fn commit(&self, disk: &DiskUuid, mutation: &MemberDiskMutation)
    -> Result<(), PortError>;
}

/// Capability supplied by the PoolNode domain.
#[async_trait]
pub trait PoolNodes: Send + Sync + 'static {
    /// Mandatory safety action. The MemberDisk service retries failures.
    async fn set_disk_down(&self, disk: &DiskUuid) -> Result<(), PortError>;

    /// Online is two-stage: open first, then publish UP.
    async fn open_disk(&self, disk: &DiskUuid, cancel: &CancellationToken)
    -> Result<(), PortError>;

    async fn publish_disk_up(
        &self,
        disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), PortError>;
}

/// Capability supplied by the VirtualDisk domain.
#[async_trait]
pub trait VirtualDisks: Send + Sync + 'static {
    async fn has_references(&self, disk: &DiskUuid) -> Result<bool, PortError>;

    /// Returns only after in-flight BG work reaches a stable boundary.
    async fn evacuate(&self, disk: &DiskUuid, cancel: &CancellationToken) -> Result<(), PortError>;
}
