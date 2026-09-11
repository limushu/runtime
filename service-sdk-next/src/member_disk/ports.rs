use super::{DiskStateChange, DiskUuid, MemberDiskCommit};
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskOpenResult {
    pub disk: DiskUuid,
    pub result: Result<(), PortError>,
}

/// SDB-facing boundary. A successful return means the mutation is durable.
#[async_trait]
pub trait MemberDiskMetadata: Send + Sync + 'static {
    async fn commit(&self, commits: Vec<MemberDiskCommit>) -> Result<(), PortError>;
}

/// Capability supplied by the PoolNode domain.
#[async_trait]
pub trait PoolNodes: Send + Sync + 'static {
    /// Opens all supplied disks in one network request and returns one result
    /// per disk. This is phase one of the online workflow.
    async fn open_disks(&self, disks: Vec<DiskUuid>) -> Result<Vec<DiskOpenResult>, PortError>;

    /// Pushes the supplied IO states to all currently serviceable Pool nodes
    /// in one network request. MemberDisk decides the contents of the batch.
    async fn push_disk_states(&self, changes: Vec<DiskStateChange>) -> Result<(), PortError>;
}

/// Capability supplied by the VirtualDisk domain.
#[async_trait]
pub trait VirtualDisks: Send + Sync + 'static {
    /// Returns only after in-flight BG work reaches a stable boundary.
    async fn evacuate(&self, disk: &DiskUuid, cancel: &CancellationToken) -> Result<(), PortError>;
}
