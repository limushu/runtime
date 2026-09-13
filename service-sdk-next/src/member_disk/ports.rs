use async_trait::async_trait;
use std::fmt;
use tokio_util::sync::CancellationToken;

use super::{DiskUuid, IoState, MemberDiskChange};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortError(pub String);

impl fmt::Display for PortError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for PortError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskCommit {
    pub disk: DiskUuid,
    pub change: MemberDiskChange,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskStateChange {
    pub disk: DiskUuid,
    pub state: IoState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskOpenResult {
    pub disk: DiskUuid,
    pub result: Result<(), PortError>,
}

#[async_trait]
pub trait MemberDiskMetadata: Send + Sync + 'static {
    async fn commit(&self, changes: Vec<MemberDiskCommit>) -> Vec<Result<(), PortError>>;
}

#[async_trait]
pub trait PoolNodes: Send + Sync + 'static {
    async fn open_disks(&self, disks: Vec<DiskUuid>) -> Result<Vec<DiskOpenResult>, PortError>;

    async fn push_disk_states(&self, changes: Vec<DiskStateChange>) -> Result<(), PortError>;
}

#[async_trait]
pub trait VirtualDisks: Send + Sync + 'static {
    /// Returns only after any cancellation has reached a stable business boundary.
    async fn evacuate(&self, disk: &DiskUuid, cancel: &CancellationToken) -> Result<(), PortError>;
}
