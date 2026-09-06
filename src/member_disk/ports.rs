use super::{DiskIoState, DiskUuid, MemberDiskUpdate};
use async_trait::async_trait;
use std::fmt;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetadataError(String);

impl MetadataError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for MetadataError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for MetadataError {}

#[async_trait]
pub trait MetadataService: Send + Sync {
    /// Durably commits one field-level MemberDisk update to SDB.
    ///
    /// The service retries transient SDB failures internally and returns only
    /// after the change is committed. It never receives or owns the in-memory
    /// MemberDisk object.
    async fn update_member_disk(
        &self,
        disk: &DiskUuid,
        update: &MemberDiskUpdate,
    ) -> Result<(), MetadataError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserDpRequest {
    OpenDisk(DiskUuid),
    SetDiskState { disk: DiskUuid, state: DiskIoState },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cancelled;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolNodeError {
    Cancelled,
    Failed(String),
}

impl PoolNodeError {
    pub fn failed(message: impl Into<String>) -> Self {
        Self::Failed(message.into())
    }
}

impl fmt::Display for PoolNodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Cancelled => f.write_str("PoolNode broadcast was cancelled"),
            Self::Failed(message) => write!(f, "PoolNode broadcast failed: {message}"),
        }
    }
}

impl std::error::Error for PoolNodeError {}

#[async_trait]
pub trait PoolNodeService: Send + Sync {
    /// Makes one broadcast attempt to all currently serviceable Pool nodes.
    /// The caller decides whether a failed attempt is retried or returned.
    async fn broadcast(
        &self,
        cancel: &CancellationToken,
        request: UserDpRequest,
    ) -> Result<(), PoolNodeError>;
}

#[async_trait]
pub trait VirtualDiskService: Send + Sync {
    /// Returns after all BGs have left this MemberDisk. On cancellation, VDm
    /// first stops new BG work and settles work already in flight.
    async fn evacuate(&self, cancel: &CancellationToken, disk: &DiskUuid) -> Result<(), Cancelled>;
}
