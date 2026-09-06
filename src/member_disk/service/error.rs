use crate::member_disk::{Cancelled, DiskUuid, MemberDiskError, MetadataError, PoolNodeError};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskServiceError {
    UnknownDisk(DiskUuid),
    ServiceStopped,
    Cancelled,
    PoolNode(PoolNodeError),
    Metadata(MetadataError),
    InvalidState(MemberDiskError),
}

impl fmt::Display for MemberDiskServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownDisk(disk) => write!(f, "unknown MemberDisk {disk}"),
            Self::ServiceStopped => write!(f, "MemberDisk service has stopped"),
            Self::Cancelled => write!(f, "MemberDisk operation was cancelled"),
            Self::PoolNode(error) => error.fmt(f),
            Self::Metadata(error) => write!(f, "MemberDisk metadata update failed: {error}"),
            Self::InvalidState(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for MemberDiskServiceError {}

impl From<Cancelled> for MemberDiskServiceError {
    fn from(_: Cancelled) -> Self {
        Self::Cancelled
    }
}

impl From<MemberDiskError> for MemberDiskServiceError {
    fn from(error: MemberDiskError) -> Self {
        Self::InvalidState(error)
    }
}

impl From<PoolNodeError> for MemberDiskServiceError {
    fn from(error: PoolNodeError) -> Self {
        match error {
            PoolNodeError::Cancelled => Self::Cancelled,
            error => Self::PoolNode(error),
        }
    }
}

impl From<MetadataError> for MemberDiskServiceError {
    fn from(error: MetadataError) -> Self {
        Self::Metadata(error)
    }
}
