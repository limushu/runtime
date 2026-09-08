use crate::member_disk::{
    DiskUuid, MemberDiskError, MetadataError, PoolNodeError, VirtualDiskError,
};
use crate::runtime::ServiceUnavailable;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskServiceError {
    UnknownDisk(DiskUuid),
    EmptyAllocation,
    InsufficientAllocationCandidates {
        tier: String,
        requested: usize,
        available: usize,
    },
    ServiceStopped,
    Unavailable(ServiceUnavailable),
    Cancelled,
    PoolNode(PoolNodeError),
    VirtualDisk(VirtualDiskError),
    Metadata(MetadataError),
    TransitionIncomplete(String),
    InvalidState(MemberDiskError),
}

impl fmt::Display for MemberDiskServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownDisk(disk) => write!(f, "unknown MemberDisk {disk}"),
            Self::EmptyAllocation => write!(f, "BLK allocation count must be greater than zero"),
            Self::InsufficientAllocationCandidates {
                tier,
                requested,
                available,
            } => write!(
                f,
                "Tier {tier} has {available} eligible allocation candidates, but {requested} were requested"
            ),
            Self::ServiceStopped => write!(f, "MemberDisk service has stopped"),
            Self::Unavailable(reason) => reason.fmt(f),
            Self::Cancelled => write!(f, "MemberDisk operation was cancelled"),
            Self::PoolNode(error) => error.fmt(f),
            Self::VirtualDisk(error) => error.fmt(f),
            Self::Metadata(error) => write!(f, "MemberDisk metadata update failed: {error}"),
            Self::TransitionIncomplete(message) => message.fmt(f),
            Self::InvalidState(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for MemberDiskServiceError {}

impl From<ServiceUnavailable> for MemberDiskServiceError {
    fn from(reason: ServiceUnavailable) -> Self {
        Self::Unavailable(reason)
    }
}

impl From<MemberDiskError> for MemberDiskServiceError {
    fn from(error: MemberDiskError) -> Self {
        Self::InvalidState(error)
    }
}

impl From<VirtualDiskError> for MemberDiskServiceError {
    fn from(error: VirtualDiskError) -> Self {
        match error {
            VirtualDiskError::Cancelled => Self::Cancelled,
            error => Self::VirtualDisk(error),
        }
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
