use super::{DiskUuid, PortError};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskServiceError {
    UnknownDisk(DiskUuid),
    InvalidState(String),
    Metadata(PortError),
    PoolNodes(PortError),
    VirtualDisks(PortError),
    Cancelled,
}

impl fmt::Display for MemberDiskServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownDisk(disk) => write!(formatter, "unknown MemberDisk {disk}"),
            Self::InvalidState(message) => message.fmt(formatter),
            Self::Metadata(error) => write!(formatter, "metadata: {error}"),
            Self::PoolNodes(error) => write!(formatter, "PoolNode: {error}"),
            Self::VirtualDisks(error) => write!(formatter, "VirtualDisk: {error}"),
            Self::Cancelled => formatter.write_str("operation cancelled at a stable boundary"),
        }
    }
}

impl std::error::Error for MemberDiskServiceError {}
