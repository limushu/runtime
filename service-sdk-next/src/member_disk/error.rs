use std::fmt;

use super::DiskUuid;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskError {
    UnknownDisk(DiskUuid),
    InvalidState {
        disk: DiskUuid,
        reason: &'static str,
    },
    InvalidRequest(&'static str),
    NoSpace,
    Metadata(String),
    PoolNodes(String),
    VirtualDisks(String),
    Cancelled,
    Internal(String),
}

impl fmt::Display for MemberDiskError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownDisk(disk) => write!(formatter, "unknown member disk {disk}"),
            Self::InvalidState { disk, reason } => {
                write!(
                    formatter,
                    "member disk {disk} is in an invalid state: {reason}"
                )
            }
            Self::InvalidRequest(reason) => write!(formatter, "invalid request: {reason}"),
            Self::NoSpace => formatter.write_str("not enough eligible member-disk space"),
            Self::Metadata(reason) => write!(formatter, "metadata operation failed: {reason}"),
            Self::PoolNodes(reason) => write!(formatter, "pool-node operation failed: {reason}"),
            Self::VirtualDisks(reason) => {
                write!(formatter, "virtual-disk operation failed: {reason}")
            }
            Self::Cancelled => formatter.write_str("request was cancelled"),
            Self::Internal(reason) => write!(formatter, "internal error: {reason}"),
        }
    }
}

impl std::error::Error for MemberDiskError {}
