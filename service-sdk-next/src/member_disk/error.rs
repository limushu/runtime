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
    NeedsRecovery(String),
    PartialSpace {
        allocated: bool,
        committed: Vec<super::AllocatedBlock>,
        cause: Box<MemberDiskError>,
    },
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
            Self::Cancelled => formatter.write_str("workflow was cancelled"),
            Self::Internal(reason) => write!(formatter, "internal error: {reason}"),
            Self::NeedsRecovery(reason) => write!(formatter, "reconciliation required: {reason}"),
            Self::PartialSpace {
                allocated,
                committed,
                cause,
            } => write!(
                formatter,
                "partial space operation (allocate={allocated}, committed={}): {cause}",
                committed.len()
            ),
        }
    }
}

impl std::error::Error for MemberDiskError {}

impl crate::runtime::workflow::WorkflowFailure for MemberDiskError {
    fn is_cancelled(&self) -> bool {
        match self {
            Self::Cancelled => true,
            Self::PartialSpace { cause, .. } => cause.is_cancelled(),
            _ => false,
        }
    }
    fn cancelled() -> Self {
        Self::Cancelled
    }
    fn recovery(reason: &'static str) -> Self {
        Self::NeedsRecovery(reason.into())
    }
    fn needs_recovery(&self) -> bool {
        match self {
            Self::NeedsRecovery(_) => true,
            Self::PartialSpace { cause, .. } => cause.needs_recovery(),
            _ => false,
        }
    }
}
