mod activities;
mod error;
mod model;
mod ports;
mod protocol;
mod service;
mod workflows;

pub use crate::runtime::workflow::WorkflowStatus;
pub use error::MemberDiskError;
pub use model::{
    AllocatedBlock, AllocationState, BlockId, DiskUuid, IoState, MemberDisk, MemberDiskChange,
    MemberDiskState,
};
pub use ports::{
    DiskOpenResult, DiskStateChange, MemberDiskCommit, MemberDiskMetadata, PoolNodes, PortError,
    VirtualDisks,
};
pub use protocol::{
    AllocateBlocks, CommandKind, CommandSnapshot, CommandStatus, DiskEvent, DiskOutcome,
    EpochMillis, MemberDiskCommand, MemberDiskQuery, MemberDiskQueryReply, MemberDiskReply,
    MemberDiskSnapshot, PhysicalState, ReleaseBlocks, WorkflowSnapshot, now_millis,
};
pub use service::{MemberDiskConfig, MemberDiskService};

#[cfg(test)]
mod tests;
