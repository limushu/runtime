mod actions;
mod commands;
mod error;
mod model;
mod ports;
mod protocol;
mod requests;
mod service;
mod state_machine;

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
    MemberDiskSnapshot, PhysicalState, ReleaseBlocks, RequestSnapshot, RequestStatus, now_millis,
};
#[doc(hidden)]
pub use requests::RequestCompletion;
pub use service::{MemberDiskConfig, MemberDiskService};

#[cfg(test)]
mod tests;
