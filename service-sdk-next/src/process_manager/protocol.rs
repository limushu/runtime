use std::time::SystemTime;

use crate::runtime::CommandId;

use crate::member_disk::{AllocatedBlock, DiskUuid, MemberDisk, MemberDiskError};

pub type EpochMillis = u64;

pub fn now_millis() -> EpochMillis {
    SystemTime::UNIX_EPOCH
        .elapsed()
        .unwrap_or_default()
        .as_millis() as u64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalState {
    Up,
    Down,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskEvent {
    pub disk: DiskUuid,
    pub state: PhysicalState,
    pub observed_at: EpochMillis,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocateBlocks {
    pub count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseBlocks {
    pub blocks: Vec<AllocatedBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessCommand {
    ApplyEvents(Vec<DiskEvent>),
    Shrink(Vec<DiskUuid>),
    Allocate(AllocateBlocks),
    Release(ReleaseBlocks),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandKind {
    DiskEvents,
    Shrink,
    Allocate,
    Release,
}

impl ProcessCommand {
    pub(crate) fn kind(&self) -> CommandKind {
        match self {
            Self::ApplyEvents(_) => CommandKind::DiskEvents,
            Self::Shrink(_) => CommandKind::Shrink,
            Self::Allocate(_) => CommandKind::Allocate,
            Self::Release(_) => CommandKind::Release,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskOutcome {
    pub disk: DiskUuid,
    pub result: Result<MemberDisk, MemberDiskError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessReply {
    Disks(Vec<DiskOutcome>),
    Allocated(Vec<AllocatedBlock>),
    Released,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessQuery {
    Get(DiskUuid),
    List,
    InFlightCommands,
    Work,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessQueryReply {
    Disk(Option<MemberDisk>),
    Disks(Vec<MemberDisk>),
    Commands(Vec<CommandSnapshot>),
    Work(Vec<WorkflowSnapshot>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandStatus {
    Accepted,
    Running,
    Cancelling,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandSnapshot {
    pub id: CommandId,
    pub kind: CommandKind,
    pub status: CommandStatus,
    pub workflows: usize,
    pub finished: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkflowSnapshot {
    pub id: u64,
    pub command_id: CommandId,
    pub disk: Option<DiskUuid>,
    pub status: super::WorkflowStatus,
    pub result: Option<Result<ProcessReply, MemberDiskError>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessSnapshot {
    pub pool: String,
    pub disk_blocked: Vec<Option<DiskUuid>>,
    pub vd_blocked: Vec<Option<DiskUuid>>,
    pub commands: Vec<CommandSnapshot>,
    pub workflows: Vec<WorkflowSnapshot>,
}
