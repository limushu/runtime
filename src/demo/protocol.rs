use std::{fmt, sync::Arc};

use crate::{Reply, TaskOutcome, define_messages};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ServiceKind {
    Event,
    Rebuild,
    Bg,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DiskId(pub Arc<str>);

impl DiskId {
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for DiskId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct BgId(pub Arc<str>);

impl BgId {
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for BgId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DemoError {
    AlreadyRebuilding(DiskId),
    NotRebuilding(DiskId),
    Cancelled,
    Runtime(String),
}

impl fmt::Display for DemoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for DemoError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RebuildStatus {
    Completed,
}

pub type RebuildResult = Result<RebuildStatus, DemoError>;

#[derive(Debug)]
pub struct DiskOfflineRequest {
    pub disk: DiskId,
    pub completed: Reply<RebuildResult>,
}

#[derive(Debug)]
pub struct OfflineDispatched {
    pub disk: DiskId,
    pub completed: Reply<RebuildResult>,
    pub outcome: TaskOutcome<RebuildResult>,
}

#[derive(Debug)]
pub struct StartRebuildRequest {
    pub disk: DiskId,
    pub completed: Reply<RebuildResult>,
}

#[derive(Debug)]
pub struct SuspendRebuildRequest {
    pub completed: Reply<Result<(), DemoError>>,
}

#[derive(Debug)]
pub struct ResumeRebuildRequest {
    pub completed: Reply<Result<(), DemoError>>,
}

#[derive(Debug)]
pub struct CancelRebuildRequest {
    pub disk: DiskId,
    pub completed: Reply<Result<(), DemoError>>,
}

#[derive(Debug)]
pub struct DiskResolved {
    pub disk: DiskId,
    pub outcome: TaskOutcome<Vec<BgId>>,
}

#[derive(Debug)]
pub struct BgFinished {
    pub bg: BgId,
    pub outcome: TaskOutcome<()>,
}

#[derive(Debug)]
pub struct CampaignFinished {
    pub outcome: TaskOutcome<()>,
}

#[derive(Debug)]
pub struct CancelBgSent {
    pub bg: BgId,
    pub outcome: TaskOutcome<()>,
}

#[derive(Debug)]
pub struct DoBgRebuild {
    pub bg: BgId,
    pub completed: Reply<Result<(), DemoError>>,
}

#[derive(Debug)]
pub struct CancelBgRebuild {
    pub bg: BgId,
}

#[derive(Debug)]
pub struct BgWorkflowFinished {
    pub bg: BgId,
    pub completed: Reply<Result<(), DemoError>>,
    pub outcome: TaskOutcome<()>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RebuildSnapshot {
    pub suspended: bool,
    pub resolving_disks: usize,
    pub active_disks: usize,
    pub queued_bgs: usize,
    pub running_bgs: usize,
    pub campaigns_started: usize,
    pub bg_delegations_started: usize,
}

#[derive(Debug)]
pub struct QueryRebuild {
    pub completed: Reply<RebuildSnapshot>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BgSnapshot {
    pub started: Vec<BgId>,
    pub completed: Vec<BgId>,
    pub cancelled: Vec<BgId>,
}

#[derive(Debug)]
pub struct QueryBg {
    pub completed: Reply<BgSnapshot>,
}

define_messages! {
    pub enum DemoMessage => DemoMessageKind {
        DiskOffline(DiskOfflineRequest),
        OfflineDispatched(OfflineDispatched),
        StartRebuild(StartRebuildRequest),
        SuspendRebuild(SuspendRebuildRequest),
        ResumeRebuild(ResumeRebuildRequest),
        CancelRebuild(CancelRebuildRequest),
        DiskResolved(DiskResolved),
        BgFinished(BgFinished),
        CampaignFinished(CampaignFinished),
        CancelBgSent(CancelBgSent),
        DoBgRebuild(DoBgRebuild),
        CancelBgRebuild(CancelBgRebuild),
        BgWorkflowFinished(BgWorkflowFinished),
        QueryRebuild(QueryRebuild),
        QueryBg(QueryBg)
    }
}
