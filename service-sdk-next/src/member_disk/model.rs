use super::MemberDiskServiceError;
use std::fmt;

pub type EpochMillis = u64;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiskUuid(String);

impl DiskUuid {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for DiskUuid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

/// Construction data. The live MemberDisk object remains private to its service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskSeed {
    pub uuid: DiskUuid,
    pub pool_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskIoState {
    Up,
    Down,
}

/// One MemberDisk-domain fact carried by a PoolNode broadcast packet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskStateChange {
    pub disk: DiskUuid,
    pub state: DiskIoState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskState {
    UpActive,
    UpInactive,
    DownActive,
    DownInactive,
    Removed,
}

/// The only durable field changes emitted by the MemberDisk domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskMutation {
    SetIo(DiskIoState),
    RequestShrink,
    DisableAllocation,
    CompleteOnline,
    Remove,
    Rejoin,
}

/// One typed SDB update. The metadata port can commit several independent
/// MemberDisk updates in one request while preserving SDB-first publication.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskCommit {
    pub disk: DiskUuid,
    pub mutation: MemberDiskMutation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskCommand {
    DiskDown {
        disks: Vec<DiskUuid>,
        observed_at: EpochMillis,
    },
    DiskUp {
        disks: Vec<DiskUuid>,
    },
    Shrink {
        disks: Vec<DiskUuid>,
    },
}

impl MemberDiskCommand {
    pub fn disks(&self) -> &[DiskUuid] {
        match self {
            Self::DiskDown { disks, .. } | Self::DiskUp { disks } | Self::Shrink { disks } => disks,
        }
    }
}

#[derive(Debug)]
pub enum MemberDiskQuery {
    Get(DiskUuid),
    List,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskReply {
    pub outcomes: Vec<MemberDiskOutcome>,
}

impl MemberDiskReply {
    pub fn outcome(&self, disk: &DiskUuid) -> Option<&MemberDiskOutcome> {
        self.outcomes.iter().find(|outcome| &outcome.disk == disk)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskOutcome {
    pub disk: DiskUuid,
    pub result: Result<MemberDiskState, MemberDiskServiceError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskQueryReply {
    One(MemberDiskView),
    List(Vec<MemberDiskView>),
}

/// Read-only DTO returned by Query. It is not the live domain object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskView {
    pub uuid: DiskUuid,
    pub pool_id: String,
    pub state: MemberDiskState,
    pub shrink_requested: bool,
}

#[derive(Debug, Clone)]
pub(super) struct MemberDisk {
    uuid: DiskUuid,
    pool_id: String,
    io: DiskIoState,
    allocation_active: bool,
    removed: bool,
    shrink_requested: bool,
}

impl MemberDisk {
    pub(super) fn from_seed(seed: MemberDiskSeed) -> Self {
        Self {
            uuid: seed.uuid,
            pool_id: seed.pool_id,
            io: DiskIoState::Up,
            allocation_active: true,
            removed: false,
            shrink_requested: false,
        }
    }

    pub(super) fn state(&self) -> MemberDiskState {
        if self.removed {
            return MemberDiskState::Removed;
        }
        match (self.io, self.allocation_active) {
            (DiskIoState::Up, true) => MemberDiskState::UpActive,
            (DiskIoState::Up, false) => MemberDiskState::UpInactive,
            (DiskIoState::Down, true) => MemberDiskState::DownActive,
            (DiskIoState::Down, false) => MemberDiskState::DownInactive,
        }
    }

    pub(super) fn shrinking(&self) -> bool {
        self.shrink_requested
    }

    pub(super) fn view(&self) -> MemberDiskView {
        MemberDiskView {
            uuid: self.uuid.clone(),
            pool_id: self.pool_id.clone(),
            state: self.state(),
            shrink_requested: self.shrink_requested,
        }
    }

    pub(super) fn validate(
        &self,
        mutation: &MemberDiskMutation,
    ) -> Result<bool, MemberDiskServiceError> {
        use MemberDiskMutation::*;
        match mutation {
            Rejoin => Ok(self.removed),
            RequestShrink if self.removed => Ok(false),
            _ if self.removed => Err(MemberDiskServiceError::InvalidState(format!(
                "MemberDisk {} has been removed",
                self.uuid
            ))),
            SetIo(io) => Ok(self.io != *io),
            RequestShrink => Ok(!self.shrink_requested),
            DisableAllocation => Ok(self.allocation_active),
            CompleteOnline => Ok(self.io != DiskIoState::Up || !self.allocation_active),
            Remove if self.allocation_active => Err(MemberDiskServiceError::InvalidState(
                "allocation must be disabled before removal".into(),
            )),
            Remove => Ok(true),
        }
    }

    pub(super) fn apply_committed(&mut self, mutation: &MemberDiskMutation) {
        use MemberDiskMutation::*;
        match mutation {
            SetIo(io) => self.io = *io,
            RequestShrink => self.shrink_requested = true,
            DisableAllocation => self.allocation_active = false,
            CompleteOnline => {
                self.io = DiskIoState::Up;
                self.allocation_active = true;
            }
            Remove => {
                self.io = DiskIoState::Down;
                self.removed = true;
            }
            Rejoin => {
                self.io = DiskIoState::Up;
                self.allocation_active = true;
                self.removed = false;
                self.shrink_requested = false;
            }
        }
    }
}
