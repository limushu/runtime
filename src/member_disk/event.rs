use super::DiskUuid;

/// Wall-clock time attached to a DiskMap observation. It belongs to the event,
/// not to persisted MemberDisk metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EpochMillis(u64);

impl EpochMillis {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Physical fact reported by DiskMap. MemberDisk does not persist a copy of
/// this state; the event drives its effective IO state toward convergence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalState {
    Up,
    Down,
}

/// Every external input accepted by the MemberDisk service.
///
/// DiskMap reports physical facts with `PhysicalChanged`; the management plane
/// requests planned removal with `Shrink`. Both enter the same event path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskEvent {
    PhysicalChanged {
        disk: DiskUuid,
        state: PhysicalState,
        observed_at: EpochMillis,
    },
    Shrink {
        disk: DiskUuid,
    },
}

impl MemberDiskEvent {
    pub fn disk(&self) -> &DiskUuid {
        match self {
            Self::PhysicalChanged { disk, .. } | Self::Shrink { disk } => disk,
        }
    }

    pub(crate) fn same_kind(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (
                Self::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                },
                Self::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                }
            ) | (
                Self::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                },
                Self::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                }
            ) | (Self::Shrink { .. }, Self::Shrink { .. })
        )
    }
}
