use std::time::Duration;

use super::{EpochMillis, MemberDisk, MemberDiskState};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DiskIntentKind {
    Offline,
    Online,
    Shrink,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DiskIntent {
    Offline { observed_at: EpochMillis },
    Online,
    Shrink,
}

impl DiskIntent {
    pub fn kind(&self) -> DiskIntentKind {
        match self {
            Self::Offline { .. } => DiskIntentKind::Offline,
            Self::Online => DiskIntentKind::Online,
            Self::Shrink => DiskIntentKind::Shrink,
        }
    }
}

/// One concrete operation that the service can turn into a Future.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) enum Action {
    CloseIo,
    WaitRecovery,
    DisableAllocation,
    Evacuate,
    BringOnline,
    Allocate,
    Release,
}

impl Action {
    pub fn name(self) -> &'static str {
        match self {
            Self::CloseIo => "close-io",
            Self::WaitRecovery => "wait-recovery",
            Self::DisableAllocation => "disable-allocation",
            Self::Evacuate => "evacuate",
            Self::BringOnline => "bring-online",
            Self::Allocate => "allocate",
            Self::Release => "release",
        }
    }
}

pub(crate) enum Decision {
    Run(Action),
    Done,
}

/// Pure MemberDisk state machine: committed state + intent -> next action.
pub(crate) fn decide(
    member: &MemberDisk,
    intent: &DiskIntent,
    now: EpochMillis,
    recovery_window: Duration,
) -> Decision {
    match intent {
        DiskIntent::Offline { observed_at } => {
            offline(member.state(), *observed_at, now, recovery_window)
        }
        DiskIntent::Online => online(member.state()),
        DiskIntent::Shrink => shrink(member.state()),
    }
}

fn offline(
    state: MemberDiskState,
    observed_at: EpochMillis,
    now: EpochMillis,
    recovery_window: Duration,
) -> Decision {
    use Action::*;
    use Decision::*;
    use MemberDiskState::*;

    match state {
        UpActive | UpInactive => Run(CloseIo),
        DownActive if before_recovery_deadline(observed_at, now, recovery_window) => {
            Run(WaitRecovery)
        }
        DownActive => Run(DisableAllocation),
        DownInactive => Run(Evacuate),
        Removed => Done,
    }
}

fn online(state: MemberDiskState) -> Decision {
    use Action::BringOnline;
    use Decision::*;
    use MemberDiskState::*;

    match state {
        UpActive => Done,
        UpInactive | DownActive | DownInactive | Removed => Run(BringOnline),
    }
}

fn shrink(state: MemberDiskState) -> Decision {
    use Action::{DisableAllocation, Evacuate};
    use Decision::*;
    use MemberDiskState::*;

    match state {
        UpActive | DownActive => Run(DisableAllocation),
        UpInactive | DownInactive => Run(Evacuate),
        Removed => Done,
    }
}

fn before_recovery_deadline(
    observed_at: EpochMillis,
    now: EpochMillis,
    recovery_window: Duration,
) -> bool {
    now < observed_at.saturating_add(recovery_window.as_millis() as u64)
}
