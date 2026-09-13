/// Runtime state shared by every service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    Running,
    Paused,
    Draining,
    Stopping,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Activity {
    Idle,
    Busy,
}

impl Lifecycle {
    pub(crate) fn accepts_commands(self) -> bool {
        self == Self::Running
    }

    pub(crate) fn schedules_work(self) -> bool {
        matches!(self, Self::Running | Self::Draining)
    }
}

/// Why a runtime rejected a new command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceUnavailable {
    Paused,
    Draining,
    Stopping,
    Stopped,
}

impl From<Lifecycle> for ServiceUnavailable {
    fn from(value: Lifecycle) -> Self {
        match value {
            Lifecycle::Paused => Self::Paused,
            Lifecycle::Draining => Self::Draining,
            Lifecycle::Stopping => Self::Stopping,
            Lifecycle::Stopped => Self::Stopped,
            Lifecycle::Running => unreachable!("a running service accepts commands"),
        }
    }
}
