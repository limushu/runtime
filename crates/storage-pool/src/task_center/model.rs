use crate::Failure;
use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TaskId(uuid::Uuid);
impl TaskId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}
impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl FromStr for TaskId {
    type Err = Failure;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value =
            uuid::Uuid::parse_str(s).map_err(|_| Failure::Rejected("invalid task UUID".into()))?;
        if value.is_nil() {
            return Err(Failure::Rejected("nil task UUID".into()));
        }
        Ok(Self(value))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskStatus {
    Queued,
    Running,
    Paused,
    WaitingConditions,
    Ready,
    NeedsCheck,
    Completed,
    Cancelled,
    Failed,
    Interrupted,
}
impl TaskStatus {
    pub fn terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Cancelled | Self::Failed | Self::Interrupted
        )
    }
}

/// Scheduling identity survives handoff; never infer it from a workflow name.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaskClass {
    #[default]
    General,
    Shrink,
    Topology,
}
