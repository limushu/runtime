use crate::DiskId;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct EvacuationId(pub uuid::Uuid);
impl Default for EvacuationId {
    fn default() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}
impl std::fmt::Display for EvacuationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShrinkReadiness {
    Ready,
    Wait(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EvacuationState {
    Running,
    Paused,
    Completed,
    Cancelled,
    Failed(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvacuationProgress {
    pub disk: DiskId,
    pub completed: u64,
    pub total: Option<u64>,
    pub state: EvacuationState,
}
