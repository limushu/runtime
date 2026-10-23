pub use crate::task_center::{TaskClass, TaskId, TaskStatus};
use crate::{Failure, PoolInstanceId, PoolUuid};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};
use workflow_foundation::{
    ActivitySnapshot, ActivityState, ObjectKey, ResourceAccess, RunState, TaskProgress, WorkflowId,
};

/// Opaque business encoding. kind/version select an explicitly registered recovery function.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeSpec {
    pub kind: String,
    pub version: u32,
    pub input: Vec<u8>,
    pub checkpoint: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordedActivityState {
    WaitingResources,
    Queued,
    Running,
    Pausing,
    Paused,
    Completed,
    Failed(Failure),
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordedActivity {
    pub sequence: u64,
    pub name: String,
    pub service: String,
    pub resources: Vec<ResourceAccess>,
    pub state: RecordedActivityState,
    pub created_at: SystemTime,
    pub started_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
}
impl From<ActivitySnapshot> for RecordedActivity {
    fn from(a: ActivitySnapshot) -> Self {
        let state = match a.state {
            ActivityState::WaitingResources => RecordedActivityState::WaitingResources,
            ActivityState::Queued => RecordedActivityState::Queued,
            ActivityState::Running => RecordedActivityState::Running,
            ActivityState::Pausing => RecordedActivityState::Pausing,
            ActivityState::Paused => RecordedActivityState::Paused,
            ActivityState::Completed => RecordedActivityState::Completed,
            ActivityState::Failed(e) => RecordedActivityState::Failed(e),
        };
        Self {
            sequence: a.id.sequence,
            name: a.name.into(),
            service: a.service,
            resources: a.resources,
            state,
            created_at: a.created_at,
            started_at: a.started_at,
            finished_at: a.finished_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredExecution {
    pub attempt: u64,
    pub instance: PoolInstanceId,
    pub workflow: Option<WorkflowId>,
    pub state: RunState,
    pub progress: TaskProgress,
    pub activities: BTreeMap<u64, RecordedActivity>,
    pub created_at: SystemTime,
    pub started_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
    /// The previous host is gone; its last RunState is a historical observation only.
    pub interrupted: bool,
}
impl StoredExecution {
    pub(crate) fn new(attempt: u64, instance: PoolInstanceId) -> Self {
        Self {
            attempt,
            instance,
            workflow: None,
            state: RunState::WaitingObjects,
            progress: TaskProgress::default(),
            activities: BTreeMap::new(),
            created_at: SystemTime::now(),
            started_at: None,
            finished_at: None,
            interrupted: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredTask {
    pub format_version: u32,
    pub id: TaskId,
    pub pool: PoolUuid,
    pub name: String,
    #[serde(default)]
    pub class: TaskClass,
    pub objects: Vec<ObjectKey>,
    #[serde(default)]
    pub source_event: Option<uuid::Uuid>,
    pub status: TaskStatus,
    pub revision: u64,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
    pub resume: Option<ResumeSpec>,
    pub pause_requested: bool,
    #[serde(default)]
    pub system_pause_requested: bool,
    pub cancellation_requested: bool,
    pub last_error: Option<Failure>,
    pub executions: Vec<StoredExecution>,
}
impl StoredTask {
    pub fn recoverable(&self) -> bool {
        self.resume.is_some() && !self.status.terminal()
    }
    pub(crate) fn index_key(&self) -> String {
        let nanos = self
            .created_at
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        format!("{}/{nanos:039}/{}", self.pool, self.id)
    }
}

#[derive(Debug, Clone)]
pub struct TaskQuery {
    /// Opaque cursor returned by the previous page; newest tasks first.
    pub before: Option<String>,
    pub limit: usize,
    pub recoverable_only: bool,
}
impl Default for TaskQuery {
    fn default() -> Self {
        Self {
            before: None,
            limit: 50,
            recoverable_only: false,
        }
    }
}
#[derive(Debug, Clone)]
pub struct TaskPage {
    pub tasks: Vec<StoredTask>,
    pub next: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PersistenceConfig {
    pub queue_capacity: usize,
    pub max_resume_bytes: usize,
}
impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            max_resume_bytes: 1024 * 1024,
        }
    }
}
#[derive(Debug, Clone, Default)]
pub struct PersistenceHealth {
    pub failed_records: u64,
    pub last_error: Option<String>,
}
