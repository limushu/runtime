use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Failure {
    Cancelled,
    Rejected(String),
    Failed(String),
    /// Effects may have occurred; never retry or release affected objects blindly.
    Uncertain(String),
    Unavailable,
    Busy,
}
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Failure {}
impl Failure {
    pub(crate) fn uncertain(&self) -> bool {
        matches!(self, Self::Uncertain(_))
    }
}
pub type Outcome<T> = Result<T, Failure>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActivityId {
    pub task_id: crate::TaskId,
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
pub struct ObjectKey {
    pub namespace: String,
    pub id: String,
}
impl ObjectKey {
    pub fn new(namespace: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            id: id.into(),
        }
    }
    pub(crate) fn validate(&self) -> Outcome<()> {
        if self.namespace.trim().is_empty() || self.id.trim().is_empty() {
            Err(Failure::Rejected(
                "object namespace and id must be nonempty".into(),
            ))
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Read,
    Write,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceAccess {
    pub object: ObjectKey,
    pub access: Access,
}
impl ResourceAccess {
    pub fn read(object: ObjectKey) -> Self {
        Self {
            object,
            access: Access::Read,
        }
    }
    pub fn write(object: ObjectKey) -> Self {
        Self {
            object,
            access: Access::Write,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Signal {
    pub name: String,
    pub payload: String,
}
impl Signal {
    pub fn new(name: impl Into<String>, payload: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            payload: payload.into(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunState {
    WaitingObjects,
    Running,
    /// Acknowledged safe pause, including an in-flight operation that has stopped.
    Suspended,
    Suspending,
    WaitingConditions(String),
    WaitingResources,
    WaitingSignal,
    WaitingTimer,
    Activity(ActivityId),
    Settling,
    Completed,
    Failed(Failure),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    Running,
    Stopping,
    Stopped,
}
#[derive(Debug, Clone)]
pub struct RuntimeSnapshot {
    pub pool: String,
    pub lifecycle: Lifecycle,
    pub workflows: usize,
    pub waiting: usize,
    pub held: usize,
    pub fenced: usize,
}
#[derive(Debug, Clone)]
pub struct ServiceSnapshot {
    pub lifecycle: Lifecycle,
    pub running: usize,
    pub completed: u64,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub capacity: usize,
    pub max_workflows: usize,
    pub max_signal_bytes: usize,
    /// Ordinary completed task records. Uncertain records are retained separately.
    pub task_history: usize,
    /// Most recent Activity invocations retained per task (including its current Activity).
    pub activity_history: usize,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            capacity: 64,
            max_workflows: 256,
            max_signal_bytes: 64 * 1024,
            task_history: 256,
            activity_history: 1024,
        }
    }
}
impl Config {
    pub(crate) fn validate(&self) -> Outcome<()> {
        if self.capacity == 0
            || self.max_workflows == 0
            || self.max_signal_bytes == 0
            || self.activity_history == 0
        {
            Err(Failure::Rejected("limits must be nonzero".into()))
        } else {
            Ok(())
        }
    }
}

pub(crate) struct Scope {
    pub closed: AtomicBool,
}
impl Scope {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            closed: AtomicBool::new(false),
        })
    }
    pub fn close(&self) {
        self.closed.store(true, Ordering::SeqCst);
    }
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}
