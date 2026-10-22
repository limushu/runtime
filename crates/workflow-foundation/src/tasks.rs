//! Bounded in-memory execution observation; never a business WAL.
use crate::*;
use std::collections::{BTreeMap, VecDeque};
use std::time::SystemTime;
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivityState {
    WaitingResources,
    /// Includes waiting for space in the service mailbox.
    Queued,
    Running,
    Completed,
    Failed(Failure),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivitySnapshot {
    pub id: ActivityId,
    pub service: String,
    pub name: &'static str,
    pub resources: Vec<ResourceAccess>,
    pub state: ActivityState,
    pub created_at: SystemTime,
    /// Set only when the service actually starts execution.
    pub started_at: Option<SystemTime>,
    /// Cancellation requests alone do not set this timestamp.
    pub finished_at: Option<SystemTime>,
}

/// Actual invocations, not a static plan of future workflow branches.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ActivityHistory {
    pub total: u64,
    /// Oldest entries removed by Config::activity_history. Never silently truncated.
    pub omitted: u64,
    pub entries: VecDeque<ActivitySnapshot>,
}

/// Ownership follows the real operation into the service, even if its caller stops waiting.
pub(crate) struct ActivityObservation {
    history: watch::Sender<ActivityHistory>,
    id: ActivityId,
    finished: bool,
}
impl ActivityObservation {
    pub fn new(
        history: watch::Sender<ActivityHistory>,
        limit: usize,
        id: ActivityId,
        service: String,
        name: &'static str,
        resources: Vec<ResourceAccess>,
    ) -> Self {
        history.send_modify(|history| {
            history.total += 1;
            if history.entries.len() == limit {
                history.entries.pop_front();
                history.omitted += 1;
            }
            history.entries.push_back(ActivitySnapshot {
                id,
                service,
                name,
                resources,
                state: ActivityState::WaitingResources,
                created_at: SystemTime::now(),
                started_at: None,
                finished_at: None,
            });
        });
        Self {
            history,
            id,
            finished: false,
        }
    }
    fn update(&self, update: impl FnOnce(&mut ActivitySnapshot)) {
        self.history.send_modify(|history| {
            if let Some(entry) = history.entries.iter_mut().find(|entry| entry.id == self.id) {
                update(entry);
            }
        });
    }
    pub fn queued(&self) {
        self.update(|entry| entry.state = ActivityState::Queued);
    }
    pub fn started(&self) {
        self.update(|entry| {
            entry.started_at = Some(SystemTime::now());
            entry.state = ActivityState::Running;
        });
    }
    pub fn settled<T>(&mut self, result: &Outcome<T>) {
        self.update(|entry| {
            entry.state = match result {
                Ok(_) => ActivityState::Completed,
                Err(error) => ActivityState::Failed(error.clone()),
            };
            entry.finished_at = Some(SystemTime::now());
        });
        self.finished = true;
    }
    pub fn abandon(&mut self) {
        if self.finished {
            return;
        }
        self.update(|entry| {
            entry.state = ActivityState::Failed(if entry.started_at.is_some() {
                Failure::Uncertain("activity forcefully terminated".into())
            } else {
                Failure::Cancelled
            });
            entry.finished_at = Some(SystemTime::now());
        });
        self.finished = true;
    }
}
impl Drop for ActivityObservation {
    fn drop(&mut self) {
        self.abandon();
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TaskProgress {
    pub stage: Option<String>,
    pub completed: u64,
    pub total: Option<u64>,
}
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub pool: String,
    pub id: WorkflowId,
    pub name: &'static str,
    pub state: RunState,
    pub objects: Vec<ObjectKey>,
    pub progress: TaskProgress,
    pub activities: ActivityHistory,
    pub pause_requested: bool,
    pub cancellation_requested: bool,
}
#[derive(Clone)]
pub(crate) struct TaskRecord {
    pub control: RunControl,
    pub pool: String,
}
impl TaskRecord {
    fn snapshot(&self) -> TaskSnapshot {
        TaskSnapshot {
            pool: self.pool.clone(),
            id: self.control.id,
            name: self.control.name,
            state: self.control.state(),
            objects: self.control.objects.lock().unwrap().clone(),
            progress: self.control.progress(),
            activities: self.control.activities(),
            pause_requested: self.control.pause_requested(),
            cancellation_requested: self.control.cancellation_requested(),
        }
    }
}
/// Read-only subscription survives history eviction. watch coalesces updates;
/// it is a current-state feed, not a guaranteed event journal.
pub struct TaskObserver {
    record: TaskRecord,
    progress: tokio::sync::watch::Receiver<TaskProgress>,
    activities: watch::Receiver<ActivityHistory>,
    ended: bool,
}
impl TaskObserver {
    pub fn snapshot(&self) -> TaskSnapshot {
        self.record.snapshot()
    }
    pub async fn changed(&mut self) -> Outcome<TaskSnapshot> {
        if self.ended {
            return Err(Failure::Unavailable);
        }
        let changed = tokio::select! {
            changed = self.record.control.changed() => changed.map(|_| ()),
            changed = self.progress.changed() => changed.map_err(|_| Failure::Unavailable),
            changed = self.activities.changed() => changed.map_err(|_| Failure::Unavailable),
        };
        let snapshot = self.snapshot();
        self.ended = matches!(snapshot.state, RunState::Completed | RunState::Failed(_));
        if let Err(error) = changed
            && !self.ended
        {
            return Err(error);
        }
        Ok(snapshot)
    }
}
pub(crate) struct TaskRegistry {
    entries: BTreeMap<WorkflowId, TaskRecord>,
    completed: VecDeque<WorkflowId>,
    limit: usize,
}
impl TaskRegistry {
    pub fn new(limit: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            completed: VecDeque::new(),
            limit,
        }
    }
    pub fn insert(&mut self, pool: String, control: RunControl) {
        self.entries
            .insert(control.id, TaskRecord { control, pool });
    }
    pub fn finish(&mut self, id: WorkflowId) {
        if self.entries.get(&id).is_some_and(|record| {
            matches!(
                record.control.state(),
                RunState::Failed(Failure::Uncertain(_))
            )
        }) {
            return;
        }
        self.completed.push_back(id);
        while self.completed.len() > self.limit {
            if let Some(id) = self.completed.pop_front() {
                self.entries.remove(&id);
            }
        }
    }
    pub fn observer(&self, id: WorkflowId) -> Outcome<TaskObserver> {
        let record = self
            .entries
            .get(&id)
            .cloned()
            .ok_or_else(|| Failure::Rejected("unknown or expired task id".into()))?;
        Ok(TaskObserver {
            progress: record.control.progress.clone(),
            activities: record.control.activities.clone(),
            record,
            ended: false,
        })
    }
    pub fn snapshots(&self) -> Vec<TaskSnapshot> {
        self.entries.values().map(TaskRecord::snapshot).collect()
    }
    pub fn ids(&self) -> Vec<WorkflowId> {
        self.entries.keys().copied().collect()
    }
    pub fn control(&self, id: WorkflowId) -> Outcome<RunControl> {
        let control = self.observer(id)?.record.control;
        if matches!(control.state(), RunState::Completed | RunState::Failed(_)) {
            return Err(Failure::Rejected("task is terminal".into()));
        }
        Ok(control)
    }
}
