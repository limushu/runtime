//! Bounded in-memory execution observation; never a business WAL.
use crate::audit::AuditEmitter;
use crate::*;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivityState {
    WaitingResources,
    /// Includes waiting for space in the service mailbox.
    Queued,
    Running,
    Pausing,
    Paused,
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
    entry: Arc<Mutex<ActivitySnapshot>>,
    audit: AuditEmitter,
}
impl ActivityObservation {
    pub fn new(
        history: watch::Sender<ActivityHistory>,
        limit: usize,
        id: ActivityId,
        service: String,
        name: &'static str,
        resources: Vec<ResourceAccess>,
        audit: AuditEmitter,
    ) -> Self {
        let entry = ActivitySnapshot {
            id,
            service,
            name,
            resources,
            state: ActivityState::WaitingResources,
            created_at: SystemTime::now(),
            started_at: None,
            finished_at: None,
        };
        audit.emit(AuditEvent::Activity(entry.clone()));
        history.send_modify(|history| {
            history.total += 1;
            if history.entries.len() == limit {
                history.entries.pop_front();
                history.omitted += 1;
            }
            history.entries.push_back(entry.clone());
        });
        Self {
            history,
            id,
            finished: false,
            entry: Arc::new(Mutex::new(entry)),
            audit,
        }
    }
    pub(crate) fn reporter(&self) -> ActivityReporter {
        ActivityReporter {
            history: self.history.clone(),
            id: self.id,
            entry: self.entry.clone(),
            audit: self.audit.clone(),
        }
    }
    fn update(&self, update: impl FnOnce(&mut ActivitySnapshot)) {
        let mut entry = self.entry.lock().unwrap();
        update(&mut entry);
        self.history.send_modify(|history| {
            if let Some(retained) = history.entries.iter_mut().find(|e| e.id == self.id) {
                *retained = entry.clone();
            }
        });
        self.audit.emit(AuditEvent::Activity(entry.clone()));
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

/// Shares the same entry mutex as settlement: a late pause acknowledgement can
/// never publish Paused AFTER the service has published the final business result.
pub(crate) struct ActivityReporter {
    history: watch::Sender<ActivityHistory>,
    id: ActivityId,
    entry: Arc<Mutex<ActivitySnapshot>>,
    audit: AuditEmitter,
}
impl ActivityReporter {
    pub fn state(&self, state: ActivityState) {
        let mut entry = self.entry.lock().unwrap();
        if entry.finished_at.is_some() {
            return;
        }
        entry.state = state;
        self.history.send_modify(|history| {
            if let Some(retained) = history.entries.iter_mut().find(|e| e.id == self.id) {
                *retained = entry.clone();
            }
        });
        self.audit.emit(AuditEvent::Activity(entry.clone()));
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct TaskProgress {
    pub stage: Option<String>,
    pub completed: u64,
    pub total: Option<u64>,
}
pub(crate) fn report_progress(
    sender: &watch::Sender<TaskProgress>,
    audit: &AuditEmitter,
    stage: String,
    completed: u64,
    total: Option<u64>,
) {
    if stage.trim().is_empty() || stage.len() > 256 || total.is_some_and(|t| completed > t) {
        return;
    }
    let progress = TaskProgress {
        stage: Some(stage),
        completed,
        total,
    };
    audit.emit(AuditEvent::Progress(progress.clone()));
    sender.send_replace(progress);
}
#[derive(Debug, Clone)]
pub struct ExecutionSnapshot {
    pub pool: String,
    pub id: WorkflowId,
    pub name: &'static str,
    pub state: RunState,
    pub objects: Vec<ObjectKey>,
    pub progress: TaskProgress,
    pub activities: ActivityHistory,
    pub pause_requested: bool,
    pub user_pause_requested: bool,
    pub system_pause_requested: bool,
    pub cancellation_requested: bool,
}
#[derive(Clone)]
pub(crate) struct ExecutionRecord {
    pub control: RunControl,
    pub pool: String,
}
impl ExecutionRecord {
    fn snapshot(&self) -> ExecutionSnapshot {
        ExecutionSnapshot {
            pool: self.pool.clone(),
            id: self.control.id,
            name: self.control.name,
            state: self.control.state(),
            objects: self.control.objects.lock().unwrap().clone(),
            progress: self.control.progress(),
            activities: self.control.activities(),
            pause_requested: self.control.pause_requested(),
            user_pause_requested: self.control.user_pause_requested(),
            system_pause_requested: self.control.system_pause_requested(),
            cancellation_requested: self.control.cancellation_requested(),
        }
    }
}
/// Read-only subscription survives history eviction. watch coalesces updates;
/// it is a current-state feed, not a guaranteed event journal.
#[derive(Clone)]
pub struct ExecutionObserver {
    record: ExecutionRecord,
    progress: tokio::sync::watch::Receiver<TaskProgress>,
    activities: watch::Receiver<ActivityHistory>,
    ended: bool,
}
impl ExecutionObserver {
    pub fn snapshot(&self) -> ExecutionSnapshot {
        self.record.snapshot()
    }
    pub async fn changed(&mut self) -> Outcome<ExecutionSnapshot> {
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
pub(crate) struct ExecutionRegistry {
    entries: BTreeMap<WorkflowId, ExecutionRecord>,
    completed: VecDeque<WorkflowId>,
    limit: usize,
}
impl ExecutionRegistry {
    pub fn new(limit: usize) -> Self {
        Self {
            entries: BTreeMap::new(),
            completed: VecDeque::new(),
            limit,
        }
    }
    pub fn insert(&mut self, pool: String, control: RunControl) {
        self.entries
            .insert(control.id, ExecutionRecord { control, pool });
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
    pub fn observer(&self, id: WorkflowId) -> Outcome<ExecutionObserver> {
        let record = self
            .entries
            .get(&id)
            .cloned()
            .ok_or_else(|| Failure::Rejected("unknown or expired task id".into()))?;
        Ok(ExecutionObserver {
            progress: record.control.progress.clone(),
            activities: record.control.activities.clone(),
            record,
            ended: false,
        })
    }
    pub fn snapshots(&self) -> Vec<ExecutionSnapshot> {
        self.entries
            .values()
            .map(ExecutionRecord::snapshot)
            .collect()
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
