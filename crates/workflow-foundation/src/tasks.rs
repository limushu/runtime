//! Bounded in-memory execution observation; never a business WAL.
use crate::*;
use std::collections::{BTreeMap, VecDeque};
use std::time::SystemTime;
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivityState {
    WaitingConditions(String),
    WaitingTimer,
    Suspending,
    Suspended,
    /// An abandoned/failed step is still awaiting actual child operations.
    Settling(Failure),
    WaitingResources,
    /// Includes waiting for space in the service mailbox.
    Queued,
    Running,
    Completed,
    Failed(Failure),
}
impl ActivityState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed(_))
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActivityKind {
    Step,
    Call,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActivitySnapshot {
    pub id: ActivityId,
    pub kind: ActivityKind,
    pub parent_id: Option<ActivityId>,
    pub service: String,
    pub name: &'static str,
    pub description: Option<String>,
    /// execute entries, including continuation after a stopping-return pause.
    pub attempts: u32,
    /// Approved Retry decisions; suspension does not consume this budget.
    pub retries: u32,
    pub last_error: Option<Failure>,
    pub retry_reason: Option<String>,
    pub retry_at: Option<SystemTime>,
    pub progress: TaskProgress,
    pub resources: Vec<ResourceAccess>,
    pub state: ActivityState,
    pub created_at: SystemTime,
    /// First execute entry for a step, actual service start for a call.
    pub started_at: Option<SystemTime>,
    /// Cancellation requests alone do not set this timestamp.
    pub finished_at: Option<SystemTime>,
}

/// Started logical steps and actual calls, not a static plan of future branches.
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
    name: &'static str,
    limit: usize,
    logical: bool,
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
        parent_id: Option<ActivityId>,
    ) -> Self {
        history.send_modify(|history| {
            history.total += 1;
            history.entries.push_back(ActivitySnapshot {
                id,
                kind: ActivityKind::Call,
                parent_id,
                service,
                name,
                description: None,
                attempts: 0,
                retries: 0,
                last_error: None,
                retry_reason: None,
                retry_at: None,
                progress: TaskProgress::default(),
                resources,
                state: ActivityState::WaitingResources,
                created_at: SystemTime::now(),
                started_at: None,
                finished_at: None,
            });
            retain_history(history, limit);
        });
        Self {
            history,
            id,
            name,
            limit,
            logical: false,
            finished: false,
        }
    }
    pub fn logical(
        history: watch::Sender<ActivityHistory>,
        limit: usize,
        id: ActivityId,
        name: &'static str,
        description: Option<String>,
    ) -> Self {
        let mut observation = Self::new(history, limit, id, "workflow".into(), name, vec![], None);
        observation.logical = true;
        observation.update(|entry| {
            entry.kind = ActivityKind::Step;
            entry.description = description;
        });
        observation
    }
    pub fn id(&self) -> ActivityId {
        self.id
    }
    pub fn name(&self) -> &'static str {
        self.name
    }
    pub fn state(&self, state: ActivityState) {
        self.update(|entry| entry.state = state);
    }
    pub fn update(&self, update: impl FnOnce(&mut ActivitySnapshot)) {
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
            entry.started_at.get_or_insert_with(SystemTime::now);
            entry.attempts = entry.attempts.saturating_add(1);
            entry.state = ActivityState::Running;
        });
    }
    pub fn settled<T>(&mut self, result: &Outcome<T>) {
        self.history.send_modify(|history| {
            let pending = history
                .entries
                .iter()
                .any(|entry| entry.parent_id == Some(self.id) && !entry.state.is_terminal());
            if let Some(entry) = history.entries.iter_mut().find(|entry| entry.id == self.id) {
                entry.state = match (pending, result) {
                    (true, Err(error)) => ActivityState::Settling(error.clone()),
                    (true, Ok(_)) => ActivityState::Settling(Failure::Uncertain(
                        "unconsumed service call".into(),
                    )),
                    (false, Ok(_)) => ActivityState::Completed,
                    (false, Err(error)) => ActivityState::Failed(error.clone()),
                };
                if !pending {
                    entry.finished_at = Some(SystemTime::now());
                }
                if let Err(error) = result {
                    entry.last_error = Some(error.clone());
                }
                entry.retry_at = None;
                if let (Some(parent), Err(error)) = (entry.parent_id, result)
                    && error.uncertain()
                    && let Some(parent) =
                        history.entries.iter_mut().find(|entry| entry.id == parent)
                {
                    parent.last_error = Some(error.clone());
                    if matches!(parent.state, ActivityState::Settling(_)) {
                        parent.state = ActivityState::Settling(error.clone());
                    }
                }
            }
            finish_settling(history);
            retain_history(history, self.limit);
        });
        self.finished = true;
    }
    pub fn abandon(&mut self) {
        if self.finished {
            return;
        }
        let history = self.history.borrow();
        let entry = history.entries.iter().find(|entry| entry.id == self.id);
        let started = entry.is_some_and(|entry| entry.started_at.is_some());
        let uncertain = entry
            .and_then(|entry| entry.last_error.clone())
            .filter(Failure::uncertain);
        drop(history);
        let error = if let Some(error) = uncertain {
            error
        } else if started && !self.logical {
            Failure::Uncertain("activity forcefully terminated".into())
        } else {
            Failure::Cancelled
        };
        self.settled(&Err::<(), _>(error));
    }
}

fn finish_settling(history: &mut ActivityHistory) {
    let ready: Vec<_> = history
        .entries
        .iter()
        .filter_map(|entry| {
            if let ActivityState::Settling(error) = &entry.state
                && !history
                    .entries
                    .iter()
                    .any(|child| child.parent_id == Some(entry.id) && !child.state.is_terminal())
            {
                Some((entry.id, error.clone()))
            } else {
                None
            }
        })
        .collect();
    for (id, error) in ready {
        if let Some(entry) = history.entries.iter_mut().find(|entry| entry.id == id) {
            entry.state = ActivityState::Failed(error.clone());
            entry.last_error = Some(error);
            entry.finished_at = Some(SystemTime::now());
        }
    }
}
// Keep active parents/calls even at a very small limit. Trim completed calls
// first so a logical step remains inspectable after its last child completes.
fn retain_history(history: &mut ActivityHistory, limit: usize) {
    while history.entries.len() > limit {
        let index = history
            .entries
            .iter()
            .position(|entry| entry.parent_id.is_some() && entry.state.is_terminal())
            .or_else(|| {
                history
                    .entries
                    .iter()
                    .position(|entry| entry.state.is_terminal())
            });
        let Some(index) = index else {
            break;
        };
        history.entries.remove(index);
        history.omitted += 1;
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
    pub id: TaskId,
    pub name: &'static str,
    pub state: RunState,
    pub objects: Vec<ObjectKey>,
    pub progress: TaskProgress,
    pub activities: ActivityHistory,
    pub suspend_requested: bool,
    pub suspend_acknowledged: bool,
    pub suspend_reasons: std::collections::BTreeSet<String>,
    pub cancellation_requested: bool,
    pub details: serde_json::Value,
    pub conflicts: Vec<ConflictMatch>,
    pub blockers: Vec<TaskId>,
}
impl TaskSnapshot {
    pub fn details_as<T: serde::de::DeserializeOwned>(&self) -> Outcome<T> {
        serde_json::from_value(self.details.clone()).map_err(|e| Failure::Rejected(e.to_string()))
    }
}
#[derive(Clone)]
pub(crate) struct TaskRecord {
    pub control: RunControl,
    pub pool: String,
}
impl TaskRecord {
    fn snapshot(&self) -> TaskSnapshot {
        let info = self.control.info.borrow().clone();
        TaskSnapshot {
            pool: self.pool.clone(),
            id: self.control.id,
            name: self.control.name,
            state: self.control.state(),
            // Observation can still expose the last value of a poisoned lock.
            objects: match self.control.objects.lock() {
                Ok(objects) => objects.clone(),
                Err(poisoned) => poisoned.into_inner().clone(),
            },
            progress: self.control.progress(),
            activities: self.control.activities(),
            suspend_requested: self.control.suspend_requested(),
            suspend_acknowledged: self.control.suspend_acknowledged(),
            suspend_reasons: self.control.suspend_reasons(),
            cancellation_requested: self.control.cancellation_requested(),
            details: info.details,
            conflicts: info.conflicts,
            blockers: info.blockers,
        }
    }
}
/// Read-only subscription survives history eviction. watch coalesces updates;
/// it is a current-state feed, not a guaranteed event journal.
pub struct TaskObserver {
    record: TaskRecord,
    progress: tokio::sync::watch::Receiver<TaskProgress>,
    activities: watch::Receiver<ActivityHistory>,
    info: watch::Receiver<TaskInfo>,
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
            changed = self.info.changed() => changed.map_err(|_| Failure::Unavailable),
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
    entries: BTreeMap<TaskId, TaskRecord>,
    completed: VecDeque<TaskId>,
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
    pub fn finish(&mut self, id: TaskId) {
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
    pub fn observer(&self, id: TaskId) -> Outcome<TaskObserver> {
        let record = self
            .entries
            .get(&id)
            .cloned()
            .ok_or_else(|| Failure::Rejected("unknown or expired task id".into()))?;
        Ok(TaskObserver {
            progress: record.control.progress.clone(),
            activities: record.control.activities.clone(),
            record: record.clone(),
            ended: false,
            info: record.control.info.subscribe(),
        })
    }
    pub fn snapshots(&self) -> Vec<TaskSnapshot> {
        self.entries.values().map(TaskRecord::snapshot).collect()
    }
    pub fn ids(&self) -> Vec<TaskId> {
        self.entries.keys().copied().collect()
    }
    pub fn control(&self, id: TaskId) -> Outcome<RunControl> {
        let control = self.observer(id)?.record.control;
        if matches!(control.state(), RunState::Completed | RunState::Failed(_)) {
            return Err(Failure::Rejected("task is terminal".into()));
        }
        Ok(control)
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TaskInfo {
    pub details: serde_json::Value,
    pub conflicts: Vec<ConflictMatch>,
    pub blockers: Vec<TaskId>,
}
