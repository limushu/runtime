//! Storage representation of the same Task, not another task manager.
use crate::{Failure, ObjectKey, Outcome, RunState, TaskId, TaskProgress};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::SystemTime,
};

#[derive(Debug, Clone, Default, PartialEq)]
pub enum Persistence {
    /// Keep a read-only history record when a store and history recording are enabled.
    #[default]
    History,
    /// Explicit opt-out, even on a host configured to record ordinary tasks.
    Memory,
    Durable {
        version: u32,
        input: Value,
    },
}
impl Persistence {
    pub fn durable<T: Serialize>(version: u32, input: &T) -> Outcome<Self> {
        if version == 0 {
            return Err(Failure::Rejected(
                "workflow schema version must be nonzero".into(),
            ));
        }
        Ok(Self::Durable {
            version,
            input: serde_json::to_value(input).map_err(|e| Failure::Rejected(e.to_string()))?,
        })
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum StoredOutcome {
    Pending,
    Completed,
    Cancelled,
    Failed(String),
    NeedsCheck(String),
    /// Execution was interrupted; no terminal business result was confirmed.
    Interrupted,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskObservation {
    pub created_at: Option<SystemTime>,
    pub started_at: Option<SystemTime>,
    pub finished_at: Option<SystemTime>,
    pub updated_at: Option<SystemTime>,
    /// Last confirmed observation, not proof of the current remote state.
    pub state: Option<RunState>,
    pub progress: TaskProgress,
}
fn legacy_recoverable() -> bool {
    true
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedTask {
    pub id: TaskId,
    /// History-only records are never passed to Workflow::restore.
    #[serde(default = "legacy_recoverable")]
    pub recoverable: bool,
    #[serde(default)]
    pub observation: TaskObservation,
    /// These three fields are continuation data only when recoverable is true.
    pub version: u32,
    pub input: Value,
    pub checkpoint: Value,
    pub objects: Vec<ObjectKey>,
    pub metadata: Value,
    pub details: Value,
    pub revision: u64,
    pub owner_epoch: u64,
    pub user_paused: bool,
    pub cancel_requested: bool,
    pub outcome: StoredOutcome,
}
/// Implementations MUST atomically enforce epoch fencing and compare-and-swap.
/// Acknowledged writes must survive the backend's advertised failure model.
pub trait TaskStore: Send + Sync + 'static {
    fn claim(&self, pool: u128, epoch: u64) -> Outcome<()>;
    fn list(&self, pool: u128) -> Outcome<Vec<SavedTask>>;
    fn save(&self, task: SavedTask, previous: Option<u64>) -> Outcome<()>;
    /// Override with a point read in a KV adapter. Defaults preserve existing adapters.
    fn get(&self, id: TaskId) -> Outcome<Option<SavedTask>> {
        Ok(self.list(id.pool)?.into_iter().find(|task| task.id == id))
    }
    /// Descending creation order, exclusive cursor, scoped to one stable Pool UUID.
    /// KV adapters should override with an indexed scan rather than loading all history.
    fn query(&self, pool: u128, query: &TaskQuery) -> Outcome<TaskPage> {
        query.validate(pool)?;
        let mut tasks: Vec<_> = self
            .list(pool)?
            .into_iter()
            .filter(|task| query.matches(pool, task))
            .collect();
        tasks.sort_by_key(|task| std::cmp::Reverse(task.id));
        Ok(TaskPage::from_sorted(tasks, query.limit))
    }
    /// Startup scan. Completed history must not count against execution capacity.
    fn unfinished(&self, pool: u128) -> Outcome<Vec<SavedTask>> {
        Ok(self
            .list(pool)?
            .into_iter()
            .filter(SavedTask::unfinished)
            .collect())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskQuery {
    pub before: Option<TaskId>,
    pub limit: usize,
    pub business: Option<String>,
}
impl Default for TaskQuery {
    fn default() -> Self {
        Self {
            before: None,
            limit: 100,
            business: None,
        }
    }
}
impl TaskQuery {
    pub fn validate(&self, pool: u128) -> Outcome<()> {
        if !(1..=1000).contains(&self.limit) || self.before.is_some_and(|id| id.pool != pool) {
            return Err(Failure::Rejected(
                "invalid history page size or pool cursor".into(),
            ));
        }
        Ok(())
    }
    pub fn matches(&self, pool: u128, task: &SavedTask) -> bool {
        task.id.pool == pool
            && self.before.is_none_or(|before| task.id < before)
            && self
                .business
                .as_ref()
                .is_none_or(|name| name == task.id.business())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskPage {
    pub tasks: Vec<SavedTask>,
    pub next: Option<TaskId>,
}
impl TaskPage {
    /// Supply descending records, including one extra record to detect the next page.
    pub fn from_sorted(mut tasks: Vec<SavedTask>, limit: usize) -> Self {
        let more = tasks.len() > limit;
        tasks.truncate(limit);
        let next = if more {
            tasks.last().map(|task| task.id)
        } else {
            None
        };
        Self { tasks, next }
    }
}
impl SavedTask {
    fn unfinished(&self) -> bool {
        matches!(
            self.outcome,
            StoredOutcome::Pending | StoredOutcome::NeedsCheck(_)
        )
    }
}
#[derive(Default)]
pub struct MemoryTaskStore {
    state: Mutex<StoreState>,
}
#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct StoreState {
    pub epochs: BTreeMap<u128, u64>,
    pub records: BTreeMap<TaskId, SavedTask>,
}
impl StoreState {
    pub fn query(&self, pool: u128, query: &TaskQuery) -> Outcome<TaskPage> {
        query.validate(pool)?;
        Ok(TaskPage::from_sorted(
            self.records
                .values()
                .rev()
                .filter(|task| query.matches(pool, task))
                .take(query.limit + 1)
                .cloned()
                .collect(),
            query.limit,
        ))
    }
    pub fn unfinished(&self, pool: u128) -> Vec<SavedTask> {
        self.records
            .values()
            .filter(|task| task.id.pool == pool && task.unfinished())
            .cloned()
            .collect()
    }
    pub fn claim(&mut self, pool: u128, epoch: u64) -> Outcome<()> {
        if epoch == 0 || self.epochs.get(&pool).is_some_and(|old| epoch <= *old) {
            return Err(Failure::Rejected("leader epoch is not newer".into()));
        }
        self.epochs.insert(pool, epoch);
        Ok(())
    }
    pub fn save(&mut self, task: SavedTask, previous: Option<u64>) -> Outcome<()> {
        if self.epochs.get(&task.id.pool) != Some(&task.owner_epoch) {
            return Err(Failure::Rejected("stale task owner".into()));
        }
        if self.records.get(&task.id).map(|r| r.revision) != previous
            || Some(task.revision) != previous.map_or(Some(0), |r| r.checked_add(1))
        {
            return Err(Failure::Rejected("task revision conflict".into()));
        }
        self.records.insert(task.id, task);
        Ok(())
    }
}
impl TaskStore for MemoryTaskStore {
    fn get(&self, id: TaskId) -> Outcome<Option<SavedTask>> {
        Ok(self.state.lock().unwrap().records.get(&id).cloned())
    }
    fn query(&self, pool: u128, query: &TaskQuery) -> Outcome<TaskPage> {
        self.state.lock().unwrap().query(pool, query)
    }
    fn unfinished(&self, pool: u128) -> Outcome<Vec<SavedTask>> {
        Ok(self.state.lock().unwrap().unfinished(pool))
    }
    fn claim(&self, pool: u128, epoch: u64) -> Outcome<()> {
        self.state.lock().unwrap().claim(pool, epoch)
    }
    fn list(&self, pool: u128) -> Outcome<Vec<SavedTask>> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .records
            .values()
            .filter(|r| r.id.pool == pool)
            .cloned()
            .collect())
    }
    fn save(&self, task: SavedTask, previous: Option<u64>) -> Outcome<()> {
        self.state.lock().unwrap().save(task, previous)
    }
}
pub(crate) async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Outcome<T> + Send + 'static,
) -> Outcome<T> {
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| Failure::Uncertain(format!("task store: {e}")))?
}
#[derive(Clone)]
pub(crate) struct Journal {
    pub record: Arc<tokio::sync::Mutex<SavedTask>>,
    pub store: Arc<dyn TaskStore>,
}
impl Journal {
    pub async fn create(&self) -> Outcome<()> {
        let task = self.record.lock().await.clone();
        let store = self.store.clone();
        blocking(move || store.save(task, None)).await
    }
    pub async fn update(&self, change: impl FnOnce(&mut SavedTask)) -> Outcome<()> {
        self.update_if(|task| {
            change(task);
            true
        })
        .await
    }
    pub async fn update_if(&self, change: impl FnOnce(&mut SavedTask) -> bool) -> Outcome<()> {
        let mut current = self.record.lock().await;
        let mut next = current.clone();
        if !change(&mut next) {
            return Ok(());
        }
        next.observation.updated_at = Some(SystemTime::now());
        next.revision = next
            .revision
            .checked_add(1)
            .ok_or_else(|| Failure::Rejected("task revision exhausted".into()))?;
        let store = self.store.clone();
        let saved = next.clone();
        let revision = current.revision;
        blocking(move || store.save(saved, Some(revision))).await?;
        *current = next;
        Ok(())
    }
}

// Compatibility exports; implementation belongs to execution, not storage.
pub use crate::driver::recovery::Restored;
pub use crate::workflow::Dependencies;

mod file;
pub use file::FileTaskStore;
