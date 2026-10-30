//! Storage representation of the same Task, not another task manager.
use crate::{Failure, ObjectKey, Outcome, TaskId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, Default, PartialEq)]
pub enum Persistence {
    #[default]
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
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedTask {
    pub id: TaskId,
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
        let mut current = self.record.lock().await;
        let mut next = current.clone();
        change(&mut next);
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
