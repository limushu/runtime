//! Optional reference adapter. User adapters do not need to enable kv-redb.
use super::{
    store::{validate_query, validate_record},
    *,
};
use crate::{Failure, Outcome, PoolUuid};
use ::redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use std::{path::Path, sync::Arc};

const TASKS: TableDefinition<&str, &[u8]> = TableDefinition::new("task_records_v1");
const HISTORY: TableDefinition<&str, &str> = TableDefinition::new("task_history_v1");
const ACTIVE: TableDefinition<&str, &str> = TableDefinition::new("recoverable_tasks_v1");
pub struct RedbTaskStore {
    db: Arc<Database>,
}
fn err(e: impl std::fmt::Display) -> Failure {
    Failure::Failed(format!("task database: {e}"))
}
fn decode(bytes: &[u8]) -> Outcome<StoredTask> {
    let task: StoredTask = serde_json::from_slice(bytes).map_err(err)?;
    if task.format_version != 1 {
        return Err(Failure::Rejected("unsupported task record format".into()));
    }
    Ok(task)
}
impl RedbTaskStore {
    /// Open in a blocking context (before starting a runtime, or via spawn_blocking).
    pub fn open(path: impl AsRef<Path>) -> Outcome<Self> {
        Self::from_database(Arc::new(Database::create(path).map_err(err)?))
    }
    /// Embed task history beside metadata in the application's existing database.
    pub fn from_database(db: Arc<Database>) -> Outcome<Self> {
        let tx = db.begin_write().map_err(err)?;
        tx.open_table(TASKS).map_err(err)?;
        tx.open_table(HISTORY).map_err(err)?;
        tx.open_table(ACTIVE).map_err(err)?;
        tx.commit()
            .map_err(|e| Failure::Uncertain(format!("task database commit: {e}")))?;
        Ok(Self { db })
    }
}
impl TaskStore for RedbTaskStore {
    fn get(&self, pool: PoolUuid, id: TaskId) -> Outcome<Option<StoredTask>> {
        let tx = self.db.begin_read().map_err(err)?;
        let table = tx.open_table(TASKS).map_err(err)?;
        let key = format!("{pool}/{id}");
        table
            .get(key.as_str())
            .map_err(err)?
            .map(|v| decode(v.value()))
            .transpose()
    }
    fn compare_and_set(&self, expected: Option<u64>, task: &StoredTask) -> Outcome<bool> {
        validate_record(expected, task)?;
        let mut tx = self.db.begin_write().map_err(err)?;
        tx.set_durability(::redb::Durability::Immediate)
            .map_err(err)?;
        {
            let mut tasks = tx.open_table(TASKS).map_err(err)?;
            let key = format!("{}/{}", task.pool, task.id);
            let old = tasks
                .get(key.as_str())
                .map_err(err)?
                .map(|v| decode(v.value()))
                .transpose()?;
            if old.as_ref().map(|t| t.revision) != expected {
                return Ok(false);
            }
            if old
                .as_ref()
                .is_some_and(|t| t.created_at != task.created_at)
            {
                return Err(Failure::Rejected("task creation time is immutable".into()));
            }
            let bytes = serde_json::to_vec(task).map_err(err)?;
            tasks.insert(key.as_str(), bytes.as_slice()).map_err(err)?;
            let index = task.index_key();
            tx.open_table(HISTORY)
                .map_err(err)?
                .insert(index.as_str(), key.as_str())
                .map_err(err)?;
            let mut active = tx.open_table(ACTIVE).map_err(err)?;
            if task.recoverable() {
                active.insert(index.as_str(), key.as_str()).map_err(err)?;
            } else {
                active.remove(index.as_str()).map_err(err)?;
            }
        }
        tx.commit()
            .map_err(|e| Failure::Uncertain(format!("task database commit: {e}")))?;
        Ok(true)
    }
    fn list(&self, pool: PoolUuid, q: &TaskQuery) -> Outcome<TaskPage> {
        validate_query(pool, q)?;
        let tx = self.db.begin_read().map_err(err)?;
        let tasks = tx.open_table(TASKS).map_err(err)?;
        let index = tx
            .open_table(if q.recoverable_only { ACTIVE } else { HISTORY })
            .map_err(err)?;
        let start = format!("{pool}/");
        let end = q.before.clone().unwrap_or_else(|| format!("{pool}/~"));
        let mut page = Vec::new();
        for item in index
            .range(start.as_str()..end.as_str())
            .map_err(err)?
            .rev()
            .take(q.limit + 1)
        {
            let (_, key) = item.map_err(err)?;
            let value = tasks
                .get(key.value())
                .map_err(err)?
                .ok_or_else(|| err("dangling task index"))?;
            page.push(decode(value.value())?);
        }
        let more = page.len() > q.limit;
        page.truncate(q.limit);
        let next = more.then(|| page.last().unwrap().index_key());
        Ok(TaskPage { tasks: page, next })
    }
}
