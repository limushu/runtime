use super::*;
use crate::{Failure, Outcome, PoolUuid};
use std::{collections::BTreeMap, sync::Mutex};

/// Blocking storage protocol, invoked ONLY by the dedicated blocking recorder worker.
/// Supply your own adapter; TaskCenter never depends on a concrete database.
pub trait TaskStore: Send + Sync + 'static {
    fn get(&self, pool: PoolUuid, id: TaskId) -> Outcome<Option<StoredTask>>;
    /// Atomic create (None) or revision-checked replacement. Return false on CAS conflict.
    /// Ok(true) means DURABLE commit, including history/recoverable query indexes.
    /// On an ambiguous commit return Failure::Uncertain, not a known no-effect failure.
    fn compare_and_set(&self, expected: Option<u64>, task: &StoredTask) -> Outcome<bool>;
    fn list(&self, pool: PoolUuid, query: &TaskQuery) -> Outcome<TaskPage>;
}

pub(crate) fn validate_query(pool: PoolUuid, q: &TaskQuery) -> Outcome<()> {
    if q.limit == 0
        || q.limit > 200
        || q.before
            .as_ref()
            .is_some_and(|s| !s.starts_with(&format!("{pool}/")))
    {
        return Err(Failure::Rejected(
            "invalid page size or foreign pool cursor".into(),
        ));
    }
    Ok(())
}
pub(crate) fn validate_record(expected: Option<u64>, t: &StoredTask) -> Outcome<()> {
    if t.format_version != 1 || t.revision != expected.map_or(0, |v| v.saturating_add(1)) {
        return Err(Failure::Rejected(
            "unsupported task format or invalid revision".into(),
        ));
    }
    Ok(())
}

/// Useful for tests and applications that explicitly choose non-durable history.
#[derive(Default)]
pub struct MemoryTaskStore {
    records: Mutex<BTreeMap<(PoolUuid, TaskId), StoredTask>>,
}
impl TaskStore for MemoryTaskStore {
    fn get(&self, pool: PoolUuid, id: TaskId) -> Outcome<Option<StoredTask>> {
        Ok(self.records.lock().unwrap().get(&(pool, id)).cloned())
    }
    fn compare_and_set(&self, expected: Option<u64>, task: &StoredTask) -> Outcome<bool> {
        validate_record(expected, task)?;
        let mut records = self.records.lock().unwrap();
        if records.get(&(task.pool, task.id)).map(|t| t.revision) != expected {
            return Ok(false);
        }
        if records
            .get(&(task.pool, task.id))
            .is_some_and(|t| t.created_at != task.created_at)
        {
            return Err(Failure::Rejected("task creation time is immutable".into()));
        }
        records.insert((task.pool, task.id), task.clone());
        Ok(true)
    }
    fn list(&self, pool: PoolUuid, query: &TaskQuery) -> Outcome<TaskPage> {
        validate_query(pool, query)?;
        let mut tasks: Vec<_> = self
            .records
            .lock()
            .unwrap()
            .values()
            .filter(|t| t.pool == pool && (!query.recoverable_only || t.recoverable()))
            .filter(|t| query.before.as_ref().is_none_or(|b| t.index_key() < *b))
            .cloned()
            .collect();
        tasks.sort_by_key(|t| std::cmp::Reverse(t.index_key()));
        let more = tasks.len() > query.limit;
        tasks.truncate(query.limit);
        let next = more.then(|| tasks.last().unwrap().index_key());
        Ok(TaskPage { tasks, next })
    }
}
