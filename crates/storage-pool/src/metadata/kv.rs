use crate::Outcome;
use async_trait::async_trait;
use std::{collections::BTreeMap, sync::Mutex};

pub type KvSnapshot = BTreeMap<Vec<u8>, Vec<u8>>;
pub struct KvCheck {
    pub key: Vec<u8>,
    /// None requires absence. Bytes must match exactly.
    pub expected: Option<Vec<u8>>,
}
pub struct KvWrite {
    pub key: Vec<u8>,
    /// None deletes a key.
    pub value: Option<Vec<u8>>,
}
#[derive(Default)]
pub struct KvTransaction {
    pub checks: Vec<KvCheck>,
    pub writes: Vec<KvWrite>,
}

/// Implement with a native snapshot/read transaction and conditional write transaction.
/// All prefixes in one read share a consistent view. All checks and writes are atomic.
/// false means a comparison failed and nothing changed. Known errors mean no commit;
/// an ambiguous commit MUST return Failure::Uncertain. Do not emulate atomicity with
/// unrelated get/put calls or a lock confined to one repository instance.
#[async_trait]
pub trait KvStore: Send + Sync + 'static {
    async fn snapshot(&self, prefixes: &[Vec<u8>]) -> Outcome<KvSnapshot>;
    async fn transact(&self, transaction: KvTransaction) -> Outcome<bool>;
}

#[derive(Default)]
pub struct MemoryKv {
    entries: Mutex<KvSnapshot>,
}
#[async_trait]
impl KvStore for MemoryKv {
    async fn snapshot(&self, prefixes: &[Vec<u8>]) -> Outcome<KvSnapshot> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .iter()
            .filter(|(key, _)| prefixes.iter().any(|p| key.starts_with(p)))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
    async fn transact(&self, transaction: KvTransaction) -> Outcome<bool> {
        let mut entries = self.entries.lock().unwrap();
        if transaction
            .checks
            .iter()
            .any(|c| entries.get(&c.key) != c.expected.as_ref())
        {
            return Ok(false);
        }
        for write in transaction.writes {
            match write.value {
                Some(value) => {
                    entries.insert(write.key, value);
                }
                None => {
                    entries.remove(&write.key);
                }
            }
        }
        Ok(true)
    }
}
