//! Optional reference backend. Native read/write transactions implement the portable KV contract.
use super::*;
use crate::{Failure, Outcome};
use ::redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use async_trait::async_trait;
use std::{path::Path, sync::Arc};
const RECORDS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("pool_metadata_kv_v1");
pub struct RedbKv {
    db: Arc<Database>,
}
fn error(e: impl std::fmt::Display) -> Failure {
    Failure::Failed(format!("metadata KV: {e}"))
}
impl RedbKv {
    /// Call in a blocking context. All subsequent repository calls use spawn_blocking.
    pub fn open(path: impl AsRef<Path>) -> Outcome<Self> {
        Self::from_database(Arc::new(Database::create(path).map_err(error)?))
    }
    /// Share one database with other tables owned by the embedding application.
    pub fn from_database(db: Arc<Database>) -> Outcome<Self> {
        let tx = db.begin_write().map_err(error)?;
        tx.open_table(RECORDS).map_err(error)?;
        tx.commit()
            .map_err(|e| Failure::Uncertain(format!("metadata table commit: {e}")))?;
        Ok(Self { db })
    }
}
#[async_trait]
impl KvStore for RedbKv {
    async fn snapshot(&self, prefixes: &[Vec<u8>]) -> Outcome<KvSnapshot> {
        let db = self.db.clone();
        let prefixes = prefixes.to_vec();
        tokio::task::spawn_blocking(move || {
            let tx = db.begin_read().map_err(error)?;
            let table = tx.open_table(RECORDS).map_err(error)?;
            let mut records = KvSnapshot::new();
            for prefix in prefixes {
                for item in table.range(prefix.as_slice()..).map_err(error)? {
                    let (key, value) = item.map_err(error)?;
                    if !key.value().starts_with(&prefix) {
                        break;
                    }
                    records.insert(key.value().to_vec(), value.value().to_vec());
                }
            }
            Ok(records)
        })
        .await
        .map_err(error)?
    }
    async fn transact(&self, transaction: KvTransaction) -> Outcome<bool> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let mut tx = db.begin_write().map_err(error)?;
            tx.set_durability(::redb::Durability::Immediate)
                .map_err(error)?;
            {
                let mut table = tx.open_table(RECORDS).map_err(error)?;
                for check in transaction.checks {
                    let actual = table
                        .get(check.key.as_slice())
                        .map_err(error)?
                        .map(|v| v.value().to_vec());
                    if actual != check.expected {
                        return Ok(false);
                    }
                }
                for write in transaction.writes {
                    match write.value {
                        Some(value) => {
                            table
                                .insert(write.key.as_slice(), value.as_slice())
                                .map_err(error)?;
                        }
                        None => {
                            table.remove(write.key.as_slice()).map_err(error)?;
                        }
                    }
                }
            }
            tx.commit()
                .map_err(|e| Failure::Uncertain(format!("metadata KV commit: {e}")))?;
            Ok(true)
        })
        .await
        .map_err(|e| Failure::Uncertain(format!("metadata KV writer interrupted: {e}")))?
    }
}
