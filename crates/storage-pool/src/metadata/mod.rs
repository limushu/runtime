//! Portable metadata repository. Storage adapters implement KV primitives, never pool rules.
mod kv;
mod model;
#[cfg(feature = "kv-redb")]
mod redb;
mod repository;
mod state;
pub use kv::{KvCheck, KvSnapshot, KvStore, KvTransaction, KvWrite, MemoryKv};
pub use model::*;
#[cfg(feature = "kv-redb")]
pub use redb::RedbKv;
pub use repository::KvMetadata;
