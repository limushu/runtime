//! Native balance boundary. A submission is one whole batch, not a disk-at-a-time loop.
mod readiness;
mod runtime;
use crate::{DiskId, PoolId};
use async_trait::async_trait;
pub(crate) use readiness::evacuation_readiness;
pub use runtime::{BalanceClient, BalanceHost};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::watch;
use workflow_foundation::{ActivityExit, Outcome, TaskId};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationProgress {
    pub completed: u64,
    pub total: u64,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MigrationCheckpoint {
    /// Backend-defined durable data, never a C pointer, callback address or live handle.
    pub resume: Value,
    pub progress: MigrationProgress,
}
#[derive(Debug, Clone)]
pub struct Migrate {
    pub pool: PoolId,
    /// Stable across resumed invocations and leader recovery; use for deduplication.
    pub task_id: TaskId,
    pub disks: Vec<DiskId>,
    pub checkpoint: MigrationCheckpoint,
}
pub type MigrationExit = ActivityExit<MigrationProgress, MigrationCheckpoint>;

/// Adapt the C submit API here. Submission must settle even if cancellation arrives
/// while it is in flight. Return an independently owned callback/operation handle.
#[async_trait]
pub trait BalanceBackend: Send + Sync + 'static {
    async fn submit(&self, request: Migrate) -> Outcome<Arc<dyn BalanceOperation>>;
}

/// The bridge owns C callback memory until completion guarantees it is no longer used.
/// Before publishing any terminal result (including suspension/cancellation), all
/// settled BG moves must be committed to the same MetadataStore read by Disk/VD:
/// references, allocation bitmaps, owners and revisions must agree. A partial move
/// with unknown commit status is Uncertain, not a safe suspension acknowledgment.
/// Every result ends this invocation and releases its running resources. Suspended
/// retains only checkpoint data. A late cancellation must preserve actual completion.
#[async_trait]
pub trait BalanceOperation: Send + Sync + 'static {
    /// Retained terminal result. Safe to stop polling; never resubmits or cancels C work.
    async fn completion(&self) -> Outcome<MigrationExit>;
    /// Send the separate C suspend/cancel command. These methods acknowledge command
    /// delivery; completion() confirms actual quiescence, including callback teardown.
    /// Commands are idempotent, including when completion races command delivery.
    async fn suspend(&self) -> Outcome<()>;
    async fn cancel(&self) -> Outcome<()>;
    fn progress(&self) -> watch::Receiver<MigrationProgress>;
}
