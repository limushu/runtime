//! Storage capabilities used by shrink. Implement against the authoritative domain
//! metadata/VD references and user_dp; do not maintain a second disk catalogue here.
use crate::{DiskId, PoolId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use workflow_foundation::{
    ActivityClient, ActivityContext, Outcome, Readiness, ResourceAccess, ServiceHandler,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ShrinkStage {
    #[default]
    RecordIntent,
    DisableAllocation,
    Migrate,
    VerifyEvacuated,
    DisableIo,
    RemoveMembers,
    Complete,
}

/// Every mutation receives the entire target set. Steps must be idempotent after
/// process failure, including partially committed batches. Cancellation never rolls
/// back migrated data. Interrupted/failed shrink leaves allocation disabled until an
/// explicit management decision; cancellation never silently resumes the old task.
#[async_trait]
pub trait ShrinkStorage: Send + Sync + 'static {
    /// Read-only, cancellable check. Include membership, capacity, replica placement
    /// and topology versions as appropriate for this stage. Mutations revalidate too.
    async fn readiness(
        &self,
        pool: &PoolId,
        disks: &[DiskId],
        stage: ShrinkStage,
    ) -> Outcome<Readiness>;
    async fn record_intent(
        &self,
        pool: &PoolId,
        disks: &[DiskId],
        cancel: &CancellationToken,
    ) -> Outcome<()>;
    async fn disable_allocation(
        &self,
        pool: &PoolId,
        disks: &[DiskId],
        cancel: &CancellationToken,
    ) -> Outcome<()>;
    /// Check authoritative BG references AND allocation ownership, not only migration's reply.
    async fn verify_evacuated(
        &self,
        pool: &PoolId,
        disks: &[DiskId],
        cancel: &CancellationToken,
    ) -> Outcome<()>;
    async fn disable_io(
        &self,
        pool: &PoolId,
        disks: &[DiskId],
        cancel: &CancellationToken,
    ) -> Outcome<()>;
    /// Recheck absence of references and disabled IO before committed membership removal.
    async fn remove_members(
        &self,
        pool: &PoolId,
        disks: &[DiskId],
        cancel: &CancellationToken,
    ) -> Outcome<()>;
}
#[derive(Clone)]
pub struct ShrinkCommand {
    pub pool: PoolId,
    pub disks: Vec<DiskId>,
    pub stage: ShrinkStage,
}
pub struct ShrinkStorageHost(pub Arc<dyn ShrinkStorage>);
pub type ShrinkStorageClient = ActivityClient<ShrinkCommand, (), (), ()>;
#[async_trait]
impl ServiceHandler for ShrinkStorageHost {
    type Request = ShrinkCommand;
    type Reply = ();
    type Query = ();
    type View = ();
    fn resources(_: &ShrinkCommand) -> Vec<ResourceAccess> {
        vec![]
    }
    fn activity_name(command: &ShrinkCommand) -> &'static str {
        match command.stage {
            ShrinkStage::RecordIntent => "shrink.record-intent",
            ShrinkStage::DisableAllocation => "shrink.disable-allocation",
            ShrinkStage::VerifyEvacuated => "shrink.verify-evacuated",
            ShrinkStage::DisableIo => "shrink.disable-io",
            ShrinkStage::RemoveMembers => "shrink.remove-members",
            _ => "shrink.invalid-storage-step",
        }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
    async fn execute(&self, command: ShrinkCommand, ctx: ActivityContext) -> Outcome<()> {
        let (pool, disks, cancel) = (&command.pool, command.disks.as_slice(), ctx.cancellation());
        match command.stage {
            ShrinkStage::RecordIntent => self.0.record_intent(pool, disks, cancel).await,
            ShrinkStage::DisableAllocation => self.0.disable_allocation(pool, disks, cancel).await,
            ShrinkStage::VerifyEvacuated => self.0.verify_evacuated(pool, disks, cancel).await,
            ShrinkStage::DisableIo => self.0.disable_io(pool, disks, cancel).await,
            ShrinkStage::RemoveMembers => self.0.remove_members(pool, disks, cancel).await,
            _ => Err(crate::support::reject("not a storage mutation")),
        }
    }
}
