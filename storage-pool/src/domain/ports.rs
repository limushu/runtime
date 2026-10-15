//! Explicit external boundaries. No implementation here performs real disk IO.
use crate::domain::{
    PoolId,
    disk::{DiskId, IoState, MemberDisk},
    node::NodeId,
    vd::{BgEntry, VirtualDisk, VolumeId},
};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use workflow_foundation::{Failure, Outcome};

/// Field-level mutations, not overwrites of stale MemberDisk snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataChange {
    AddDisk(MemberDisk),
    DiskIo {
        disk: DiskId,
        expected_revision: u64,
        io: IoState,
    },
    Allocate {
        disk: DiskId,
        expected_revision: u64,
        volume: VolumeId,
        blk: u32,
    },
    CreateVolume(VirtualDisk),
    InvalidateDisk {
        disk: DiskId,
    },
}

/// Ok means acknowledged commit. Known failure MUST mean not committed;
/// ambiguous transport/commit outcomes MUST be Uncertain. No automatic retries.
/// Production adapters must enforce revisions and fencing; memory is only a simulator.
#[async_trait]
pub trait MetadataStore: Send + Sync + 'static {
    async fn commit(&self, pool: &PoolId, change: MetadataChange) -> Outcome<()>;
}

/// An operation must return only after it is stable. Cancelled means stopped with
/// no unreported effects; successful irreversible completion wins cancellation.
/// A possibly applied effect must return Uncertain, never Failed/Unavailable.
#[async_trait]
pub trait DataPlane: Send + Sync + 'static {
    async fn load_pool(
        &self,
        pool: &PoolId,
        node: &NodeId,
        cancel: &CancellationToken,
    ) -> Outcome<()>;
    /// Idempotently open/prepare the member disk, without enabling IO or allocation.
    /// An opened-but-disabled disk is a safe partial state for retry/reconciliation.
    async fn open_disk(
        &self,
        pool: &PoolId,
        disk: &DiskId,
        cancel: &CancellationToken,
    ) -> Outcome<()>;
    /// Enable/disable IO. Up requires a previously confirmed open_disk step.
    async fn set_disk_io(
        &self,
        pool: &PoolId,
        disk: &DiskId,
        io: IoState,
        cancel: &CancellationToken,
    ) -> Outcome<()>;
    /// One batch request. Return exactly one identified result for every input disk,
    /// only after every dispatched effect is stable. Transport ambiguity is Uncertain.
    /// Adapters may override this to issue one native multi-disk notification.
    async fn set_disks_io(
        &self,
        pool: &PoolId,
        disks: &[DiskId],
        io: IoState,
        cancel: &CancellationToken,
    ) -> Outcome<Vec<(DiskId, Outcome<()>)>> {
        let mut results = Vec::new();
        for disk in disks {
            results.push((disk.clone(), self.set_disk_io(pool, disk, io, cancel).await));
        }
        Ok(results)
    }
    /// Skeleton integration point. Ok must mean rebuild and committed reference
    /// removal are confirmed, not merely that rebuilding was scheduled.
    /// Cancellation returns only after in-flight rebuild effects are stable.
    async fn wait_rebuild(
        &self,
        _pool: &PoolId,
        _disks: &[DiskId],
        _cancel: &CancellationToken,
    ) -> Outcome<()> {
        Err(Failure::Failed(
            "waiting for rebuild is not implemented".into(),
        ))
    }
    /// Confirm initialization of every replica before metadata can mark it Valid.
    async fn initialize_bg(
        &self,
        pool: &PoolId,
        volume: &VolumeId,
        entries: &[BgEntry],
        cancel: &CancellationToken,
    ) -> Outcome<()>;
}
