//! Explicit external boundaries. No implementation here performs real disk IO.
use crate::domains::balance::*;
use crate::domains::{
    PoolId,
    disk::{DiskId, IoState, MemberDisk},
    node::NodeId,
    vd::{BgEntry, VirtualDisk, VolumeId},
};
use crate::{Tier, TierId};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
use workflow_foundation::{Failure, Outcome};

#[derive(Debug, Clone)]
pub struct AllocationOwner {
    pub disk: DiskId,
    pub blk: u32,
    pub volume: VolumeId,
}
#[derive(Debug, Clone, Default)]
pub struct MetadataSnapshot {
    pub tiers: Vec<Tier>,
    pub disks: Vec<MemberDisk>,
    pub volumes: Vec<VirtualDisk>,
    pub allocations: Vec<AllocationOwner>,
}

/// Existing balancing domain service: one idempotent whole-disk job, not WF batches.
#[async_trait]
pub trait Balancer: Send + Sync + 'static {
    async fn check_shrink(&self, pool: &PoolId, disk: &DiskId) -> Outcome<ShrinkReadiness>;
    /// Create or attach by the durable intent ID. Never create a second job on retry.
    /// Return only after acceptance is known; ambiguous acceptance is Uncertain.
    async fn evacuate(&self, pool: &PoolId, disk: &DiskId, id: EvacuationId) -> Outcome<ArcJob>;
    async fn find(&self, pool: &PoolId, id: EvacuationId) -> Outcome<Option<ArcJob>>;
}
pub type ArcJob = std::sync::Arc<dyn EvacuationJob>;
#[async_trait]
pub trait EvacuationJob: Send + Sync + 'static {
    fn progress(&self) -> tokio::sync::watch::Receiver<EvacuationProgress>;
    async fn wait(&self) -> Outcome<()>;
    async fn pause(&self) -> Outcome<()>;
    async fn resume(&self) -> Outcome<()>;
    async fn cancel(&self) -> Outcome<()>;
}

/// Field-level mutations, not overwrites of stale MemberDisk snapshots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MetadataChange {
    ShrinkIntent {
        disk: DiskId,
        expected_revision: u64,
        job: EvacuationId,
    },
    DisableAllocation {
        disk: DiskId,
        expected_revision: u64,
        job: EvacuationId,
    },
    ClearShrink {
        disk: DiskId,
        expected_revision: u64,
        job: EvacuationId,
    },
    MarkRemoved {
        disk: DiskId,
        expected_revision: u64,
        job: EvacuationId,
    },
    /// Atomic placement/allocation update after the balancing module confirms data copy.
    Remap {
        volume: VolumeId,
        expected_revision: u64,
        group: u32,
        source: BgEntry,
        target: BgEntry,
        source_revision: u64,
        target_revision: u64,
    },
    CreateTier(Tier),
    DeleteTier {
        id: TierId,
        expected_revision: u64,
    },
    RemoveDisk {
        disk: DiskId,
        expected_revision: u64,
    },
    DeleteVolume {
        volume: VolumeId,
        expected_revision: u64,
    },
    ReleaseAllocation {
        disk: DiskId,
        expected_revision: u64,
        volume: VolumeId,
        blk: u32,
    },
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
    /// Authoritative, internally consistent projection; required for shrink and takeover.
    async fn snapshot(&self, _pool: &PoolId) -> Outcome<MetadataSnapshot> {
        Err(Failure::Rejected(
            "metadata snapshot is not configured".into(),
        ))
    }
}

/// An operation must return only after it is stable. Cancelled means stopped with
/// no unreported effects; successful irreversible completion wins cancellation.
/// A possibly applied effect must return Uncertain, never Failed/Unavailable.
#[async_trait]
pub trait DataPlane: Send + Sync + 'static {
    /// Ok confirms that this Pool is no longer loaded on the node.
    async fn unload_pool(
        &self,
        _pool: &PoolId,
        _node: &NodeId,
        _cancel: &CancellationToken,
    ) -> Outcome<()> {
        Err(Failure::Rejected(
            "pool unload is not supported by this data plane".into(),
        ))
    }
    /// Ok confirms no further IO can reference this disk through this Pool.
    async fn close_disk(
        &self,
        _pool: &PoolId,
        _disk: &DiskId,
        _cancel: &CancellationToken,
    ) -> Outcome<()> {
        Err(Failure::Rejected(
            "disk close is not supported by this data plane".into(),
        ))
    }
    /// Ok confirms all volume access and outstanding IO have stopped.
    async fn delete_volume(
        &self,
        _pool: &PoolId,
        _volume: &VolumeId,
        _cancel: &CancellationToken,
    ) -> Outcome<()> {
        Err(Failure::Rejected(
            "volume deletion is not supported by this data plane".into(),
        ))
    }
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
