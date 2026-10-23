use crate::{domains::balance::EvacuationId, domains::vd::BgEntry, *};
use async_trait::async_trait;
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct AllocationOwner {
    pub disk: DiskId,
    pub blk: u32,
    pub volume: VolumeId,
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetadataSnapshot {
    pub tiers: Vec<Tier>,
    pub disks: Vec<MemberDisk>,
    pub volumes: Vec<VirtualDisk>,
    pub allocations: Vec<AllocationOwner>,
    pub retired_tiers: Vec<TierId>,
    pub retired_disks: Vec<DiskId>,
    pub deleted_volumes: Vec<VirtualDisk>,
    pub released: Vec<AllocationOwner>,
}
impl MetadataSnapshot {
    pub fn disk(&self, id: &DiskId) -> Outcome<MemberDisk> {
        self.disks
            .iter()
            .find(|d| &d.id == id)
            .cloned()
            .ok_or_else(|| crate::support::reject("unknown member disk"))
    }
    /// Deleted placement is durable evidence for retrying allocation cleanup.
    pub fn volume(&self, id: &VolumeId) -> Outcome<VirtualDisk> {
        self.volumes
            .iter()
            .chain(&self.deleted_volumes)
            .find(|v| &v.id == id)
            .cloned()
            .ok_or_else(|| crate::support::reject("unknown volume"))
    }
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
/// ambiguous transport/commit outcomes MUST be Uncertain and must never be replayed.
/// KvMetadata enforces entity revisions and atomic multi-record invariants for all backends.
#[async_trait]
pub trait MetadataStore: Send + Sync + 'static {
    async fn commit(&self, pool: &PoolId, change: MetadataChange) -> Outcome<()>;
    /// Authoritative, internally consistent projection; required for shrink and takeover.
    async fn snapshot(&self, pool: &PoolId) -> Outcome<MetadataSnapshot>;
}
