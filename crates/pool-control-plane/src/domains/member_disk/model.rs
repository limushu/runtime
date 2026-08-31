use crate::kernel::{
    BlkId, ByteCount, FailureDomainId, MemberDiskId, PhysicalDiskId, PoolId, TierId,
};
use control_runtime::{RuntimeError, RuntimeResult};
use std::sync::Arc;

const ONE_GIB: u64 = 1024 * 1024 * 1024;
const TWO_GIB: u64 = 2 * ONE_GIB;
const LARGE_DISK_THRESHOLD: u64 = 8 * 1024 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MediaClass(Arc<str>);

impl MediaClass {
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlkSize {
    GiB1,
    GiB2,
}

impl BlkSize {
    pub const fn bytes(self) -> ByteCount {
        ByteCount::new(match self {
            Self::GiB1 => ONE_GIB,
            Self::GiB2 => TWO_GIB,
        })
    }

    pub const fn for_capacity(capacity: ByteCount) -> Self {
        if capacity.get() > LARGE_DISK_THRESHOLD {
            Self::GiB2
        } else {
            Self::GiB1
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PhysicalState {
    Up,
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationState {
    Active,
    Inactive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MembershipState {
    Member,
    Removed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskSharing {
    Exclusive,
    SharedCache,
}

/// Operational projection used by the statechart. It is not the MemberDisk
/// entity and is never the only persisted metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskState {
    Ua,
    Da,
    Di,
    Ui,
    Removed,
}

impl MemberDiskState {
    pub const fn project(
        physical: PhysicalState,
        allocation: AllocationState,
        membership: MembershipState,
    ) -> Self {
        if matches!(membership, MembershipState::Removed) {
            return Self::Removed;
        }
        match (physical, allocation) {
            (PhysicalState::Up, AllocationState::Active) => Self::Ua,
            (PhysicalState::Down, AllocationState::Active) => Self::Da,
            (PhysicalState::Down, AllocationState::Inactive) => Self::Di,
            (PhysicalState::Up, AllocationState::Inactive) => Self::Ui,
        }
    }

    pub const fn io_available(self) -> bool {
        matches!(self, Self::Ua | Self::Ui)
    }

    pub const fn allocation_active(self) -> bool {
        matches!(self, Self::Ua | Self::Da)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskSpec {
    pub id: MemberDiskId,
    pub physical_disk: PhysicalDiskId,
    pub pool: PoolId,
    pub tier: TierId,
    pub media_class: MediaClass,
    pub capacity: ByteCount,
    pub failure_domains: Vec<FailureDomainId>,
    pub sharing: DiskSharing,
}

impl MemberDiskSpec {
    pub fn new(
        id: MemberDiskId,
        physical_disk: PhysicalDiskId,
        pool: PoolId,
        tier: TierId,
        media_class: MediaClass,
        capacity: ByteCount,
        failure_domains: Vec<FailureDomainId>,
    ) -> Self {
        Self {
            id,
            physical_disk,
            pool,
            tier,
            media_class,
            capacity,
            failure_domains,
            sharing: DiskSharing::Exclusive,
        }
    }

    pub fn shared_cache(mut self) -> Self {
        self.sharing = DiskSharing::SharedCache;
        self
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MemberDiskPatch {
    pub tier: Option<TierId>,
    pub failure_domains: Option<Vec<FailureDomainId>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationBitmap {
    total_blocks: u64,
    words: Vec<u64>,
}

impl AllocationBitmap {
    fn new(total_blocks: u64) -> Self {
        Self {
            total_blocks,
            words: vec![0; total_blocks.div_ceil(64) as usize],
        }
    }

    pub fn total_blocks(&self) -> u64 {
        self.total_blocks
    }

    pub fn allocated_blocks(&self) -> u64 {
        self.words.iter().map(|word| word.count_ones() as u64).sum()
    }

    pub(crate) fn allocate_one(&mut self) -> RuntimeResult<BlkId> {
        for index in 0..self.total_blocks {
            let word = (index / 64) as usize;
            let bit = index % 64;
            if self.words[word] & (1 << bit) == 0 {
                self.words[word] |= 1 << bit;
                return Ok(BlkId::new(index.to_string()));
            }
        }
        Err(RuntimeError::Rejected("member disk has no free BLK".into()))
    }

    pub(crate) fn release(&mut self, blk: &BlkId) -> RuntimeResult<()> {
        let index = blk
            .as_str()
            .parse::<u64>()
            .map_err(|_| RuntimeError::InvalidState(format!("invalid BLK id {blk}")))?;
        if index >= self.total_blocks {
            return Err(RuntimeError::InvalidState(format!(
                "BLK {blk} is outside the member disk"
            )));
        }
        let word = (index / 64) as usize;
        let bit = index % 64;
        if self.words[word] & (1 << bit) == 0 {
            return Err(RuntimeError::InvalidState(format!(
                "BLK {blk} is not allocated"
            )));
        }
        self.words[word] &= !(1 << bit);
        Ok(())
    }
}

/// Persisted decision record owned exclusively by the MemberDisk domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskRecord {
    spec: MemberDiskSpec,
    allocation_state: AllocationState,
    membership_state: MembershipState,
    allocation: AllocationBitmap,
    revision: u64,
}

impl MemberDiskRecord {
    pub fn new(spec: MemberDiskSpec) -> Self {
        let block_size = BlkSize::for_capacity(spec.capacity).bytes().get();
        let total_blocks = spec.capacity.get() / block_size;
        Self {
            spec,
            allocation_state: AllocationState::Active,
            membership_state: MembershipState::Member,
            allocation: AllocationBitmap::new(total_blocks),
            revision: 1,
        }
    }

    pub fn id(&self) -> &MemberDiskId {
        &self.spec.id
    }

    pub fn pool(&self) -> &PoolId {
        &self.spec.pool
    }

    pub fn spec(&self) -> &MemberDiskSpec {
        &self.spec
    }

    pub fn allocation_state(&self) -> AllocationState {
        self.allocation_state
    }

    pub fn membership_state(&self) -> MembershipState {
        self.membership_state
    }

    pub fn allocation(&self) -> &AllocationBitmap {
        &self.allocation
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn apply_patch(&mut self, patch: MemberDiskPatch) {
        if let Some(tier) = patch.tier {
            self.spec.tier = tier;
        }
        if let Some(failure_domains) = patch.failure_domains {
            self.spec.failure_domains = failure_domains;
        }
        self.revision += 1;
    }

    pub(crate) fn set_allocation_state(&mut self, state: AllocationState) {
        if self.allocation_state != state {
            self.allocation_state = state;
            self.revision += 1;
        }
    }

    pub(crate) fn set_membership_state(&mut self, state: MembershipState) {
        if self.membership_state != state {
            self.membership_state = state;
            self.revision += 1;
        }
    }

    pub(crate) fn allocate_one(&mut self) -> RuntimeResult<BlkId> {
        let blk = self.allocation.allocate_one()?;
        self.revision += 1;
        Ok(blk)
    }

    pub(crate) fn release(&mut self, blk: &BlkId) -> RuntimeResult<()> {
        self.allocation.release(blk)?;
        self.revision += 1;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskSnapshot {
    pub spec: MemberDiskSpec,
    pub physical_state: PhysicalState,
    pub allocation_state: AllocationState,
    pub membership_state: MembershipState,
    pub operational_state: MemberDiskState,
    pub block_size: BlkSize,
    pub total_blocks: u64,
    pub allocated_blocks: u64,
    pub revision: u64,
}
