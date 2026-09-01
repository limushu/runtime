use super::machine::{MemberDiskChange, MemberDiskEvent, MemberDiskMachine, MemberDiskTransition};
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

/// Operational projection computed from the MemberDisk object. It is never
/// stored as a second source of truth.
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

    fn allocate_one(&mut self) -> RuntimeResult<BlkId> {
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

    fn release(&mut self, blk: &BlkId) -> RuntimeResult<()> {
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

/// The one authoritative in-memory business object for a member disk.
///
/// Persisted decisions and the latest DiskMap observation live together here.
/// `state()` is derived; neither the state machine nor the hidden ActorCell
/// keeps a second copy of this business state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDisk {
    id: MemberDiskId,
    physical_disk: PhysicalDiskId,
    pool: PoolId,
    tier: TierId,
    media_class: MediaClass,
    capacity: ByteCount,
    failure_domains: Vec<FailureDomainId>,
    sharing: DiskSharing,
    physical: PhysicalState,
    allocation_state: AllocationState,
    membership_state: MembershipState,
    allocation: AllocationBitmap,
    revision: u64,
}

impl MemberDisk {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: MemberDiskId,
        physical_disk: PhysicalDiskId,
        pool: PoolId,
        tier: TierId,
        media_class: MediaClass,
        capacity: ByteCount,
        failure_domains: Vec<FailureDomainId>,
    ) -> Self {
        let block_size = BlkSize::for_capacity(capacity).bytes().get();
        Self {
            id,
            physical_disk,
            pool,
            tier,
            media_class,
            capacity,
            failure_domains,
            sharing: DiskSharing::Exclusive,
            physical: PhysicalState::Down,
            allocation_state: AllocationState::Active,
            membership_state: MembershipState::Member,
            allocation: AllocationBitmap::new(capacity.get() / block_size),
            revision: 1,
        }
    }

    pub fn shared_cache(mut self) -> Self {
        self.sharing = DiskSharing::SharedCache;
        self
    }

    pub fn id(&self) -> &MemberDiskId {
        &self.id
    }

    pub fn physical_disk(&self) -> &PhysicalDiskId {
        &self.physical_disk
    }

    pub fn pool(&self) -> &PoolId {
        &self.pool
    }

    pub fn tier(&self) -> &TierId {
        &self.tier
    }

    pub fn media_class(&self) -> &MediaClass {
        &self.media_class
    }

    pub fn capacity(&self) -> ByteCount {
        self.capacity
    }

    pub fn failure_domains(&self) -> &[FailureDomainId] {
        &self.failure_domains
    }

    pub fn sharing(&self) -> DiskSharing {
        self.sharing
    }

    pub fn physical_state(&self) -> PhysicalState {
        self.physical
    }

    pub fn allocation_state(&self) -> AllocationState {
        self.allocation_state
    }

    pub fn membership_state(&self) -> MembershipState {
        self.membership_state
    }

    pub fn state(&self) -> MemberDiskState {
        MemberDiskState::project(self.physical, self.allocation_state, self.membership_state)
    }

    pub fn block_size(&self) -> BlkSize {
        BlkSize::for_capacity(self.capacity)
    }

    pub fn total_blocks(&self) -> u64 {
        self.allocation.total_blocks()
    }

    pub fn allocated_blocks(&self) -> u64 {
        self.allocation.allocated_blocks()
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub(crate) fn apply_event(
        &mut self,
        event: MemberDiskEvent,
    ) -> RuntimeResult<MemberDiskTransition> {
        let from = self.state();
        let decision = MemberDiskMachine::transition(from, event)?;
        let (expected, effect, change) = decision.into_parts();
        if let Some(change) = change {
            self.apply_change(change);
        }
        let to = self.state();
        if to != expected {
            return Err(RuntimeError::Internal(format!(
                "state machine expected {expected:?}, MemberDisk projected {to:?}"
            )));
        }
        Ok(MemberDiskTransition {
            from,
            event,
            to,
            effect,
        })
    }

    fn apply_change(&mut self, change: MemberDiskChange) {
        match change {
            MemberDiskChange::ObservePhysical(state) => self.physical = state,
            MemberDiskChange::SetAllocation(state) if self.allocation_state != state => {
                self.allocation_state = state;
                self.revision += 1;
            }
            MemberDiskChange::SetMembership(state) if self.membership_state != state => {
                self.membership_state = state;
                self.revision += 1;
            }
            MemberDiskChange::SetAllocation(_) | MemberDiskChange::SetMembership(_) => {}
        }
    }

    pub(crate) fn allocate_one(&mut self) -> RuntimeResult<BlkId> {
        if !matches!(self.allocation_state, AllocationState::Active)
            || !matches!(self.physical, PhysicalState::Up)
            || !matches!(self.membership_state, MembershipState::Member)
        {
            return Err(RuntimeError::Rejected(
                "member disk is not currently allocatable".into(),
            ));
        }
        let blk = self.allocation.allocate_one()?;
        self.revision += 1;
        Ok(blk)
    }

    pub(crate) fn release(&mut self, blk: &BlkId) -> RuntimeResult<()> {
        self.allocation.release(blk)?;
        self.revision += 1;
        Ok(())
    }

    pub(crate) fn commit_persisted(
        &mut self,
        expected_revision: u64,
        mut next: MemberDisk,
    ) -> RuntimeResult<()> {
        if self.revision != expected_revision {
            return Err(RuntimeError::Cancelled);
        }
        // DiskMap may have updated the observation while SDB was awaited.
        // Accept the persisted decision without overwriting that newer fact.
        next.physical = self.physical;
        *self = next;
        Ok(())
    }

    pub(crate) fn can_delete(&self) -> bool {
        matches!(self.membership_state, MembershipState::Removed)
            && self.allocation.allocated_blocks() == 0
    }

    /// Strip the external observation before a persistence adapter stores the
    /// object. DiskMap will supply a fresh observation after cold restore.
    pub(crate) fn without_physical_observation(mut self) -> Self {
        self.physical = PhysicalState::Down;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn disk() -> MemberDisk {
        MemberDisk::new(
            MemberDiskId::new("md-1"),
            PhysicalDiskId::new("pd-1"),
            PoolId::new("pool-1"),
            TierId::new("tier-1"),
            MediaClass::new("ssd"),
            ByteCount::new(ONE_GIB),
            Vec::new(),
        )
    }

    #[test]
    fn persisted_commit_keeps_the_latest_physical_observation() {
        let mut disk = disk();
        disk.apply_event(MemberDiskEvent::Physical(PhysicalState::Down))
            .unwrap();
        let expected_revision = disk.revision();
        let mut draining = disk.clone();
        draining.apply_event(MemberDiskEvent::DrainStarted).unwrap();

        disk.apply_event(MemberDiskEvent::Physical(PhysicalState::Up))
            .unwrap();
        disk.commit_persisted(expected_revision, draining).unwrap();

        assert_eq!(disk.state(), MemberDiskState::Ui);
        assert_eq!(disk.physical_state(), PhysicalState::Up);
        assert_eq!(disk.allocation_state(), AllocationState::Inactive);
    }
}
