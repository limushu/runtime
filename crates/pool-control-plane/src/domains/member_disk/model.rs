use super::machine::{MemberDiskInput, MemberDiskMachine, MemberDiskTransition};
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

#[derive(Debug, Clone)]
pub(crate) struct PlannedMemberDiskMutation {
    expected_revision: u64,
    record: MemberDiskRecord,
    physical: PhysicalState,
    transition: MemberDiskTransition,
    persist_record: bool,
}

impl PlannedMemberDiskMutation {
    pub(crate) fn record(&self) -> &MemberDiskRecord {
        &self.record
    }

    pub(crate) fn requires_persistence(&self) -> bool {
        self.persist_record
    }
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedRecordUpdate {
    expected_revision: u64,
    record: MemberDiskRecord,
}

impl PlannedRecordUpdate {
    pub(crate) fn record(&self) -> &MemberDiskRecord {
        &self.record
    }
}

/// The complete in-memory MemberDisk object used by its state machine.
/// It is business state, not an actor runtime, task or mailbox.
#[derive(Debug, Clone)]
pub(crate) struct MemberDisk {
    record: MemberDiskRecord,
    physical: PhysicalState,
}

impl MemberDisk {
    pub(crate) fn restore(record: MemberDiskRecord) -> Self {
        Self {
            record,
            physical: PhysicalState::Down,
        }
    }

    pub(crate) fn snapshot(&self) -> MemberDiskSnapshot {
        let block_size = BlkSize::for_capacity(self.record.spec().capacity);
        MemberDiskSnapshot {
            spec: self.record.spec().clone(),
            physical_state: self.physical,
            allocation_state: self.record.allocation_state(),
            membership_state: self.record.membership_state(),
            operational_state: self.state(),
            block_size,
            total_blocks: self.record.allocation().total_blocks(),
            allocated_blocks: self.record.allocation().allocated_blocks(),
            revision: self.record.revision(),
        }
    }

    pub(crate) fn state(&self) -> MemberDiskState {
        MemberDiskState::project(
            self.physical,
            self.record.allocation_state(),
            self.record.membership_state(),
        )
    }

    pub(crate) fn apply_physical(
        &mut self,
        physical: PhysicalState,
    ) -> RuntimeResult<MemberDiskTransition> {
        let mutation = self.plan(MemberDiskInput::Physical(physical))?;
        self.commit(mutation)
    }

    pub(crate) fn plan_patch(&self, patch: MemberDiskPatch) -> PlannedRecordUpdate {
        let mut record = self.record.clone();
        record.apply_patch(patch);
        PlannedRecordUpdate {
            expected_revision: self.record.revision(),
            record,
        }
    }

    pub(crate) fn plan_allocate(&self) -> RuntimeResult<(PlannedRecordUpdate, BlkId)> {
        if !matches!(self.record.allocation_state(), AllocationState::Active)
            || !matches!(self.physical, PhysicalState::Up)
            || !matches!(self.record.membership_state(), MembershipState::Member)
        {
            return Err(RuntimeError::Rejected(
                "member disk is not currently allocatable".into(),
            ));
        }
        let mut record = self.record.clone();
        let blk = record.allocate_one()?;
        Ok((
            PlannedRecordUpdate {
                expected_revision: self.record.revision(),
                record,
            },
            blk,
        ))
    }

    pub(crate) fn plan_release(&self, blk: &BlkId) -> RuntimeResult<PlannedRecordUpdate> {
        let mut record = self.record.clone();
        record.release(blk)?;
        Ok(PlannedRecordUpdate {
            expected_revision: self.record.revision(),
            record,
        })
    }

    pub(crate) fn commit_record(&mut self, update: PlannedRecordUpdate) -> RuntimeResult<()> {
        if self.record.revision() != update.expected_revision {
            return Err(RuntimeError::Cancelled);
        }
        self.record = update.record;
        Ok(())
    }

    pub(crate) fn can_delete(&self) -> bool {
        matches!(self.record.membership_state(), MembershipState::Removed)
            && self.record.allocation().allocated_blocks() == 0
    }

    pub(crate) fn plan(&self, input: MemberDiskInput) -> RuntimeResult<PlannedMemberDiskMutation> {
        let transition = MemberDiskMachine::transition(self.state(), input)?;
        let mut record = self.record.clone();
        let mut physical = self.physical;
        let persist_record = match input {
            MemberDiskInput::Physical(value) => {
                physical = value;
                false
            }
            MemberDiskInput::DrainStarted => {
                record.set_allocation_state(AllocationState::Inactive);
                true
            }
            MemberDiskInput::DrainCompleted => {
                record.set_membership_state(MembershipState::Removed);
                true
            }
            MemberDiskInput::OnlineSettled => {
                record.set_allocation_state(AllocationState::Active);
                true
            }
        };
        let projected = MemberDiskState::project(
            physical,
            record.allocation_state(),
            record.membership_state(),
        );
        if projected != transition.to {
            return Err(RuntimeError::Internal(format!(
                "statechart projected {:?}, entity mutation projected {projected:?}",
                transition.to
            )));
        }
        Ok(PlannedMemberDiskMutation {
            expected_revision: self.record.revision(),
            record,
            physical,
            transition,
            persist_record,
        })
    }

    pub(crate) fn commit(
        &mut self,
        mutation: PlannedMemberDiskMutation,
    ) -> RuntimeResult<MemberDiskTransition> {
        if self.record.revision() != mutation.expected_revision {
            return Err(RuntimeError::Cancelled);
        }
        let from = self.state();
        self.record = mutation.record;
        if matches!(mutation.transition.input, MemberDiskInput::Physical(_)) {
            self.physical = mutation.physical;
        }
        let mut transition = mutation.transition;
        transition.from = from;
        transition.to = self.state();
        Ok(transition)
    }
}

#[cfg(test)]
mod object_tests {
    use super::*;

    fn disk() -> MemberDisk {
        let spec = MemberDiskSpec::new(
            MemberDiskId::new("md-1"),
            PhysicalDiskId::new("pd-1"),
            PoolId::new("pool-1"),
            TierId::new("tier-1"),
            MediaClass::new("ssd"),
            ByteCount::new(1024 * 1024 * 1024),
            Vec::new(),
        );
        MemberDisk::restore(MemberDiskRecord::new(spec))
    }

    #[test]
    fn a_stable_record_commit_keeps_the_latest_physical_observation() {
        let mut disk = disk();
        disk.apply_physical(PhysicalState::Down).unwrap();
        let drain_started = disk.plan(MemberDiskInput::DrainStarted).unwrap();
        disk.apply_physical(PhysicalState::Up).unwrap();

        let transition = disk.commit(drain_started).unwrap();
        assert_eq!(transition.to, MemberDiskState::Ui);
        assert_eq!(disk.snapshot().physical_state, PhysicalState::Up);
        assert_eq!(disk.snapshot().allocation_state, AllocationState::Inactive);
    }
}
