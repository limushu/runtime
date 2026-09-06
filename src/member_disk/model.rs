use super::{AllocationBitmap, BlkId, BlkSize};
use std::fmt;

/// The one stable disk identity used by DiskMap, Pool and BG entries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DiskUuid(String);

impl DiskUuid {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DiskUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A physical disk after it joins one Pool.
///
/// This is the only MemberDisk business object. Runtime tasks, workflows and
/// state-machine execution data must not be stored here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDisk {
    uuid: DiskUuid,
    pool_id: String,
    tier_id: String,
    media_class: String,
    capacity_bytes: u64,
    failure_domains: Vec<FailureDomain>,

    // Durable management intent produced by a Shrink request. Physical UP or
    // DOWN facts do not erase it while removal is in progress.
    shrink_requested: bool,

    // Effective IO state used by Pool decisions. Down takes effect
    // conservatively as soon as an offline fact is accepted. Up takes effect
    // only after every serviceable Pool node opened the disk and accepted the
    // Up state update.
    io_state: DiskIoState,

    // Pool decisions persisted by the MemberDisk domain.
    allocation_state: AllocationState,
    membership_state: MembershipState,
    allocation_bitmap: AllocationBitmap,
}

impl MemberDisk {
    /// Creates a new Pool member in Down state with all BLKs free.
    pub fn new(
        uuid: DiskUuid,
        pool_id: impl Into<String>,
        tier_id: impl Into<String>,
        media_class: impl Into<String>,
        capacity_bytes: u64,
        failure_domains: Vec<FailureDomain>,
    ) -> Result<Self, MemberDiskError> {
        let blk_size = BlkSize::for_capacity(capacity_bytes);
        let total_blks = capacity_bytes / blk_size.bytes();
        if total_blks == 0 {
            return Err(MemberDiskError::CapacityTooSmall {
                capacity_bytes,
                minimum_bytes: blk_size.bytes(),
            });
        }

        Ok(Self {
            uuid,
            pool_id: pool_id.into(),
            tier_id: tier_id.into(),
            media_class: media_class.into(),
            capacity_bytes,
            failure_domains,
            shrink_requested: false,
            io_state: DiskIoState::Down,
            allocation_state: AllocationState::Active,
            membership_state: MembershipState::Member,
            allocation_bitmap: AllocationBitmap::empty(total_blks),
        })
    }

    pub fn uuid(&self) -> &DiskUuid {
        &self.uuid
    }

    pub fn pool_id(&self) -> &str {
        &self.pool_id
    }

    pub fn tier_id(&self) -> &str {
        &self.tier_id
    }

    pub fn media_class(&self) -> &str {
        &self.media_class
    }

    pub fn capacity_bytes(&self) -> u64 {
        self.capacity_bytes
    }

    pub fn failure_domains(&self) -> &[FailureDomain] {
        &self.failure_domains
    }

    pub fn io_state(&self) -> DiskIoState {
        self.io_state
    }

    pub fn shrink_requested(&self) -> bool {
        self.shrink_requested
    }

    pub fn allocation_state(&self) -> AllocationState {
        self.allocation_state
    }

    pub fn membership_state(&self) -> MembershipState {
        self.membership_state
    }

    pub fn allocation_bitmap(&self) -> &AllocationBitmap {
        &self.allocation_bitmap
    }

    pub fn blk_size(&self) -> BlkSize {
        BlkSize::for_capacity(self.capacity_bytes)
    }

    /// Derived operational state. It is never stored as another source of
    /// truth.
    pub fn state(&self) -> MemberDiskState {
        MemberDiskState::project(self.io_state, self.allocation_state, self.membership_state)
    }

    pub fn can_serve_io(&self) -> bool {
        matches!(self.io_state, DiskIoState::Up)
            && matches!(self.membership_state, MembershipState::Member)
    }

    pub fn can_allocate(&self) -> bool {
        self.can_serve_io() && matches!(self.allocation_state, AllocationState::Active)
    }

    /// Commits the result of the completed two-stage user_dp online workflow.
    pub fn mark_io_up(&mut self) -> Result<bool, MemberDiskError> {
        self.ensure_member()?;
        if self.io_state == DiskIoState::Up {
            return Ok(false);
        }
        self.io_state = DiskIoState::Up;
        Ok(true)
    }

    /// Commits a completed online workflow as one object transition.
    ///
    /// Opening the disk and publishing UP to user_dp happen before this call.
    /// Keeping this transition on `MemberDisk` prevents the service from
    /// knowing which metadata fields together form the online steady state.
    pub fn complete_online(&mut self) -> Result<bool, MemberDiskError> {
        self.apply_locally(MemberDiskUpdate::CompleteOnline)
    }

    /// Prevents new BLK allocation while preserving existing BLKs.
    pub fn disable_allocation(&mut self) -> Result<bool, MemberDiskError> {
        self.apply_locally(MemberDiskUpdate::DisableAllocation)
    }

    /// Allows allocation again. While IO is Down this produces `DownActive`,
    /// but allocation remains impossible until the online workflow completes.
    pub fn enable_allocation(&mut self) -> Result<bool, MemberDiskError> {
        self.ensure_member()?;
        if matches!(self.allocation_state, AllocationState::Active) {
            return Ok(false);
        }
        self.allocation_state = AllocationState::Active;
        Ok(true)
    }

    pub fn allocate_blk(&mut self) -> Result<BlkId, MemberDiskError> {
        if !self.can_allocate() {
            return Err(MemberDiskError::NotAllocatable {
                state: self.state(),
            });
        }
        self.allocation_bitmap.allocate_one()
    }

    /// Releases an existing BLK even while the disk is Down or Inactive.
    pub fn release_blk(&mut self, blk: BlkId) -> Result<(), MemberDiskError> {
        self.ensure_member()?;
        self.allocation_bitmap.release(blk)
    }

    /// Removes the disk after allocation has stopped and every BLK was returned.
    pub fn remove(&mut self) -> Result<(), MemberDiskError> {
        self.apply_locally(MemberDiskUpdate::Remove)?;
        Ok(())
    }

    /// Restores a previously removed member as an online allocatable disk.
    /// Opening the disk and publishing UP happen before this transition.
    pub fn rejoin(&mut self) -> Result<bool, MemberDiskError> {
        self.apply_locally(MemberDiskUpdate::Rejoin)
    }

    /// Checks one durable update without changing the in-memory object.
    ///
    /// `MemberDiskService` calls this before submitting the same update to the
    /// metadata service. Keeping validation here preserves MemberDisk's domain
    /// invariants while allowing SDB to be committed before memory changes.
    pub(crate) fn validate_update(
        &self,
        update: &MemberDiskUpdate,
    ) -> Result<bool, MemberDiskError> {
        if matches!(update, MemberDiskUpdate::Rejoin) {
            return Ok(matches!(self.membership_state, MembershipState::Removed));
        }

        if matches!(update, MemberDiskUpdate::RequestShrink)
            && matches!(self.membership_state, MembershipState::Removed)
        {
            return Ok(false);
        }

        self.ensure_member()?;

        match update {
            MemberDiskUpdate::RequestShrink => Ok(!self.shrink_requested),
            MemberDiskUpdate::ApplyDown => Ok(self.io_state != DiskIoState::Down),
            MemberDiskUpdate::CompleteOnline => Ok(self.io_state != DiskIoState::Up
                || self.allocation_state != AllocationState::Active),
            MemberDiskUpdate::DisableAllocation => {
                Ok(self.allocation_state != AllocationState::Inactive)
            }
            MemberDiskUpdate::Remove => {
                if matches!(self.allocation_state, AllocationState::Active) {
                    return Err(MemberDiskError::AllocationStillActive);
                }
                let allocated_blks = self.allocation_bitmap.allocated_blks();
                if allocated_blks != 0 {
                    return Err(MemberDiskError::BlksStillAllocated { allocated_blks });
                }
                Ok(true)
            }
            MemberDiskUpdate::Rejoin => Ok(false),
        }
    }

    /// Applies an update that the metadata service has already committed.
    ///
    /// This method is deliberately infallible: every domain check happened in
    /// `validate_update` before the SDB write.
    pub(crate) fn apply_committed(&mut self, update: &MemberDiskUpdate) {
        debug_assert!(self.validate_update(update).is_ok());

        match update {
            MemberDiskUpdate::RequestShrink => {
                self.shrink_requested = true;
            }
            MemberDiskUpdate::ApplyDown => {
                self.io_state = DiskIoState::Down;
            }
            MemberDiskUpdate::CompleteOnline => {
                self.io_state = DiskIoState::Up;
                self.allocation_state = AllocationState::Active;
            }
            MemberDiskUpdate::DisableAllocation => {
                self.allocation_state = AllocationState::Inactive;
            }
            MemberDiskUpdate::Remove => {
                self.io_state = DiskIoState::Down;
                self.membership_state = MembershipState::Removed;
            }
            MemberDiskUpdate::Rejoin => {
                self.io_state = DiskIoState::Up;
                self.allocation_state = AllocationState::Active;
                self.membership_state = MembershipState::Member;
                self.shrink_requested = false;
            }
        }
    }

    fn apply_locally(&mut self, update: MemberDiskUpdate) -> Result<bool, MemberDiskError> {
        let changed = self.validate_update(&update)?;
        if changed {
            self.apply_committed(&update);
        }
        Ok(changed)
    }

    fn ensure_member(&self) -> Result<(), MemberDiskError> {
        if matches!(self.membership_state, MembershipState::Removed) {
            Err(MemberDiskError::AlreadyRemoved)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureDomain {
    pub kind: String,
    pub id: String,
}

impl FailureDomain {
    pub fn new(kind: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            id: id.into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskIoState {
    Up,
    Down,
}

/// A field-level, durable change to one MemberDisk.
///
/// Workflows submit this intent to the metadata service instead of passing a
/// cloned MemberDisk snapshot across the persistence boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskUpdate {
    RequestShrink,
    ApplyDown,
    CompleteOnline,
    DisableAllocation,
    Remove,
    Rejoin,
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

/// Read-only projection of the three authoritative state dimensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskState {
    UpActive,
    DownActive,
    DownInactive,
    UpInactive,
    Removed,
}

impl MemberDiskState {
    pub const fn project(
        io: DiskIoState,
        allocation: AllocationState,
        membership: MembershipState,
    ) -> Self {
        if matches!(membership, MembershipState::Removed) {
            return Self::Removed;
        }

        match (io, allocation) {
            (DiskIoState::Up, AllocationState::Active) => Self::UpActive,
            (DiskIoState::Down, AllocationState::Active) => Self::DownActive,
            (DiskIoState::Down, AllocationState::Inactive) => Self::DownInactive,
            (DiskIoState::Up, AllocationState::Inactive) => Self::UpInactive,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskError {
    CapacityTooSmall {
        capacity_bytes: u64,
        minimum_bytes: u64,
    },
    NotAllocatable {
        state: MemberDiskState,
    },
    NoFreeBlk,
    BlkOutOfRange {
        blk: BlkId,
        total_blks: u64,
    },
    BlkNotAllocated {
        blk: BlkId,
    },
    AllocationStillActive,
    BlksStillAllocated {
        allocated_blks: u64,
    },
    AlreadyRemoved,
}

impl fmt::Display for MemberDiskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CapacityTooSmall {
                capacity_bytes,
                minimum_bytes,
            } => write!(
                f,
                "member disk capacity {capacity_bytes} is smaller than one BLK ({minimum_bytes})"
            ),
            Self::NotAllocatable { state } => {
                write!(f, "member disk is not allocatable in state {state:?}")
            }
            Self::NoFreeBlk => write!(f, "member disk has no free BLK"),
            Self::BlkOutOfRange { blk, total_blks } => write!(
                f,
                "BLK {} is outside member disk range 0..{total_blks}",
                blk.index()
            ),
            Self::BlkNotAllocated { blk } => {
                write!(f, "BLK {} is not allocated", blk.index())
            }
            Self::AllocationStillActive => {
                write!(f, "allocation must be disabled before removing member disk")
            }
            Self::BlksStillAllocated { allocated_blks } => {
                write!(f, "member disk still owns {allocated_blks} allocated BLKs")
            }
            Self::AlreadyRemoved => write!(f, "member disk has already been removed"),
        }
    }
}

impl std::error::Error for MemberDiskError {}
