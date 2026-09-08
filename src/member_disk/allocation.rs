use super::{DiskUuid, MemberDiskError};

const GIB: u64 = 1024 * 1024 * 1024;
const TIB: u64 = 1024 * GIB;
const LARGE_DISK_THRESHOLD: u64 = 8 * TIB;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlkSize {
    GiB1,
    GiB2,
}

impl BlkSize {
    pub const fn for_capacity(capacity_bytes: u64) -> Self {
        if capacity_bytes > LARGE_DISK_THRESHOLD {
            Self::GiB2
        } else {
            Self::GiB1
        }
    }

    pub const fn bytes(self) -> u64 {
        match self {
            Self::GiB1 => GIB,
            Self::GiB2 => 2 * GIB,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlkId(u64);

impl BlkId {
    pub const fn new(index: u64) -> Self {
        Self(index)
    }

    pub const fn index(self) -> u64 {
        self.0
    }
}

/// One BLK selected from one Pool MemberDisk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlkRef {
    disk: DiskUuid,
    blk: BlkId,
}

impl BlkRef {
    pub fn new(disk: DiskUuid, blk: BlkId) -> Self {
        Self { disk, blk }
    }

    pub fn disk(&self) -> &DiskUuid {
        &self.disk
    }

    pub fn blk(&self) -> BlkId {
        self.blk
    }
}

/// Requests BLKs from distinct MemberDisks in one Tier.
///
/// Fault-domain selection belongs here because MemberDiskService owns both
/// the Pool member directory and its Tier topology. `fault_domain` names a
/// domain kind such as `rack`; selected disks must have distinct values for
/// that kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocateBlks {
    tier: String,
    count: usize,
    fault_domain: Option<String>,
}

impl AllocateBlks {
    pub fn new(tier: impl Into<String>, count: usize) -> Self {
        Self {
            tier: tier.into(),
            count,
            fault_domain: None,
        }
    }

    pub fn distinct_by(mut self, kind: impl Into<String>) -> Self {
        self.fault_domain = Some(kind.into());
        self
    }

    pub fn tier(&self) -> &str {
        &self.tier
    }

    pub fn count(&self) -> usize {
        self.count
    }

    pub fn fault_domain(&self) -> Option<&str> {
        self.fault_domain.as_deref()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Allocation {
    blks: Vec<BlkRef>,
}

impl Allocation {
    pub(crate) fn new(blks: Vec<BlkRef>) -> Self {
        Self { blks }
    }

    pub fn blks(&self) -> &[BlkRef] {
        &self.blks
    }

    pub fn into_blks(self) -> Vec<BlkRef> {
        self.blks
    }
}

/// Semantic BLK allocation state owned by MemberDisk.
///
/// The SDB Partition layout (`1024 × 112 bits`) is a persistence concern and
/// is deliberately not represented by this type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AllocationBitmap {
    total_blks: u64,
    words: Vec<u64>,
}

impl AllocationBitmap {
    pub(super) fn empty(total_blks: u64) -> Self {
        Self {
            total_blks,
            words: vec![0; total_blks.div_ceil(64) as usize],
        }
    }

    pub fn total_blks(&self) -> u64 {
        self.total_blks
    }

    pub fn allocated_blks(&self) -> u64 {
        self.words.iter().map(|word| word.count_ones() as u64).sum()
    }

    pub fn is_allocated(&self, blk: BlkId) -> bool {
        let index = blk.index();
        if index >= self.total_blks {
            return false;
        }
        let word = (index / 64) as usize;
        let bit = index % 64;
        self.words[word] & (1_u64 << bit) != 0
    }

    pub(super) fn allocate_one(&mut self) -> Result<BlkId, MemberDiskError> {
        let blk = self.next_free().ok_or(MemberDiskError::NoFreeBlk)?;
        self.mark_allocated(blk)?;
        Ok(blk)
    }

    pub(super) fn next_free(&self) -> Option<BlkId> {
        (0..self.total_blks)
            .map(BlkId::new)
            .find(|blk| !self.is_allocated(*blk))
    }

    pub(super) fn mark_allocated(&mut self, blk: BlkId) -> Result<(), MemberDiskError> {
        let index = blk.index();
        if index >= self.total_blks {
            return Err(MemberDiskError::BlkOutOfRange {
                blk,
                total_blks: self.total_blks,
            });
        }
        if self.is_allocated(blk) {
            return Err(MemberDiskError::BlkAlreadyAllocated { blk });
        }
        let word = (index / 64) as usize;
        let bit = index % 64;
        self.words[word] |= 1_u64 << bit;
        Ok(())
    }

    pub(super) fn release(&mut self, blk: BlkId) -> Result<(), MemberDiskError> {
        let index = blk.index();
        if index >= self.total_blks {
            return Err(MemberDiskError::BlkOutOfRange {
                blk,
                total_blks: self.total_blks,
            });
        }
        if !self.is_allocated(blk) {
            return Err(MemberDiskError::BlkNotAllocated { blk });
        }
        let word = (index / 64) as usize;
        let bit = index % 64;
        self.words[word] &= !(1_u64 << bit);
        Ok(())
    }
}
