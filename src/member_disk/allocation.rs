use super::MemberDiskError;

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
        for index in 0..self.total_blks {
            let blk = BlkId::new(index);
            if self.is_allocated(blk) {
                continue;
            }
            let word = (index / 64) as usize;
            let bit = index % 64;
            self.words[word] |= 1_u64 << bit;
            return Ok(blk);
        }
        Err(MemberDiskError::NoFreeBlk)
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
