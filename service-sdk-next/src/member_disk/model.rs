use std::{fmt, sync::Arc};

use super::MemberDiskError;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DiskUuid(Arc<str>);

impl DiskUuid {
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }
}

impl From<&str> for DiskUuid {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl fmt::Display for DiskUuid {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoState {
    Up,
    Down,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocationState {
    Active,
    Inactive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskState {
    UpActive,
    UpInactive,
    DownActive,
    DownInactive,
    Removed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AllocatedBlock {
    pub disk: DiskUuid,
    pub block: BlockId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskChange {
    SetIo(IoState),
    DisableAllocation,
    Remove,
    BringOnline,
    Allocate(Vec<BlockId>),
    Release(Vec<BlockId>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDisk {
    uuid: DiskUuid,
    pool: Arc<str>,
    io: IoState,
    allocation: AllocationState,
    removed: bool,
    blocks: Vec<bool>,
}

impl MemberDisk {
    pub fn new(uuid: DiskUuid, pool: impl Into<Arc<str>>, block_count: u32) -> Self {
        Self {
            uuid,
            pool: pool.into(),
            io: IoState::Up,
            allocation: AllocationState::Active,
            removed: false,
            blocks: vec![false; block_count as usize],
        }
    }

    pub fn uuid(&self) -> &DiskUuid {
        &self.uuid
    }

    pub fn pool(&self) -> &str {
        &self.pool
    }

    pub fn state(&self) -> MemberDiskState {
        if self.removed {
            return MemberDiskState::Removed;
        }

        match (self.io, self.allocation) {
            (IoState::Up, AllocationState::Active) => MemberDiskState::UpActive,
            (IoState::Up, AllocationState::Inactive) => MemberDiskState::UpInactive,
            (IoState::Down, AllocationState::Active) => MemberDiskState::DownActive,
            (IoState::Down, AllocationState::Inactive) => MemberDiskState::DownInactive,
        }
    }

    pub fn free_blocks(&self) -> usize {
        self.blocks.iter().filter(|allocated| !**allocated).count()
    }

    pub fn allocated_blocks(&self) -> impl Iterator<Item = BlockId> + '_ {
        self.blocks
            .iter()
            .enumerate()
            .filter(|(_, allocated)| **allocated)
            .map(|(index, _)| BlockId(index as u32))
    }

    pub(crate) fn first_free_blocks(&self, count: usize) -> Vec<BlockId> {
        if self.state() != MemberDiskState::UpActive {
            return Vec::new();
        }

        self.blocks
            .iter()
            .enumerate()
            .filter(|(_, allocated)| !**allocated)
            .take(count)
            .map(|(index, _)| BlockId(index as u32))
            .collect()
    }

    pub(crate) fn validate(&self, change: &MemberDiskChange) -> Result<(), MemberDiskError> {
        match change {
            MemberDiskChange::SetIo(_) => {
                if self.removed {
                    return Err(MemberDiskError::InvalidState {
                        disk: self.uuid.clone(),
                        reason: "removed disks must use BringOnline",
                    });
                }
            }
            MemberDiskChange::DisableAllocation => {
                if self.removed {
                    return Err(MemberDiskError::InvalidState {
                        disk: self.uuid.clone(),
                        reason: "a removed disk cannot change allocation state",
                    });
                }
            }
            MemberDiskChange::Remove => {
                if self.allocation != AllocationState::Inactive {
                    return Err(MemberDiskError::InvalidState {
                        disk: self.uuid.clone(),
                        reason: "allocation must be disabled before removal",
                    });
                }
            }
            MemberDiskChange::BringOnline => {}
            MemberDiskChange::Allocate(blocks) => {
                if self.state() != MemberDiskState::UpActive {
                    return Err(MemberDiskError::InvalidState {
                        disk: self.uuid.clone(),
                        reason: "only UP/ACTIVE disks can allocate blocks",
                    });
                }
                for block in blocks {
                    match self.blocks.get(block.0 as usize) {
                        Some(false) => {}
                        Some(true) => {
                            return Err(MemberDiskError::InvalidState {
                                disk: self.uuid.clone(),
                                reason: "block is already allocated",
                            });
                        }
                        None => return Err(MemberDiskError::InvalidRequest("unknown block")),
                    }
                }
            }
            MemberDiskChange::Release(blocks) => {
                for block in blocks {
                    match self.blocks.get(block.0 as usize) {
                        Some(true) => {}
                        Some(false) => {
                            return Err(MemberDiskError::InvalidState {
                                disk: self.uuid.clone(),
                                reason: "block is already free",
                            });
                        }
                        None => return Err(MemberDiskError::InvalidRequest("unknown block")),
                    }
                }
            }
        }

        Ok(())
    }

    pub(crate) fn apply_committed(&mut self, change: MemberDiskChange) {
        match change {
            MemberDiskChange::SetIo(io) => self.io = io,
            MemberDiskChange::DisableAllocation => {
                self.allocation = AllocationState::Inactive;
            }
            MemberDiskChange::Remove => {
                self.io = IoState::Down;
                self.allocation = AllocationState::Inactive;
                self.removed = true;
                // VirtualDisks::evacuate has already removed every BG
                // reference. Publishing Remove therefore retires the old BLK
                // ownership as one committed domain change.
                self.blocks.fill(false);
            }
            MemberDiskChange::BringOnline => {
                self.io = IoState::Up;
                self.allocation = AllocationState::Active;
                self.removed = false;
            }
            MemberDiskChange::Allocate(blocks) => {
                for block in blocks {
                    self.blocks[block.0 as usize] = true;
                }
            }
            MemberDiskChange::Release(blocks) => {
                for block in blocks {
                    self.blocks[block.0 as usize] = false;
                }
            }
        }
    }
}
