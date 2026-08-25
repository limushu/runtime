use std::{
    collections::{HashMap, HashSet},
    sync::Mutex,
};

use super::{DemoError, DiskId, DiskSnapshot, DiskState};

pub(super) struct DiskMetadata {
    disks: Mutex<HashMap<DiskId, DiskState>>,
}

impl DiskMetadata {
    pub(super) fn new(disks: impl IntoIterator<Item = DiskId>) -> Self {
        Self {
            disks: Mutex::new(
                disks
                    .into_iter()
                    .map(|disk| (disk, DiskState::Online))
                    .collect(),
            ),
        }
    }

    pub(super) fn query(&self, disk: &DiskId) -> Option<DiskSnapshot> {
        self.disks
            .lock()
            .expect("disk metadata poisoned")
            .get(disk)
            .copied()
            .map(|state| DiskSnapshot {
                disk: disk.clone(),
                state,
            })
    }

    pub(super) fn set(&self, disk: &DiskId, state: DiskState) -> Result<(), DemoError> {
        let mut disks = self.disks.lock().expect("disk metadata poisoned");
        let current = disks
            .get_mut(disk)
            .ok_or_else(|| DemoError::DiskNotFound(disk.clone()))?;
        *current = state;
        Ok(())
    }
}

pub(super) struct ActiveDisks {
    disks: Mutex<HashSet<DiskId>>,
}

impl ActiveDisks {
    pub(super) fn new() -> Self {
        Self {
            disks: Mutex::new(HashSet::new()),
        }
    }

    pub(super) fn begin(&self, disk: DiskId) {
        self.disks
            .lock()
            .expect("active metadata poisoned")
            .insert(disk);
    }

    pub(super) fn finish(&self, disk: &DiskId) {
        self.disks
            .lock()
            .expect("active metadata poisoned")
            .remove(disk);
    }
}
