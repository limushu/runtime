use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::{oneshot, watch};

use super::{BgId, BgSnapshot, DemoError, DiskId, RebuildResult};

#[derive(Clone)]
pub struct DemoCatalog {
    disk_bgs: Arc<HashMap<DiskId, Vec<BgId>>>,
    delay: Duration,
}

impl DemoCatalog {
    pub fn new(disk_bgs: impl IntoIterator<Item = (DiskId, Vec<BgId>)>, delay: Duration) -> Self {
        Self {
            disk_bgs: Arc::new(disk_bgs.into_iter().collect()),
            delay,
        }
    }

    pub async fn resolve(&self, disk: &DiskId) -> Vec<BgId> {
        tokio::time::sleep(self.delay).await;
        self.disk_bgs.get(disk).cloned().unwrap_or_default()
    }
}

#[derive(Clone)]
pub struct BgBackend {
    step_delay: Duration,
    history: Arc<Mutex<BgSnapshot>>,
}

impl BgBackend {
    pub fn new(step_delay: Duration) -> Self {
        Self {
            step_delay,
            history: Arc::new(Mutex::new(BgSnapshot {
                started: Vec::new(),
                completed: Vec::new(),
                cancelled: Vec::new(),
            })),
        }
    }

    pub fn snapshot(&self) -> BgSnapshot {
        self.history.lock().expect("history lock poisoned").clone()
    }

    pub(crate) async fn rebuild(&self, bg: BgId) {
        self.history
            .lock()
            .expect("history lock poisoned")
            .started
            .push(bg.clone());
        // The three real business stages: remap, node rebuild, metadata commit.
        for _ in 0..3 {
            tokio::time::sleep(self.step_delay).await;
        }
        self.history
            .lock()
            .expect("history lock poisoned")
            .completed
            .push(bg);
    }

    pub(crate) fn record_cancelled(&self, bg: BgId) {
        self.history
            .lock()
            .expect("history lock poisoned")
            .cancelled
            .push(bg);
    }
}

pub(crate) struct EventState {
    pub activity_changes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DiskPhase {
    Resolving,
    Active,
}

pub(crate) struct DiskJob {
    pub phase: DiskPhase,
    pub pending: HashSet<BgId>,
    pub completed: Option<oneshot::Sender<RebuildResult>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BgPhase {
    Queued,
    Running,
}

pub(crate) struct BgJob {
    pub phase: BgPhase,
    pub owners: HashSet<DiskId>,
}

pub(crate) struct RebuildState {
    pub catalog: DemoCatalog,
    pub window: usize,
    pub suspended: bool,
    pub disks: HashMap<DiskId, DiskJob>,
    pub bgs: HashMap<BgId, BgJob>,
    pub queue: VecDeque<BgId>,
    pub running: usize,
    pub suspend_reply: Option<oneshot::Sender<Result<(), DemoError>>>,
    pub campaign_stop: Option<watch::Sender<bool>>,
    pub campaigns_started: usize,
    pub bg_delegations_started: usize,
    pub activity_changes: usize,
}

pub(crate) struct BgState {
    pub backend: BgBackend,
    pub activity_changes: usize,
}
