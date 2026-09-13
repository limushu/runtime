use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::runtime::CommandId;

use super::{
    AllocatedBlock, DiskUuid, MemberDisk, MemberDiskError, RequestSnapshot, RequestStatus,
    state_machine::{Action, DiskIntent, DiskIntentKind},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct RequestId(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RequestKind {
    Disk {
        disk: DiskUuid,
        intent: DiskIntent,
    },
    Allocate {
        requested: usize,
        allocated: Vec<AllocatedBlock>,
    },
    Release {
        remaining: Vec<AllocatedBlock>,
    },
}

impl RequestKind {
    pub fn disk(&self) -> Option<&DiskUuid> {
        match self {
            Self::Disk { disk, .. } => Some(disk),
            Self::Allocate { .. } | Self::Release { .. } => None,
        }
    }

    pub fn intent(&self) -> Option<DiskIntentKind> {
        match self {
            Self::Disk { intent, .. } => Some(intent.kind()),
            Self::Allocate { .. } | Self::Release { .. } => None,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum RequestPhase {
    Waiting,
    Ready,
    Running(Action),
    Retrying { action: Action, at: Instant },
    Cancelling(Option<Action>),
}

#[derive(Debug, Clone)]
pub(crate) struct RequestRecord {
    pub id: RequestId,
    pub command_id: CommandId,
    pub kind: RequestKind,
    pub phase: RequestPhase,
    pub cancel: CancellationToken,
    pub attempts: u32,
}

impl RequestRecord {
    fn snapshot(&self) -> RequestSnapshot {
        let status = match self.phase {
            RequestPhase::Waiting => RequestStatus::Waiting,
            RequestPhase::Ready => RequestStatus::Ready,
            RequestPhase::Running(action) => RequestStatus::Running {
                action: action.name(),
            },
            RequestPhase::Retrying { action, .. } => RequestStatus::Retrying {
                action: action.name(),
            },
            RequestPhase::Cancelling(action) => RequestStatus::Cancelling {
                action: action.map(Action::name),
            },
        };

        RequestSnapshot {
            id: self.id.0,
            command_id: self.command_id,
            disk: self.kind.disk().cloned(),
            status,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum RequestOutput {
    Disk(MemberDisk),
    Allocated(Vec<AllocatedBlock>),
    Released,
}

#[derive(Debug)]
pub struct RequestCompletion {
    pub(crate) request: RequestId,
    pub(crate) result: StepResult,
}

#[derive(Debug)]
pub(crate) enum StepResult {
    Progress(Vec<CommittedChange>),
    Retry(MemberDiskError),
    Cancelled,
    Failed(MemberDiskError),
}

#[derive(Debug, Clone)]
pub(crate) struct CommittedChange {
    pub disk: DiskUuid,
    pub change: super::MemberDiskChange,
}

pub(crate) struct RequestTable {
    next_id: u64,
    records: HashMap<RequestId, RequestRecord>,
    waiting: VecDeque<RequestId>,
    ready: VecDeque<RequestId>,
    current_disks: HashMap<DiskUuid, RequestId>,
    allocation_current: Option<RequestId>,
}

impl Default for RequestTable {
    fn default() -> Self {
        Self {
            next_id: 1,
            records: HashMap::new(),
            waiting: VecDeque::new(),
            ready: VecDeque::new(),
            current_disks: HashMap::new(),
            allocation_current: None,
        }
    }
}

impl RequestTable {
    pub fn push(
        &mut self,
        command_id: CommandId,
        kind: RequestKind,
        cancel: CancellationToken,
    ) -> RequestId {
        let id = RequestId(self.next_id);
        self.next_id += 1;
        self.records.insert(
            id,
            RequestRecord {
                id,
                command_id,
                kind,
                phase: RequestPhase::Waiting,
                cancel,
                attempts: 0,
            },
        );
        self.waiting.push_back(id);
        id
    }

    pub fn waiting_round(&mut self) -> VecDeque<RequestId> {
        std::mem::take(&mut self.waiting)
    }

    pub fn wait(&mut self, id: RequestId) {
        if self.records.contains_key(&id) {
            self.waiting.push_back(id);
        }
    }

    pub fn make_ready(&mut self, id: RequestId) {
        if let Some(request) = self.records.get_mut(&id) {
            request.phase = RequestPhase::Ready;
            self.ready.push_back(id);
        }
    }

    pub fn take_ready(&mut self) -> Vec<RequestId> {
        self.ready.drain(..).collect()
    }

    pub fn mark_running(&mut self, ids: &[RequestId], action: Action) {
        for id in ids {
            if let Some(request) = self.records.get_mut(id) {
                request.phase = RequestPhase::Running(action);
            }
        }
    }

    pub fn retry(&mut self, id: RequestId, action: Action, after: Duration) {
        if let Some(request) = self.records.get_mut(&id) {
            request.attempts += 1;
            request.phase = RequestPhase::Retrying {
                action,
                at: Instant::now() + after,
            };
        }
    }

    pub fn activate_due_retries(&mut self, now: Instant) {
        let due: Vec<_> = self
            .records
            .iter()
            .filter_map(|(id, request)| match request.phase {
                RequestPhase::Retrying { at, .. } if at <= now => Some(*id),
                _ => None,
            })
            .collect();

        for id in due {
            self.make_ready(id);
        }
    }

    pub fn next_retry(&self) -> Option<Instant> {
        self.records
            .values()
            .filter_map(|request| match request.phase {
                RequestPhase::Retrying { at, .. } => Some(at),
                _ => None,
            })
            .min()
    }

    pub fn claim(&mut self, id: RequestId) -> bool {
        let Some(request) = self.records.get(&id) else {
            return false;
        };

        match &request.kind {
            RequestKind::Disk { disk, .. } => {
                if self.allocation_current.is_some() || self.current_disks.contains_key(disk) {
                    return false;
                }
                self.current_disks.insert(disk.clone(), id);
            }
            RequestKind::Allocate { .. } | RequestKind::Release { .. } => {
                if self.allocation_current.is_some() || !self.current_disks.is_empty() {
                    return false;
                }
                self.allocation_current = Some(id);
            }
        }

        self.make_ready(id);
        true
    }

    pub fn current_for_disk(&self, disk: &DiskUuid) -> Option<RequestId> {
        self.current_disks.get(disk).copied()
    }

    pub fn allocation_current(&self) -> Option<RequestId> {
        self.allocation_current
    }

    pub fn get(&self, id: RequestId) -> Option<&RequestRecord> {
        self.records.get(&id)
    }

    /// Advances request-local progress after SDB has accepted the change.
    ///
    /// Allocation and release may span several MemberDisk records. Keeping the
    /// completed portion on the request means a retry never repeats work that
    /// SDB has already committed.
    pub fn record_committed(&mut self, id: RequestId, changes: &[CommittedChange]) {
        let Some(request) = self.records.get_mut(&id) else {
            return;
        };

        match &mut request.kind {
            RequestKind::Allocate { allocated, .. } => {
                for committed in changes {
                    if let super::MemberDiskChange::Allocate(blocks) = &committed.change {
                        allocated.extend(blocks.iter().map(|block| AllocatedBlock {
                            disk: committed.disk.clone(),
                            block: *block,
                        }));
                    }
                }
            }
            RequestKind::Release { remaining } => {
                for committed in changes {
                    if let super::MemberDiskChange::Release(blocks) = &committed.change {
                        remaining.retain(|candidate| {
                            candidate.disk != committed.disk || !blocks.contains(&candidate.block)
                        });
                    }
                }
            }
            RequestKind::Disk { .. } => {}
        }
    }

    pub fn cancel(&mut self, id: RequestId) {
        let Some(request) = self.records.get_mut(&id) else {
            return;
        };

        let action = match request.phase {
            RequestPhase::Running(action) | RequestPhase::Retrying { action, .. } => Some(action),
            RequestPhase::Cancelling(action) => action,
            RequestPhase::Waiting | RequestPhase::Ready => None,
        };
        request.phase = RequestPhase::Cancelling(action);
        request.cancel.cancel();
    }

    pub fn is_cancelled(&self, id: RequestId) -> bool {
        self.records
            .get(&id)
            .is_some_and(|request| request.cancel.is_cancelled())
    }

    pub fn remove(&mut self, id: RequestId) -> Option<RequestRecord> {
        let request = self.records.remove(&id)?;
        match &request.kind {
            RequestKind::Disk { disk, .. } => {
                if self.current_disks.get(disk) == Some(&id) {
                    self.current_disks.remove(disk);
                }
            }
            RequestKind::Allocate { .. } | RequestKind::Release { .. } => {
                if self.allocation_current == Some(id) {
                    self.allocation_current = None;
                }
            }
        }
        Some(request)
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn snapshots(&self) -> Vec<RequestSnapshot> {
        let mut snapshots: Vec<_> = self.records.values().map(RequestRecord::snapshot).collect();
        snapshots.sort_by_key(|snapshot| snapshot.id);
        snapshots
    }
}
