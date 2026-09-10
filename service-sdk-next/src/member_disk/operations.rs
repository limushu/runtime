use super::{DiskUuid, MemberDiskServiceError, MemberDiskState};
use crate::service::{TaskSnapshot, TaskState};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

pub(super) type DiskResult = Result<MemberDiskState, MemberDiskServiceError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OperationKind {
    Offline,
    Online,
    Shrink,
}

impl OperationKind {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Offline => "offline",
            Self::Online => "online",
            Self::Shrink => "shrink",
        }
    }
}

pub(super) struct OperationTable {
    inner: Arc<OperationTableInner>,
}

struct OperationTableInner {
    next_id: AtomicU64,
    active: Mutex<HashMap<DiskUuid, ActiveOperation>>,
}

struct ActiveOperation {
    id: u64,
    execution: u64,
    kind: OperationKind,
    cancel: CancellationToken,
    completed: watch::Sender<Option<DiskResult>>,
    progress: u8,
    detail: String,
}

pub(super) struct OperationPlan {
    pub(super) start: Vec<OperationPermit>,
    pub(super) wait: Vec<OperationWait>,
}

pub(super) struct OperationPermit {
    pub(super) id: u64,
    pub(super) disk: DiskUuid,
    pub(super) cancel: CancellationToken,
    owner: Weak<OperationTableInner>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WaitReason {
    Join,
    Retry,
}

pub(super) struct OperationWait {
    pub(super) disk: DiskUuid,
    pub(super) reason: WaitReason,
    completed: watch::Receiver<Option<DiskResult>>,
}

impl OperationTable {
    pub(super) fn new() -> Self {
        Self {
            inner: Arc::new(OperationTableInner {
                next_id: AtomicU64::new(1),
                active: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Makes one decision for every disk in the incoming batch.
    ///
    /// Different disks remain independent. An identical operation joins the
    /// running one. A conflicting operation waits for the old Future to reach
    /// a stable boundary; most conflicts request cooperative cancellation.
    pub(super) fn plan(
        &self,
        disks: &[DiskUuid],
        incoming: OperationKind,
        execution: u64,
        parent_cancel: &CancellationToken,
    ) -> OperationPlan {
        let mut active = self.inner.active.lock().expect("operation table poisoned");
        let mut start = Vec::new();
        let mut wait = Vec::new();

        for disk in disks {
            if let Some(current) = active.get_mut(disk) {
                if current.kind == incoming {
                    wait.push(OperationWait {
                        disk: disk.clone(),
                        reason: WaitReason::Join,
                        completed: current.completed.subscribe(),
                    });
                    continue;
                }

                if should_cancel(current.kind, incoming) {
                    current.cancel.cancel();
                    current.detail = format!("yielding to {}", incoming.name());
                }
                wait.push(OperationWait {
                    disk: disk.clone(),
                    reason: WaitReason::Retry,
                    completed: current.completed.subscribe(),
                });
                continue;
            }

            let id = self.inner.next_id.fetch_add(1, Ordering::Relaxed);
            let cancel = parent_cancel.child_token();
            let (completed, _) = watch::channel(None);
            active.insert(
                disk.clone(),
                ActiveOperation {
                    id,
                    execution,
                    kind: incoming,
                    cancel: cancel.clone(),
                    completed,
                    progress: 0,
                    detail: "accepted".into(),
                },
            );
            start.push(OperationPermit {
                id,
                disk: disk.clone(),
                cancel,
                owner: Arc::downgrade(&self.inner),
            });
        }

        OperationPlan { start, wait }
    }

    pub(super) fn progress(
        &self,
        permit: &OperationPermit,
        progress: u8,
        detail: impl Into<String>,
    ) {
        let mut active = self.inner.active.lock().expect("operation table poisoned");
        if let Some(operation) = active.get_mut(&permit.disk)
            && operation.id == permit.id
        {
            operation.progress = progress.min(100);
            operation.detail = detail.into();
        }
    }

    pub(super) fn finish(&self, permit: OperationPermit, result: DiskResult) {
        let operation = {
            let mut active = self.inner.active.lock().expect("operation table poisoned");
            match active.get(&permit.disk) {
                Some(operation) if operation.id == permit.id => active.remove(&permit.disk),
                _ => None,
            }
        };
        if let Some(operation) = operation {
            operation.completed.send_replace(Some(result));
        }
    }

    pub(super) fn snapshots(&self) -> Vec<TaskSnapshot> {
        let mut tasks: Vec<_> = self
            .inner
            .active
            .lock()
            .expect("operation table poisoned")
            .iter()
            .map(|(disk, operation)| TaskSnapshot {
                id: format!("{}:{}", operation.execution, operation.id),
                kind: operation.kind.name().into(),
                subject: disk.to_string(),
                state: if operation.cancel.is_cancelled() {
                    TaskState::Cancelling
                } else {
                    TaskState::Running
                },
                progress: Some(operation.progress),
                detail: Some(operation.detail.clone()),
            })
            .collect();
        tasks.sort_by(|left, right| left.id.cmp(&right.id));
        tasks
    }
}

impl Drop for OperationPermit {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        let operation = {
            let mut active = owner.active.lock().expect("operation table poisoned");
            match active.get(&self.disk) {
                Some(operation) if operation.id == self.id => active.remove(&self.disk),
                _ => None,
            }
        };
        if let Some(operation) = operation {
            operation
                .completed
                .send_replace(Some(Err(MemberDiskServiceError::Cancelled)));
        }
    }
}

impl OperationWait {
    pub(super) async fn completed(mut self) -> DiskResult {
        loop {
            if let Some(result) = self.completed.borrow().clone() {
                return result;
            }
            if self.completed.changed().await.is_err() {
                return Err(MemberDiskServiceError::InvalidState(format!(
                    "operation for {} ended without a result",
                    self.disk
                )));
            }
        }
    }
}

fn should_cancel(current: OperationKind, incoming: OperationKind) -> bool {
    // Shrink is an accepted removal target. Online waits for it to finish and
    // then becomes a rejoin. Every other conflict yields cooperatively.
    !(current == OperationKind::Shrink && incoming == OperationKind::Online)
}
