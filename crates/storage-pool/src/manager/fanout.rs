use super::*;
use futures_util::{StreamExt, stream::FuturesUnordered};
use tokio::{sync::watch, time::Instant};
use workflow_foundation::{RunControl, RunState};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskRef {
    pub pool: PoolUuid,
    pub instance: PoolUuid,
    pub task_id: TaskId,
}
#[derive(Debug, Clone)]
pub struct PoolTaskSnapshot {
    pub task: TaskRef,
    pub snapshot: TaskSnapshot,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyChange {
    NodeConnectivity { node: NodeId, connected: bool },
    DiskAvailability { disk: DiskId, online: bool },
}
/// Source epoch must be advanced by the authoritative source before its sequence resets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopologyEvent {
    pub id: uuid::Uuid,
    pub source_epoch: u64,
    pub sequence: u64,
    pub change: TopologyChange,
}
impl TopologyEvent {
    pub fn new(source_epoch: u64, sequence: u64, change: TopologyChange) -> Self {
        Self {
            id: uuid::Uuid::new_v4(),
            source_epoch,
            sequence,
            change,
        }
    }
    pub(crate) fn key(&self) -> workflow_foundation::ObjectKey {
        match &self.change {
            TopologyChange::NodeConnectivity { node, .. } => {
                crate::support::object("node", node.as_str())
            }
            TopologyChange::DiskAvailability { disk, .. } => {
                crate::support::object("disk", disk.as_str())
            }
        }
    }
    pub(crate) fn version(&self) -> (u64, u64) {
        (self.source_epoch, self.sequence)
    }
}

#[derive(Clone)]
pub(crate) enum EventDecision {
    Pending,
    NotRelevant,
    Applied,
    Accepted(Box<RunControl>),
    Rejected(Failure),
}
#[derive(Clone)]
pub struct PoolEventReceipt {
    pub pool: PoolUuid,
    pub event: uuid::Uuid,
    pub(crate) instance: PoolUuid,
    pub(crate) decision: watch::Receiver<EventDecision>,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetStatus {
    Queued,
    NotRelevant,
    Applied,
    Accepted(TaskRef),
    Completed(TaskRef),
    Failed {
        task: Option<TaskRef>,
        error: Failure,
    },
    Backpressured,
}
#[derive(Debug, Clone)]
pub struct TargetReport {
    pub pool: PoolUuid,
    pub status: TargetStatus,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitFor {
    Decision,
    Completion,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FanoutOutcome {
    Succeeded,
    NoRelevantTargets,
    Partial,
    TimedOut,
}
#[derive(Debug, Clone)]
pub struct FanoutReport {
    pub event: uuid::Uuid,
    pub phase: WaitFor,
    pub outcome: FanoutOutcome,
    pub targets: Vec<TargetReport>,
}
#[derive(Clone)]
pub struct DispatchReceipt {
    pub event: Arc<TopologyEvent>,
    pub targets: Vec<PoolEventReceipt>,
}
impl PoolEventReceipt {
    pub(crate) fn immediate(
        pool: PoolUuid,
        instance: PoolUuid,
        event: uuid::Uuid,
        decision: EventDecision,
    ) -> Self {
        let (_, decision) = watch::channel(decision);
        Self {
            pool,
            instance,
            event,
            decision,
        }
    }
    pub fn snapshot(&self) -> TargetReport {
        let status = match &*self.decision.borrow() {
            EventDecision::Pending => TargetStatus::Queued,
            EventDecision::NotRelevant => TargetStatus::NotRelevant,
            EventDecision::Applied => TargetStatus::Applied,
            EventDecision::Rejected(Failure::Busy) => TargetStatus::Backpressured,
            EventDecision::Rejected(error) => TargetStatus::Failed {
                task: None,
                error: error.clone(),
            },
            EventDecision::Accepted(control) => {
                let task = TaskRef {
                    pool: self.pool,
                    instance: self.instance,
                    task_id: control.id,
                };
                match control.state() {
                    RunState::Completed => TargetStatus::Completed(task),
                    RunState::Failed(error) => TargetStatus::Failed {
                        task: Some(task),
                        error,
                    },
                    _ => TargetStatus::Accepted(task),
                }
            }
        };
        TargetReport {
            pool: self.pool,
            status,
        }
    }
    async fn wait(mut self, phase: WaitFor) {
        let mut control = loop {
            let decision = self.decision.borrow_and_update().clone();
            match decision {
                EventDecision::Pending => {
                    if self.decision.changed().await.is_err() {
                        return;
                    }
                }
                EventDecision::Accepted(control) => break control,
                _ => return,
            }
        };
        if phase == WaitFor::Completion {
            loop {
                if matches!(control.state(), RunState::Completed | RunState::Failed(_)) {
                    return;
                }
                if control.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}
impl DispatchReceipt {
    pub fn snapshot(&self, phase: WaitFor) -> FanoutReport {
        let targets: Vec<_> = self
            .targets
            .iter()
            .map(PoolEventReceipt::snapshot)
            .collect();
        let outcome = if targets
            .iter()
            .all(|t| t.status == TargetStatus::NotRelevant)
        {
            FanoutOutcome::NoRelevantTargets
        } else if targets.iter().all(|t| match t.status {
            TargetStatus::NotRelevant | TargetStatus::Applied | TargetStatus::Completed(_) => true,
            TargetStatus::Accepted(_) => phase == WaitFor::Decision,
            _ => false,
        }) {
            FanoutOutcome::Succeeded
        } else {
            FanoutOutcome::Partial
        };
        FanoutReport {
            event: self.event.id,
            phase,
            outcome,
            targets,
        }
    }
    /// Waiting is cancellation-safe: accepted tasks remain owned by their respective Pools.
    pub async fn gather(&self, phase: WaitFor, deadline: Instant) -> FanoutReport {
        let mut pending: FuturesUnordered<_> = self
            .targets
            .iter()
            .cloned()
            .map(|target| target.wait(phase))
            .collect();
        let complete =
            tokio::time::timeout_at(deadline, async { while pending.next().await.is_some() {} })
                .await
                .is_ok();
        let mut report = self.snapshot(phase);
        if !complete {
            report.outcome = FanoutOutcome::TimedOut;
        }
        report
    }
}
impl PoolManager {
    /// Every target is offered its own queue slot before any replies are awaited.
    /// There are no await points inside the fanout, so cancellation cannot leave a partial round.
    pub async fn dispatch(&self, event: TopologyEvent) -> Outcome<DispatchReceipt> {
        if event.id.is_nil() {
            return Err(reject("nil event id"));
        }
        let event = Arc::new(event);
        let targets = self
            .fanout_handles()
            .into_iter()
            .map(|pool| pool.try_notify(event.clone()))
            .collect();
        Ok(DispatchReceipt { event, targets })
    }
    pub async fn retry(&self, receipt: &DispatchReceipt) -> DispatchReceipt {
        let targets = receipt
            .targets
            .iter()
            .map(|target| {
                if !matches!(
                    target.snapshot().status,
                    TargetStatus::Backpressured
                        | TargetStatus::Failed {
                            task: None,
                            error: Failure::Unavailable
                        }
                ) {
                    return target.clone();
                }
                match self.get(target.pool) {
                    Ok(pool) if pool.instance() == target.instance => {
                        pool.try_notify(receipt.event.clone())
                    }
                    _ => PoolEventReceipt::immediate(
                        target.pool,
                        target.instance,
                        receipt.event.id,
                        EventDecision::Rejected(reject("deleted or stale pool instance")),
                    ),
                }
            })
            .collect();
        DispatchReceipt {
            event: receipt.event.clone(),
            targets,
        }
    }
    pub async fn gather(
        &self,
        receipt: &DispatchReceipt,
        phase: WaitFor,
        deadline: Instant,
    ) -> FanoutReport {
        receipt.gather(phase, deadline).await
    }
}
