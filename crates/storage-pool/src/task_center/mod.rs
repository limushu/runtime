//! One concrete application service per Pool: admission, queue, ownership and handoff.
//! Read conflict() in conflicts.rs for business rules; State below implements their lifecycle.
use crate::*;
use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;
use workflow_foundation::{
    ExecutionView, Lifecycle, ObjectKey, RunControl, RunState, RuntimeSnapshot, StartPermit,
    Workflow, WorkflowDriver,
};

mod conflicts;
pub use conflicts::PoolSchedulingPolicy;
use workflow_foundation::scheduling::{self, Candidate, SchedulingPolicy};
pub use workflow_foundation::{
    Conflict, ConflictDecision, ConflictMatch, TaskObserver, TaskSnapshot,
};
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum OperationKind {
    ObserveNode {
        id: NodeId,
        sequence: u64,
        connected: bool,
    },
    LoadNode(NodeId),
    OfflineNode {
        id: NodeId,
        sequence: u64,
    },
    OnlineNode {
        id: NodeId,
        sequence: u64,
    },
    OnlineDisk(DiskId),
    OfflineDisk(DiskId),
    CreateVolume(VolumeId),
    AttachNode(NodeId),
    DetachNode(NodeId),
    CreateTier(TierId),
    DeleteTier(TierId),
    RemoveDisk(DiskId),
    DeleteVolume(VolumeId),
    DeletePool,
    RenamePool,
    UnloadPool,
    LoadPool,
    Shrink(Vec<DiskId>),
    Custom(String),
}
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PoolOperation {
    pub kind: OperationKind,
    pub objects: Vec<ObjectKey>,
}

pub trait PoolWorkflow: Workflow {
    fn operation(&self) -> PoolOperation;
}

#[derive(Debug, Clone, Default)]
pub struct SchedulingSnapshot {
    pub queued: usize,
    pub running: usize,
    /// Logical ownership fences; only reconciliation can release these (not implemented).
    pub fenced: usize,
}

struct Accepted {
    control: RunControl,
    start: StartPermit,
    publish: Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send>,
}
// Heterogeneous workflow submissions preserve their typed result channel in this closure.
type Accept = Box<dyn FnOnce(Outcome<&mut WorkflowDriver>) -> Option<Accepted> + Send>;
struct Submission {
    operation: PoolOperation,
    accept: Accept,
    administrative: bool,
}
struct Entry {
    operation: PoolOperation,
    control: RunControl,
    start: Option<StartPermit>,
    activated: bool,
}
struct State {
    entries: BTreeMap<TaskId, Entry>,
    fences: Vec<(TaskId, PoolOperation)>,
    policy: Arc<dyn SchedulingPolicy<PoolOperation>>,
}
impl State {
    // Reap before every admission too: a terminal notification may still be in the mailbox.
    fn reap(&mut self) {
        let terminal: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(id, e)| {
                matches!(e.control.state(), RunState::Completed | RunState::Failed(_))
                    .then_some(*id)
            })
            .collect();
        for id in terminal {
            let entry = self.entries.remove(&id).unwrap();
            if matches!(
                entry.control.state(),
                RunState::Failed(Failure::Uncertain(_))
            ) {
                self.fences.push((id, entry.operation));
            }
        }
    }
    fn check_submission(&self, operation: &PoolOperation) -> Outcome<Vec<ConflictMatch>> {
        let mut matches = Vec::new();
        for (id, entry) in &self.entries {
            let conflict = self.policy.on_task(&entry.operation, operation);
            if let ConflictDecision::Reject(reason) = conflict.decision {
                return Err(crate::support::reject(reason));
            }
            if conflict.decision != ConflictDecision::Independent {
                matches.push(ConflictMatch {
                    existing: *id,
                    conflict,
                });
            }
        }
        Ok(matches)
    }
    fn snapshot(&self) -> SchedulingSnapshot {
        SchedulingSnapshot {
            queued: self.entries.values().filter(|e| e.start.is_some()).count(),
            running: self.entries.values().filter(|e| e.start.is_none()).count(),
            fenced: self.fences.len(),
        }
    }
    fn plan(&self) -> scheduling::Plan {
        let tasks: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, e)| !self.fenced(&e.operation))
            .map(|(id, e)| Candidate {
                id: *id,
                descriptor: &e.operation,
                started: e.start.is_none(),
                accepted: e.activated,
                paused: e.control.suspend_acknowledged(),
            })
            .chain(self.fences.iter().map(|(id, op)| Candidate {
                id: *id,
                descriptor: op,
                started: true,
                accepted: true,
                paused: false,
            }))
            .collect();
        scheduling::plan(self.policy.as_ref(), &tasks)
    }
    fn fenced(&self, op: &PoolOperation) -> bool {
        self.fences.iter().any(|(_, fence)| {
            op.overlaps(fence)
                || self.policy.on_task(fence, op).decision != ConflictDecision::Independent
                || self.policy.on_task(op, fence).decision != ConflictDecision::Independent
        })
    }
    fn pump(&mut self) {
        // A failed durable registration must not pause/cancel already accepted work.
        let accepted: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, e)| !e.activated && matches!(e.control.acceptance_status(), Some(Ok(()))))
            .map(|(id, _)| *id)
            .collect();
        for id in accepted {
            self.entries.get_mut(&id).unwrap().activated = true;
        }
        let plan = self.plan();
        for (_, current) in plan.cancellations {
            if let Some(entry) = self.entries.get(&current)
                && entry.control.has_started()
                && !entry.control.cancellation_requested()
            {
                entry.control.cancel();
            }
        }
        for (id, reasons) in plan.pauses {
            if let Some(e) = self.entries.get(&id) {
                e.control.set_policy_suspensions(&reasons);
            }
        }
        let plan = self.plan();
        for (id, blockers) in plan.blockers {
            if let Some(e) = self.entries.get(&id) {
                e.control.set_blockers(blockers);
            }
        }
        let rejected: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, e)| e.start.is_some() && self.fenced(&e.operation))
            .map(|(id, _)| *id)
            .collect();
        for id in rejected {
            self.entries
                .get_mut(&id)
                .unwrap()
                .start
                .take()
                .unwrap()
                .reject(crate::support::reject("object requires reconciliation"));
        }
        for id in plan.ready {
            if let Some(start) = self.entries.get_mut(&id).and_then(|e| e.start.take()) {
                start.start();
            }
        }
    }
}

struct Host(JoinHandle<()>);
impl Drop for Host {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Pool coordinator: generic task lifecycle and an injected business scheduling policy.
#[derive(Clone)]
pub struct TaskCenter {
    execution: ExecutionView,
    failure: Arc<Mutex<Option<Failure>>>,
    tx: mpsc::Sender<Submission>,
    routing: Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
    scheduling: Arc<Mutex<SchedulingSnapshot>>,
    stop: CancellationToken,
    lifecycle: watch::Receiver<Lifecycle>,
    _host: Arc<Host>,
    admission_closed: Arc<AtomicBool>,
}
impl TaskCenter {
    pub(crate) fn start(driver: WorkflowDriver, config: &Config) -> Self {
        Self::with_policy(driver, config, Arc::new(PoolSchedulingPolicy))
    }
    pub fn with_policy(
        driver: WorkflowDriver,
        config: &Config,
        policy: Arc<dyn SchedulingPolicy<PoolOperation>>,
    ) -> Self {
        Self::build(driver, config, policy, vec![], Arc::default())
    }
    /// Called after domain restoration and Driver::restore. All claims are installed together.
    pub fn from_restored(
        driver: WorkflowDriver,
        config: &Config,
        policy: Arc<dyn SchedulingPolicy<PoolOperation>>,
        restored: Vec<workflow_foundation::persistence::Restored>,
    ) -> Outcome<Self> {
        Self::from_restored_with_routing(driver, config, policy, restored, Arc::default())
    }
    pub(crate) fn from_restored_with_routing(
        driver: WorkflowDriver,
        config: &Config,
        policy: Arc<dyn SchedulingPolicy<PoolOperation>>,
        restored: Vec<workflow_foundation::Restored>,
        routing: Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
    ) -> Outcome<Self> {
        let mut entries = vec![];
        for restored in restored {
            let operation: PoolOperation = serde_json::from_value(restored.record.metadata)
                .map_err(|e| crate::support::reject(e.to_string()))?;
            if operation.objects != restored.record.objects {
                return Err(crate::support::reject("restored scheduling claims differ"));
            }
            entries.push(Entry {
                operation,
                control: restored.control,
                start: Some(restored.start),
                activated: true,
            });
        }
        Ok(Self::build(driver, config, policy, entries, routing))
    }
    fn build(
        mut driver: WorkflowDriver,
        config: &Config,
        policy: Arc<dyn SchedulingPolicy<PoolOperation>>,
        restored: Vec<Entry>,
        routing: Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Submission>(config.capacity);
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let (life, lifecycle) = watch::channel(Lifecycle::Running);
        let scheduling = Arc::new(Mutex::new(SchedulingSnapshot::default()));
        let schedule = scheduling.clone();
        let execution = driver.view();
        let failure: Arc<Mutex<Option<Failure>>> = Arc::default();
        let shutdown_failure = failure.clone();
        let limit = config.max_workflows;
        let admission_closed = Arc::new(AtomicBool::new(false));
        let closed = admission_closed.clone();
        let task = tokio::spawn(async move {
            let mut state = State {
                entries: restored.into_iter().map(|e| (e.control.id, e)).collect(),
                fences: vec![],
                policy,
            };
            let mut closing = false;
            let mut deliveries = FuturesUnordered::new();
            loop {
                state.reap();
                state.pump();
                *schedule.lock().unwrap() = state.snapshot();
                if closing && driver.is_idle() && deliveries.is_empty() {
                    break;
                }
                tokio::select! {
                    _ = stopping.cancelled(), if !closing => {
                        closing = true;
                        life.send_replace(Lifecycle::Stopping);
                        rx.close();
                        driver.begin_shutdown();
                        while let Ok(submission) = rx.try_recv() {
                            (submission.accept)(Err(Failure::Unavailable));
                        }
                    }
                    _ = driver.tick() => {}
                    Some(()) = deliveries.next(), if !deliveries.is_empty() => {}
                    submission = rx.recv(), if !closing => {
                        let Some(submission) = submission else { stopping.cancel(); continue; };
                        state.reap();
                        let decision = if closed.load(Ordering::SeqCst) && !submission.administrative { Err(Failure::Unavailable) }
                            else if state.entries.len() >= limit { Err(Failure::Busy) }
                            else { state.check_submission(&submission.operation) };
                        let matches = match decision {
                            Ok(matches) => matches,
                            Err(error) => { (submission.accept)(Err(error)); continue; }
                        };
                        // Runtime capacity/validation can still reject; no cancellation until
                        // the replacement has actually been accepted and registered.
                        if let Some(accepted) = (submission.accept)(Ok(&mut driver)) {
                            state.reap();
                            let id = accepted.control.id;
                            accepted.control.explain(matches, vec![]);
                            state.entries.insert(id, Entry {
                                operation: submission.operation,
                                control: accepted.control,
                                start: Some(accepted.start),
                                activated: false,

                            });
                            state.reap();
                            state.pump();
                            *schedule.lock().unwrap() = state.snapshot();
                            deliveries.push((accepted.publish)());
                        }
                    }
                }
            }
            if let Err(error) = driver.shutdown().await {
                *shutdown_failure.lock().unwrap() = Some(error);
            }
            life.send_replace(Lifecycle::Stopped);
        });
        Self {
            execution,
            failure,
            tx,
            routing,
            scheduling,
            stop,
            lifecycle,
            _host: Arc::new(Host(task)),
            admission_closed,
        }
    }
    pub(crate) fn abort(&self) {
        self._host.0.abort();
    }
    pub(crate) fn close_admission(&self) {
        self.admission_closed.store(true, Ordering::SeqCst);
    }
    pub(crate) fn open_admission(&self) {
        self.admission_closed.store(false, Ordering::SeqCst);
    }
    pub(crate) fn accepts(&self) -> bool {
        !self.admission_closed.load(Ordering::SeqCst) && !self.stop.is_cancelled()
    }
    pub(crate) fn stopped(&self) -> bool {
        self.stop.is_cancelled()
    }
    pub(crate) fn has_reservation(&self, id: &DiskId) -> bool {
        self.routing.lock().unwrap().contains_key(id)
    }
    pub(crate) fn disk_spec(&self, id: &DiskId) -> Option<DiskSpec> {
        self.routing.lock().unwrap().get(id).cloned()
    }
    pub(crate) fn disk_directory(&self) -> Arc<Mutex<BTreeMap<DiskId, DiskSpec>>> {
        self.routing.clone()
    }
    pub(crate) fn stop(&self) {
        self.stop.cancel();
    }
    pub async fn shutdown(&self) -> Outcome<()> {
        self.stop();
        let mut lifecycle = self.lifecycle.clone();
        while *lifecycle.borrow_and_update() != Lifecycle::Stopped {
            lifecycle
                .changed()
                .await
                .map_err(|_| Failure::Unavailable)?;
        }
        self.failure.lock().unwrap().clone().map_or(Ok(()), Err)
    }
    pub(crate) fn register_disk(&self, spec: &DiskSpec) -> Outcome<()> {
        let mut routing = self.routing.lock().unwrap();
        if let Some(old) = routing.get(&spec.id)
            && (old.node != spec.node
                || old.tier != spec.tier
                || old.capacity_blks != spec.capacity_blks)
        {
            return Err(crate::support::reject("disk routing identity is immutable"));
        }
        routing.insert(spec.id.clone(), spec.clone());
        Ok(())
    }
    pub(crate) fn node_disks(&self, node: &NodeId) -> Vec<DiskSpec> {
        self.routing
            .lock()
            .unwrap()
            .values()
            .filter(|s| &s.node == node)
            .cloned()
            .collect()
    }
    pub fn tasks(&self) -> Vec<TaskSnapshot> {
        self.execution.tasks()
    }
    pub fn task(&self, id: TaskId) -> Outcome<TaskSnapshot> {
        self.execution.task(id)
    }
    pub fn observe_task(&self, id: TaskId) -> Outcome<TaskObserver> {
        self.execution.observe_task(id)
    }
    pub fn suspend(&self, id: TaskId) -> Outcome<()> {
        self.execution.suspend(id)
    }
    pub fn resume(&self, id: TaskId) -> Outcome<()> {
        self.execution.resume(id)
    }
    pub fn recheck(&self, id: TaskId) -> Outcome<()> {
        self.execution.recheck(id)
    }
    pub fn cancel(&self, id: TaskId) -> Outcome<()> {
        self.execution.cancel(id)
    }
    pub async fn wait_suspended(&self, id: TaskId) -> Outcome<()> {
        self.execution.wait_suspended(id).await
    }
    pub fn scheduling(&self) -> SchedulingSnapshot {
        self.scheduling.lock().unwrap().clone()
    }
    pub fn snapshot(&self) -> RuntimeSnapshot {
        self.execution.snapshot()
    }

    pub async fn submit<W: PoolWorkflow>(&self, workflow: W) -> Outcome<Run<W::Output>> {
        self.submit_with_access(workflow, false).await
    }
    pub(crate) async fn submit_admin<W: PoolWorkflow>(
        &self,
        workflow: W,
    ) -> Outcome<Run<W::Output>> {
        self.submit_with_access(workflow, true).await
    }
    async fn submit_with_access<W: PoolWorkflow>(
        &self,
        workflow: W,
        administrative: bool,
    ) -> Outcome<Run<W::Output>> {
        if self.stop.is_cancelled() {
            return Err(Failure::Unavailable);
        }
        let operation = workflow.operation();
        let metadata =
            serde_json::to_value(&operation).map_err(|e| crate::support::reject(e.to_string()))?;
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Submission {
                operation,
                administrative,
                accept: Box::new(move |driver| {
                    let result =
                        driver.and_then(|driver| driver.defer_with_metadata(workflow, metadata));
                    match result {
                        Err(error) => {
                            let _ = reply.send(Err(error));
                            None
                        }
                        Ok((run, start)) => Some(Accepted {
                            control: run.control.clone(),
                            start,
                            publish: Box::new(move || {
                                Box::pin(async move {
                                    let mut run = run;
                                    let result = match run.accepted().await {
                                        Ok(()) => Ok(run),
                                        Err(e) => Err(e),
                                    };
                                    let _ = reply.send(result);
                                })
                            }),
                        }),
                    }
                }),
            })
            .await
            .map_err(|_| Failure::Unavailable)?;
        let mut run = response.await.unwrap_or(Err(Failure::Unavailable))?;
        run.accepted().await?;
        Ok(run)
    }
    pub(crate) fn try_submit<W: PoolWorkflow>(
        &self,
        workflow: W,
        callback: impl FnOnce(Outcome<Run<W::Output>>) + Send + 'static,
    ) -> Outcome<()> {
        if !self.accepts() {
            return Err(Failure::Unavailable);
        }
        let operation = workflow.operation();
        let metadata =
            serde_json::to_value(&operation).map_err(|e| crate::support::reject(e.to_string()))?;
        self.tx
            .try_send(Submission {
                operation,
                administrative: false,
                accept: Box::new(move |driver| {
                    match driver.and_then(|driver| driver.defer_with_metadata(workflow, metadata)) {
                        Err(error) => {
                            callback(Err(error));
                            None
                        }
                        Ok((run, start)) => Some(Accepted {
                            control: run.control.clone(),
                            start,
                            publish: Box::new(move || {
                                Box::pin(async move {
                                    let mut run = run;
                                    callback(match run.accepted().await {
                                        Ok(()) => Ok(run),
                                        Err(e) => Err(e),
                                    });
                                })
                            }),
                        }),
                    }
                }),
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => Failure::Busy,
                mpsc::error::TrySendError::Closed(_) => Failure::Unavailable,
            })
    }
}

#[cfg(test)]
mod tests;
