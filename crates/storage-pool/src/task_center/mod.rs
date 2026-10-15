//! One concrete application service per Pool: admission, queue, ownership and handoff.
//! Read conflict() in conflicts.rs for business rules; State below implements their lifecycle.
use crate::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    ops::Deref,
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
pub use conflicts::{Conflict, ConflictDecision};
#[derive(Debug, Clone, PartialEq, Eq)]
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
}
#[derive(Debug, Clone)]
pub struct PoolOperation {
    pub kind: OperationKind,
    pub objects: Vec<ObjectKey>,
}

pub(crate) trait PoolWorkflow: Workflow {
    fn operation(&self) -> PoolOperation;
}

#[derive(Debug, Clone)]
pub struct ConflictMatch {
    pub existing: WorkflowId,
    pub conflict: Conflict,
}
/// Runtime observation plus admission-time business explanations (not live blockers).
#[derive(Debug, Clone)]
pub struct TaskSnapshot {
    pub execution: workflow_foundation::TaskSnapshot,
    pub conflicts: Vec<ConflictMatch>,
}
impl Deref for TaskSnapshot {
    type Target = workflow_foundation::TaskSnapshot;
    fn deref(&self) -> &Self::Target {
        &self.execution
    }
}
pub struct TaskObserver {
    execution: workflow_foundation::TaskObserver,
    conflicts: Vec<ConflictMatch>,
}
impl TaskObserver {
    pub fn snapshot(&self) -> TaskSnapshot {
        TaskSnapshot {
            execution: self.execution.snapshot(),
            conflicts: self.conflicts.clone(),
        }
    }
    pub async fn changed(&mut self) -> Outcome<TaskSnapshot> {
        Ok(TaskSnapshot {
            execution: self.execution.changed().await?,
            conflicts: self.conflicts.clone(),
        })
    }
}
#[derive(Debug, Clone, Default)]
pub struct SchedulingSnapshot {
    pub queued: usize,
    pub running: usize,
    /// Logical ownership fences; only reconciliation can release these (not implemented).
    pub fenced: usize,
}

type Explanations = Arc<Mutex<BTreeMap<WorkflowId, Vec<ConflictMatch>>>>;
struct Accepted {
    control: RunControl,
    start: StartPermit,
    publish: Box<dyn FnOnce() + Send>,
}
// Heterogeneous workflow submissions preserve their typed result channel in this closure.
type Accept = Box<dyn FnOnce(Outcome<&mut WorkflowDriver>) -> Option<Accepted> + Send>;
struct Submission {
    operation: PoolOperation,
    accept: Accept,
}
struct Entry {
    operation: PoolOperation,
    control: RunControl,
    start: Option<StartPermit>,
}
#[derive(Default)]
struct State {
    entries: BTreeMap<WorkflowId, Entry>,
    fences: Vec<PoolOperation>,
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
                self.fences.push(entry.operation);
            }
        }
    }
    fn inspect(&self, operation: &PoolOperation) -> Outcome<Vec<ConflictMatch>> {
        let mut matches = Vec::new();
        for (id, entry) in &self.entries {
            let conflict = TaskCenter::conflict(operation, &entry.operation);
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
    fn pump(&mut self) {
        // Started owners always block. Earlier queued requests also block later overlapping
        // requests, preventing overtaking, while unrelated objects can run concurrently.
        let mut blocked: Vec<PoolOperation> = self
            .entries
            .values()
            .filter(|e| e.start.is_none())
            .map(|e| e.operation.clone())
            .collect();
        for entry in self.entries.values_mut() {
            if entry.start.is_none() {
                continue;
            }
            if self.fences.iter().any(|f| f.overlaps(&entry.operation)) {
                entry
                    .start
                    .take()
                    .unwrap()
                    .reject(crate::support::reject("object requires reconciliation"));
            } else if !blocked.iter().any(|b| b.overlaps(&entry.operation)) {
                entry.start.take().unwrap().start();
            }
            blocked.push(entry.operation.clone());
        }
    }
}
struct Host(JoinHandle<()>);
impl Drop for Host {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Concrete Pool coordinator. There is deliberately no TaskCenter trait or policy injection.
#[derive(Clone)]
pub struct TaskCenter {
    execution: ExecutionView,
    failure: Arc<Mutex<Option<Failure>>>,
    tx: mpsc::Sender<Submission>,
    routing: Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
    explanations: Explanations,
    scheduling: Arc<Mutex<SchedulingSnapshot>>,
    stop: CancellationToken,
    lifecycle: watch::Receiver<Lifecycle>,
    _host: Arc<Host>,
}
impl TaskCenter {
    pub(crate) fn start(mut driver: WorkflowDriver, config: &Config) -> Self {
        let (tx, mut rx) = mpsc::channel::<Submission>(config.capacity);
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let (life, lifecycle) = watch::channel(Lifecycle::Running);
        let explanations: Explanations = Arc::default();
        let notes = explanations.clone();
        let scheduling = Arc::new(Mutex::new(SchedulingSnapshot::default()));
        let schedule = scheduling.clone();
        let execution = driver.view();
        let failure: Arc<Mutex<Option<Failure>>> = Arc::default();
        let shutdown_failure = failure.clone();
        let limit = config.max_workflows;
        let task = tokio::spawn(async move {
            let mut state = State::default();
            let mut closing = false;
            loop {
                state.reap();
                state.pump();
                *schedule.lock().unwrap() = state.snapshot();
                // Keep only records still retained by runtime history.
                let retained: BTreeSet<_> = driver.view().tasks().iter().map(|t| t.id).collect();
                notes.lock().unwrap().retain(|id, _| retained.contains(id));
                if closing && driver.is_idle() {
                    break;
                }
                tokio::select! {
                    biased;
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
                    submission = rx.recv(), if !closing => {
                        let Some(submission) = submission else { stopping.cancel(); continue; };
                        state.reap();
                        let decision = if state.entries.len() >= limit { Err(Failure::Busy) }
                            else { state.inspect(&submission.operation) };
                        let matches = match decision {
                            Ok(matches) => matches,
                            Err(error) => { (submission.accept)(Err(error)); continue; }
                        };
                        // Runtime capacity/validation can still reject; no cancellation until
                        // the replacement has actually been accepted and registered.
                        if let Some(accepted) = (submission.accept)(Ok(&mut driver)) {
                            state.reap();
                            let fenced = state.fences.iter().any(|f| f.overlaps(&submission.operation));
                            let id = accepted.control.id;
                            notes.lock().unwrap().insert(id, matches.clone());
                            state.entries.insert(id, Entry {
                                operation: submission.operation,
                                control: accepted.control,
                                start: Some(accepted.start),
                            });
                            for found in matches {
                                if !fenced && found.conflict.decision == ConflictDecision::CancelThenWait
                                    && let Some(entry) = state.entries.get(&found.existing) {
                                    entry.control.cancel();
                                }
                            }
                            state.reap();
                            state.pump();
                            *schedule.lock().unwrap() = state.snapshot();
                            (accepted.publish)();
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
            routing: Arc::default(),
            explanations,
            scheduling,
            stop,
            lifecycle,
            _host: Arc::new(Host(task)),
        }
    }
    pub(crate) fn abort(&self) {
        self._host.0.abort();
    }
    pub(crate) fn stop(&self) {
        self.stop.cancel();
    }
    pub(crate) async fn shutdown(&self) -> Outcome<()> {
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
        self.execution
            .tasks()
            .into_iter()
            .map(|execution| {
                let conflicts = self
                    .explanations
                    .lock()
                    .unwrap()
                    .get(&execution.id)
                    .cloned()
                    .unwrap_or_default();
                TaskSnapshot {
                    execution,
                    conflicts,
                }
            })
            .collect()
    }
    pub fn task(&self, id: WorkflowId) -> Outcome<TaskSnapshot> {
        Ok(self.observe_task(id)?.snapshot())
    }
    pub fn observe_task(&self, id: WorkflowId) -> Outcome<TaskObserver> {
        Ok(TaskObserver {
            execution: self.execution.observe_task(id)?,
            conflicts: self
                .explanations
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .unwrap_or_default(),
        })
    }
    pub fn suspend(&self, id: WorkflowId) -> Outcome<()> {
        self.execution.suspend(id)
    }
    pub fn resume(&self, id: WorkflowId) -> Outcome<()> {
        self.execution.resume(id)
    }
    pub fn cancel(&self, id: WorkflowId) -> Outcome<()> {
        self.execution.cancel(id)
    }
    pub async fn wait_suspended(&self, id: WorkflowId) -> Outcome<()> {
        self.execution.wait_suspended(id).await
    }
    pub fn scheduling(&self) -> SchedulingSnapshot {
        self.scheduling.lock().unwrap().clone()
    }
    pub fn snapshot(&self) -> RuntimeSnapshot {
        self.execution.snapshot()
    }

    pub(crate) async fn submit<W: PoolWorkflow>(&self, workflow: W) -> Outcome<Run<W::Output>> {
        if self.stop.is_cancelled() {
            return Err(Failure::Unavailable);
        }
        let operation = workflow.operation();
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Submission {
                operation,
                accept: Box::new(move |driver| {
                    let result = driver.and_then(|driver| driver.defer(workflow));
                    match result {
                        Err(error) => {
                            let _ = reply.send(Err(error));
                            None
                        }
                        Ok((run, start)) => Some(Accepted {
                            control: run.control.clone(),
                            start,
                            publish: Box::new(move || {
                                let _ = reply.send(Ok(run));
                            }),
                        }),
                    }
                }),
            })
            .await
            .map_err(|_| Failure::Unavailable)?;
        response.await.unwrap_or(Err(Failure::Unavailable))
    }
}

#[cfg(test)]
mod tests;
