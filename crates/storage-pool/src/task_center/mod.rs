//! One concrete application service per Pool: admission, queue, ownership and handoff.
//! Read conflict() in conflicts.rs for business rules; State below implements their lifecycle.
use crate::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{
    collections::{BTreeMap, BTreeSet},
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
mod observation;
mod operation;
mod scheduler;
pub use observation::{ConflictMatch, SchedulingSnapshot, TaskObserver, TaskSnapshot};
pub(crate) use operation::PoolWorkflow;
pub use operation::{OperationKind, PoolOperation};
use scheduler::{Entry, State};
pub mod persistence;
mod recovery;
pub use conflicts::{Conflict, ConflictDecision};
use persistence::{TaskBinding, TaskId, TaskPersistence};
pub use recovery::PersistentRun;
type Explanations = Arc<Mutex<BTreeMap<WorkflowId, Vec<ConflictMatch>>>>;
type Blockers = Arc<Mutex<BTreeMap<WorkflowId, Vec<WorkflowId>>>>;
struct Accepted {
    control: RunControl,
    start: StartPermit,
    publish: Box<dyn FnOnce() + Send>,
    task_id: Option<TaskId>,
}
// Heterogeneous workflow submissions preserve their typed result channel in this closure.
type Accept = Box<dyn FnOnce(Outcome<&mut WorkflowDriver>) -> Option<Accepted> + Send>;
struct Submission {
    operation: PoolOperation,
    accept: Accept,
    administrative: bool,
    recovery_task: Option<TaskId>,
}
enum PersistenceControl {
    Restore {
        claims: Vec<(TaskId, PoolOperation)>,
        reply: oneshot::Sender<()>,
    },
    Release {
        task: TaskId,
        reply: oneshot::Sender<()>,
    },
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
    explanations: Explanations,
    blockers: Blockers,
    scheduling: Arc<Mutex<SchedulingSnapshot>>,
    stop: CancellationToken,
    lifecycle: watch::Receiver<Lifecycle>,
    _host: Arc<Host>,
    admission_closed: Arc<AtomicBool>,
    persistence: Option<TaskPersistence>,
    persistence_control: mpsc::Sender<PersistenceControl>,
    persistence_initialized: Arc<tokio::sync::Mutex<bool>>,
    persistence_ready: Arc<AtomicBool>,
    task_ids: Arc<Mutex<BTreeMap<WorkflowId, TaskId>>>,
    recovery: Arc<Mutex<BTreeMap<(String, u32), recovery::RecoveryFn>>>,
}
impl TaskCenter {
    pub(crate) fn has_task_store(&self) -> bool {
        self.persistence.is_some()
    }
    /// Compose a TaskCenter without persistence around application-owned domain services.
    pub fn new(driver: WorkflowDriver, config: &Config) -> Outcome<Self> {
        if config.capacity == 0 {
            return Err(Failure::Rejected(
                "TaskCenter capacity must be positive".into(),
            ));
        }
        Ok(Self::start_inner(driver, config, None))
    }
    pub(crate) fn start(driver: WorkflowDriver, config: &Config) -> Self {
        Self::start_inner(driver, config, None)
    }
    /// Embed TaskCenter with application-registered services and a configurable storage adapter.
    pub fn with_task_store(
        driver: WorkflowDriver,
        config: &Config,
        pool: PoolUuid,
        instance: PoolUuid,
        store: Arc<dyn persistence::TaskStore>,
        persistence_config: persistence::PersistenceConfig,
    ) -> Outcome<Self> {
        if config.capacity == 0 {
            return Err(Failure::Rejected(
                "TaskCenter capacity must be positive".into(),
            ));
        }
        let persistence = TaskPersistence::new(pool, instance, store, persistence_config)?;
        Ok(Self::start_inner(driver, config, Some(persistence)))
    }
    fn start_inner(
        mut driver: WorkflowDriver,
        config: &Config,
        persistence: Option<TaskPersistence>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Submission>(config.capacity);
        let (persistence_control, mut controls) =
            mpsc::channel::<PersistenceControl>(config.capacity);
        let needs_initialization = persistence.is_some();
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let (life, lifecycle) = watch::channel(Lifecycle::Running);
        let explanations: Explanations = Arc::default();
        let notes = explanations.clone();
        let blockers: Blockers = Arc::default();
        let live_blockers = blockers.clone();
        let scheduling = Arc::new(Mutex::new(SchedulingSnapshot::default()));
        let schedule = scheduling.clone();
        let execution = driver.view();
        let failure: Arc<Mutex<Option<Failure>>> = Arc::default();
        let shutdown_failure = failure.clone();
        let limit = config.max_workflows;
        let admission_closed = Arc::new(AtomicBool::new(needs_initialization));
        let closed = admission_closed.clone();
        let task_ids = Arc::new(Mutex::new(BTreeMap::new()));
        let task_id_index = task_ids.clone();
        let task = tokio::spawn(async move {
            let mut state = State::default();
            let mut closing = false;
            loop {
                state.reap();
                state.pump();
                let topology: Vec<_> = state
                    .entries
                    .iter()
                    .filter(|(_, e)| e.operation.kind.is_topology())
                    .map(|(id, _)| *id)
                    .collect();
                {
                    let mut published = live_blockers.lock().unwrap();
                    let next: BTreeMap<_, _> = state
                        .entries
                        .iter()
                        .filter(|(_, e)| e.operation.kind == OperationKind::Shrink)
                        .map(|(id, _)| (*id, topology.clone()))
                        .collect();
                    for (id, blockers) in &next {
                        if published.get(id) != Some(blockers) {
                            state.entries[id].control.recheck();
                        }
                    }
                    *published = next;
                }
                *schedule.lock().unwrap() = state.snapshot();
                // Keep only records still retained by runtime history.
                let retained: BTreeSet<_> = driver.view().task_ids().into_iter().collect();
                notes.lock().unwrap().retain(|id, _| retained.contains(id));
                task_id_index
                    .lock()
                    .unwrap()
                    .retain(|id, _| retained.contains(id));
                if closing && driver.is_idle() {
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
                    command = controls.recv(), if !closing => {
                        match command {
                            Some(PersistenceControl::Restore { claims, reply }) => {
                                state.pending_tasks.extend(claims);
                                let _ = reply.send(());
                            }
                            Some(PersistenceControl::Release { task, reply }) => {
                                state.pending_tasks.remove(&task);
                                let _ = reply.send(());
                            }
                            None => {}
                        }
                    }
                    submission = rx.recv(), if !closing => {
                        let Some(submission) = submission else { stopping.cancel(); continue; };
                        state.reap();
                        let decision = if closed.load(Ordering::SeqCst) && !submission.administrative { Err(Failure::Unavailable) }
                            else if state.entries.len() >= limit { Err(Failure::Busy) }
                            else { state.inspect(&submission.operation, submission.recovery_task) };
                        let matches = match decision {
                            Ok(matches) => matches,
                            Err(error) => { (submission.accept)(Err(error)); continue; }
                        };
                        // Runtime capacity/validation can still reject; no cancellation until
                        // the replacement has actually been accepted and registered.
                        if let Some(accepted) = (submission.accept)(Ok(&mut driver)) {
                            state.reap();
                            if let Some(task) = submission.recovery_task {
                                state.pending_tasks.remove(&task);
                                state.fences.retain(|(owner, _)| *owner != Some(task));
                            }
                            let fenced = state.fences.iter().any(|(_, f)| f.overlaps(&submission.operation));
                            let id = accepted.control.id;
                            if let Some(task_id) = accepted.task_id { task_id_index.lock().unwrap().insert(id, task_id); }
                            notes.lock().unwrap().insert(id, matches.clone());
                            state.entries.insert(id, Entry {
                                operation: submission.operation,
                                control: accepted.control,
                                start: Some(accepted.start),
                                task_id: accepted.task_id,
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
            explanations,
            blockers,
            scheduling,
            stop,
            lifecycle,
            _host: Arc::new(Host(task)),
            admission_closed,
            persistence,
            persistence_control,
            persistence_initialized: Arc::new(tokio::sync::Mutex::new(!needs_initialization)),
            persistence_ready: Arc::new(AtomicBool::new(!needs_initialization)),
            task_ids,
            recovery: Arc::default(),
        }
    }
    pub(crate) fn abort(&self) {
        self._host.0.abort();
    }
    pub(crate) fn close_admission(&self) {
        self.admission_closed.store(true, Ordering::SeqCst);
    }
    pub(crate) fn open_admission(&self) {
        if self.persistence_ready.load(Ordering::SeqCst) {
            self.admission_closed.store(false, Ordering::SeqCst);
            // Lifecycle state is finalized outside the task body. A guard may have
            // observed Loading/Deleting just before that task released its blocker.
            for id in self.execution.task_ids() {
                let _ = self.execution.recheck(id);
            }
        }
    }
    pub(crate) fn accepts(&self) -> bool {
        !self.admission_closed.load(Ordering::SeqCst) && !self.stop.is_cancelled()
    }
    pub(crate) fn stopped(&self) -> bool {
        self.stop.is_cancelled()
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
        let result = self.failure.lock().unwrap().clone().map_or(Ok(()), Err);
        if let Some(p) = &self.persistence {
            p.flush().await?;
        }
        result
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
                    topology_blockers: self
                        .blockers
                        .lock()
                        .unwrap()
                        .get(&execution.id)
                        .cloned()
                        .unwrap_or_default(),
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
            blockers: self.blockers.clone(),
        })
    }
    pub fn suspend(&self, id: WorkflowId) -> Outcome<()> {
        self.execution.suspend(id)
    }
    pub fn resume(&self, id: WorkflowId) -> Outcome<()> {
        self.execution.resume(id)
    }
    pub fn recheck(&self, id: WorkflowId) -> Outcome<()> {
        self.execution.recheck(id)
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
        let operation = workflow.operation();
        let binding = self.persistence.as_ref().map(|p| {
            p.history_binding(
                W::NAME.into(),
                operation.objects.clone(),
                None,
                operation.kind.task_class(),
            )
        });
        self.submit_prepared(workflow, operation, administrative, binding)
            .await
    }
    async fn submit_prepared<W: Workflow>(
        &self,
        workflow: W,
        operation: PoolOperation,
        administrative: bool,
        binding: Option<TaskBinding>,
    ) -> Outcome<Run<W::Output>> {
        if self.stop.is_cancelled() {
            if let Some(b) = binding {
                b.rejected(Failure::Unavailable);
            }
            return Err(Failure::Unavailable);
        }
        let task_id = binding.as_ref().map(|b| b.id);
        let recovery_task = binding.as_ref().filter(|b| b.is_recovery).map(|b| b.id);
        let options = binding.as_ref().map(|b| b.options()).unwrap_or_default();
        let failed_binding = binding.clone();
        let (reply, response) = oneshot::channel();
        let sent = self
            .tx
            .send(Submission {
                operation,
                administrative,
                recovery_task,
                accept: Box::new(move |driver| {
                    let result =
                        driver.and_then(|driver| driver.defer_with_options(workflow, options));
                    match result {
                        Err(error) => {
                            if let Some(b) = binding {
                                b.rejected(error.clone());
                            }
                            let _ = reply.send(Err(error));
                            None
                        }
                        Ok((run, start)) => Some(Accepted {
                            control: run.control.clone(),
                            start,
                            task_id,
                            publish: Box::new(move || {
                                let _ = reply.send(Ok(run));
                            }),
                        }),
                    }
                }),
            })
            .await;
        if sent.is_err() {
            if let Some(b) = failed_binding {
                b.rejected(Failure::Unavailable);
            }
            return Err(Failure::Unavailable);
        }
        response.await.unwrap_or(Err(Failure::Unavailable))
    }
    pub(crate) fn try_submit<W: PoolWorkflow>(
        &self,
        workflow: W,
        source_event: Option<uuid::Uuid>,
        callback: impl FnOnce(Outcome<Run<W::Output>>) + Send + 'static,
    ) -> Outcome<()> {
        if !self.accepts() {
            return Err(Failure::Unavailable);
        }
        let operation = workflow.operation();
        let binding = self.persistence.as_ref().map(|p| {
            p.history_binding(
                W::NAME.into(),
                operation.objects.clone(),
                source_event,
                operation.kind.task_class(),
            )
        });
        let task_id = binding.as_ref().map(|b| b.id);
        let options = binding.as_ref().map(|b| b.options()).unwrap_or_default();
        let failed_binding = binding.clone();
        self.tx
            .try_send(Submission {
                operation,
                administrative: false,
                recovery_task: None,
                accept: Box::new(move |driver| {
                    match driver.and_then(|driver| driver.defer_with_options(workflow, options)) {
                        Err(error) => {
                            if let Some(b) = binding {
                                b.rejected(error.clone());
                            }
                            callback(Err(error));
                            None
                        }
                        Ok((run, start)) => Some(Accepted {
                            control: run.control.clone(),
                            start,
                            task_id,
                            publish: Box::new(move || callback(Ok(run))),
                        }),
                    }
                }),
            })
            .map_err(|error| {
                let failure = match error {
                    mpsc::error::TrySendError::Full(_) => Failure::Busy,
                    mpsc::error::TrySendError::Closed(_) => Failure::Unavailable,
                };
                if let Some(b) = failed_binding {
                    b.rejected(failure.clone());
                }
                failure
            })
    }
}

#[cfg(test)]
mod tests;
