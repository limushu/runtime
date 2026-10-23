//! One concrete application service per Pool: admission, queue, ownership and handoff.
//! Read conflict() in conflicts.rs for business rules; scheduler.rs implements their lifecycle.
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
mod control;
mod model;
mod registry;
mod run;
pub use model::{TaskClass, TaskId, TaskStatus};
pub use run::{TaskControl, TaskRun};
mod observation;
mod operation;
mod scheduler;
mod submission;
pub use observation::{
    ConflictMatch, SchedulingSnapshot, TaskObserver, TaskSnapshot, TaskWait, WaitCondition,
};
pub(crate) use operation::PoolWorkflow;
pub use operation::{OperationKind, PoolOperation};
use scheduler::State;
use submission::Submission;
pub mod persistence;
mod recovery;
pub use conflicts::{Conflict, ConflictDecision, PoolRules, TaskRules};
use persistence::TaskPersistence;
pub use recovery::{RecoveryFailure, RecoveryReport};
enum SchedulingCommand {
    RegisterRecovered {
        tasks: Vec<Arc<registry::TaskRecord>>,
        reply: oneshot::Sender<()>,
    },
    CancelInactive {
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

/// Owns logical tasks, admission, waiting relationships and recovery.
/// WorkflowDriver is its passive execution engine; TaskStore is an optional journal.
#[derive(Clone)]
pub struct TaskCenter {
    execution: ExecutionView,
    failure: Arc<Mutex<Option<Failure>>>,
    tx: mpsc::Sender<Submission>,
    registry: Arc<Mutex<registry::TaskRegistry>>,
    scheduling: Arc<Mutex<SchedulingSnapshot>>,
    stop: CancellationToken,
    lifecycle: watch::Receiver<Lifecycle>,
    _host: Arc<Host>,
    admission_closed: Arc<AtomicBool>,
    persistence: Option<TaskPersistence>,
    scheduling_commands: mpsc::Sender<SchedulingCommand>,
    persistence_initialized: Arc<tokio::sync::Mutex<bool>>,
    persistence_ready: Arc<AtomicBool>,
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
        Ok(Self::start_inner(driver, config, None, Arc::new(PoolRules)))
    }
    /// Substitute application rules; the wait/suspension lifecycle stays unchanged.
    pub fn with_rules(
        driver: WorkflowDriver,
        config: &Config,
        rules: Arc<dyn TaskRules>,
    ) -> Outcome<Self> {
        if config.capacity == 0 {
            return Err(Failure::Rejected(
                "TaskCenter capacity must be positive".into(),
            ));
        }
        Ok(Self::start_inner(driver, config, None, rules))
    }
    pub(crate) fn start(driver: WorkflowDriver, config: &Config) -> Self {
        Self::start_inner(driver, config, None, Arc::new(PoolRules))
    }
    /// Embed TaskCenter with application-registered services and a configurable storage adapter.
    pub fn with_task_store(
        driver: WorkflowDriver,
        config: &Config,
        pool: PoolUuid,
        instance: PoolInstanceId,
        store: Arc<dyn persistence::TaskStore>,
        persistence_config: persistence::PersistenceConfig,
    ) -> Outcome<Self> {
        if config.capacity == 0 {
            return Err(Failure::Rejected(
                "TaskCenter capacity must be positive".into(),
            ));
        }
        let persistence = TaskPersistence::new(pool, instance, store, persistence_config)?;
        Ok(Self::start_inner(
            driver,
            config,
            Some(persistence),
            Arc::new(PoolRules),
        ))
    }
    fn start_inner(
        mut driver: WorkflowDriver,
        config: &Config,
        persistence: Option<TaskPersistence>,
        rules: Arc<dyn TaskRules>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel::<Submission>(config.capacity);
        let (scheduling_commands, mut controls) =
            mpsc::channel::<SchedulingCommand>(config.capacity);
        let needs_initialization = persistence.is_some();
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let (life, lifecycle) = watch::channel(Lifecycle::Running);
        let registry: Arc<Mutex<registry::TaskRegistry>> = Arc::default();
        let catalog = registry.clone();
        let history_limit = config.task_history;
        let scheduling = Arc::new(Mutex::new(SchedulingSnapshot::default()));
        let schedule = scheduling.clone();
        let execution = driver.view();
        let failure: Arc<Mutex<Option<Failure>>> = Arc::default();
        let shutdown_failure = failure.clone();
        let limit = config.max_workflows;
        let admission_closed = Arc::new(AtomicBool::new(needs_initialization));
        let closed = admission_closed.clone();
        let task = tokio::spawn(async move {
            let mut state = State::new(rules, catalog, history_limit);
            let mut closing = false;
            loop {
                state.reap();
                state.pump();
                *schedule.lock().unwrap() = state.snapshot();
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
                            Some(SchedulingCommand::RegisterRecovered { tasks, reply }) => {
                                state.restore(tasks);
                                let _ = reply.send(());
                            }
                            Some(SchedulingCommand::CancelInactive { task, reply }) => {
                                state.cancel_inactive(task);
                                let _ = reply.send(());
                            }
                            None => {}
                        }
                    }
                    submission = rx.recv(), if !closing => {
                        let Some(submission) = submission else {
                            stopping.cancel();
                            continue;
                        };
                        let publish = state.admit(submission, &mut driver, closed.load(Ordering::SeqCst), limit);
                        *schedule.lock().unwrap() = state.snapshot();
                        if let Some(publish) = publish { publish(); }
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
            registry,
            scheduling,
            stop,
            lifecycle,
            _host: Arc::new(Host(task)),
            admission_closed,
            persistence,
            scheduling_commands,
            persistence_initialized: Arc::new(tokio::sync::Mutex::new(!needs_initialization)),
            persistence_ready: Arc::new(AtomicBool::new(!needs_initialization)),
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
            for id in self.execution.execution_ids() {
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
        self.registry
            .lock()
            .unwrap()
            .records
            .values()
            .map(|record| record.snapshot())
            .collect()
    }
    pub fn task(&self, id: TaskId) -> Outcome<TaskSnapshot> {
        Ok(self.registry.lock().unwrap().get(id)?.snapshot())
    }
    pub fn observe_task(&self, id: TaskId) -> Outcome<TaskObserver> {
        Ok(self.registry.lock().unwrap().get(id)?.observe())
    }
    pub fn current_shrink(&self) -> Option<TaskSnapshot> {
        self.tasks()
            .into_iter()
            .find(|task| task.operation.kind == OperationKind::Shrink && !task.status.terminal())
    }
    pub fn scheduling(&self) -> SchedulingSnapshot {
        self.scheduling.lock().unwrap().clone()
    }
    pub fn snapshot(&self) -> RuntimeSnapshot {
        self.execution.snapshot()
    }
}

#[cfg(test)]
mod tests;
