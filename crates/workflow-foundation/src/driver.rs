//! Execution mechanics. WorkflowDriver is polled by its owner; it never spawns itself.
//! WorkflowRuntime is a standalone convenience host, not used inside storage Pool.
mod recovery;
use crate::{
    locks::{Broker, Event, LockTable},
    model::Scope,
    service::{self, ServiceOwner},
    tasks::TaskRegistry,
    workflow::{ExecutionEnv, execution},
    *,
};
use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

/// Read/control access only. Does not submit, schedule, spawn, or own execution.
#[derive(Clone)]
pub struct ExecutionView {
    pub(crate) scope: Arc<Scope>,
    state: watch::Receiver<RuntimeSnapshot>,
    tasks: Arc<Mutex<TaskRegistry>>,
}
impl ExecutionView {
    pub fn tasks(&self) -> Vec<TaskSnapshot> {
        self.tasks.lock().unwrap().snapshots()
    }
    /// List retained tasks without copying their Activity histories.
    pub fn task_ids(&self) -> Vec<WorkflowId> {
        self.tasks.lock().unwrap().ids()
    }
    pub fn task(&self, id: WorkflowId) -> Outcome<TaskSnapshot> {
        Ok(self.observe_task(id)?.snapshot())
    }
    pub fn observe_task(&self, id: WorkflowId) -> Outcome<TaskObserver> {
        self.tasks.lock().unwrap().observer(id)
    }
    fn control(&self, id: WorkflowId) -> Outcome<RunControl> {
        if self.scope.is_closed() {
            return Err(Failure::Unavailable);
        }
        self.tasks.lock().unwrap().control(id)
    }
    pub fn suspend(&self, id: WorkflowId) -> Outcome<()> {
        self.control(id)?.pause()
    }
    pub fn resume(&self, id: WorkflowId) -> Outcome<()> {
        self.control(id)?.resume()
    }
    pub fn recheck(&self, id: TaskId) -> Outcome<()> {
        self.control(id)?.recheck();
        Ok(())
    }
    pub fn cancel(&self, id: WorkflowId) -> Outcome<()> {
        self.control(id)?.cancel();
        Ok(())
    }
    pub async fn wait_suspended(&self, id: WorkflowId) -> Outcome<()> {
        self.control(id)?.wait_paused().await
    }
    pub fn snapshot(&self) -> RuntimeSnapshot {
        self.state.borrow().clone()
    }
}

/// One-shot start permission owned by the application's queue.
pub struct StartPermit(oneshot::Sender<Outcome<()>>);
impl StartPermit {
    pub fn start(self) {
        let _ = self.0.send(Ok(()));
    }
    pub fn reject(self, error: Failure) {
        let _ = self.0.send(Err(error));
    }
}

/// Passive execution engine. All workflow futures and resource events are driven by tick()
/// in the OWNER'S event loop. No submission mailbox, policy callback or spawned workflow host.
pub struct WorkflowDriver {
    view: ExecutionView,
    state: watch::Sender<RuntimeSnapshot>,
    pool: String,
    config: Config,
    stop: CancellationToken,
    ids: TaskIds,
    pool_uuid: u128,
    store: Option<Arc<dyn crate::persistence::TaskStore>>,
    definitions: std::collections::BTreeMap<String, crate::persistence::Restore>,
    recovery_ready: bool,
    broker: Broker,
    events: mpsc::UnboundedReceiver<Event>,
    table: LockTable,
    running: FuturesUnordered<BoxFuture<'static, WorkflowId>>,
    services: Vec<ServiceOwner>,
    names: BTreeSet<String>,
    failure: Option<Failure>,
    lifecycle: Lifecycle,
    wake: Arc<tokio::sync::Notify>,
}
impl WorkflowDriver {
    pub fn new(pool: impl Into<String>, config: Config) -> Outcome<Self> {
        config.validate()?;
        let pool = pool.into();
        if pool.trim().is_empty() {
            return Err(Failure::Rejected("Pool identity must be nonempty".into()));
        }
        let scope = Scope::new();
        let (state, receiver) = watch::channel(RuntimeSnapshot {
            pool: pool.clone(),
            lifecycle: Lifecycle::Running,
            workflows: 0,
            waiting: 0,
            held: 0,
            fenced: 0,
        });
        let view = ExecutionView {
            scope,
            state: receiver,
            tasks: Arc::new(Mutex::new(TaskRegistry::new(config.task_history))),
        };
        let (broker, events) = Broker::new();
        Ok(Self {
            view,
            state,
            pool,
            config,
            stop: CancellationToken::new(),
            store: None,
            definitions: Default::default(),
            recovery_ready: true,
            ids: TaskIds::local(),
            pool_uuid: uuid::Uuid::new_v4().as_u128(),
            broker,
            events,
            table: LockTable::default(),
            running: FuturesUnordered::new(),
            services: vec![],
            names: BTreeSet::new(),
            failure: None,
            lifecycle: Lifecycle::Running,
            wake: Arc::default(),
        })
    }
    /// Bind the immutable pool UUID and the allocator supplied by the election authority.
    /// Configure before registering work; ownership/fencing remains the host's responsibility.
    pub fn with_task_ids(mut self, pool_uuid: u128, ids: TaskIds) -> Outcome<Self> {
        if pool_uuid == 0 || !self.running.is_empty() {
            return Err(Failure::Rejected(
                "invalid task identity configuration".into(),
            ));
        }
        self.pool_uuid = pool_uuid;
        self.stop = ids.cancellation();
        self.ids = ids;
        Ok(self)
    }
    pub fn view(&self) -> ExecutionView {
        self.view.clone()
    }
    pub fn is_idle(&self) -> bool {
        self.running.is_empty()
    }
    pub fn register<S: ServiceHandler>(
        &mut self,
        name: impl Into<String>,
        service: S,
    ) -> Outcome<ServiceClient<S>> {
        if self.view.scope.is_closed() {
            return Err(Failure::Unavailable);
        }
        let name = name.into();
        if name.trim().is_empty() || !self.names.insert(name.clone()) {
            return Err(Failure::Rejected(
                "service name empty or already registered".into(),
            ));
        }
        let (client, owner) = service::spawn(
            service,
            name.into(),
            self.view.scope.clone(),
            self.config.capacity,
        );
        self.services.push(owner);
        Ok(client)
    }
    /// Unmanaged standalone use: the driver provides simple logical-object exclusion.
    pub fn submit<W: Workflow>(&mut self, workflow: W) -> Outcome<Run<W::Output>> {
        self.enqueue(workflow, None)
    }
    /// Managed use: the owner retains logical objects and uncertain-result fences.
    /// This is synchronous registration, not a request to another event loop.
    pub fn defer<W: Workflow>(&mut self, workflow: W) -> Outcome<(Run<W::Output>, StartPermit)> {
        let (tx, rx) = oneshot::channel();
        Ok((self.enqueue(workflow, Some(rx))?, StartPermit(tx)))
    }
    pub fn defer_with_metadata<W: Workflow>(
        &mut self,
        workflow: W,
        metadata: serde_json::Value,
    ) -> Outcome<(Run<W::Output>, StartPermit)> {
        let (tx, rx) = oneshot::channel();
        Ok((
            self.enqueue_with(workflow, Some(rx), metadata, None)?,
            StartPermit(tx),
        ))
    }
    fn enqueue<W: Workflow>(
        &mut self,
        workflow: W,
        start: Option<oneshot::Receiver<Outcome<()>>>,
    ) -> Outcome<Run<W::Output>> {
        self.enqueue_with(workflow, start, serde_json::Value::Null, None)
    }
    fn enqueue_with<W: Workflow>(
        &mut self,
        workflow: W,
        start: Option<oneshot::Receiver<Outcome<()>>>,
        metadata: serde_json::Value,
        restored: Option<crate::persistence::SavedTask>,
    ) -> Outcome<Run<W::Output>> {
        if self.view.scope.is_closed() {
            return Err(Failure::Unavailable);
        }
        if W::NAME.trim().is_empty() {
            return Err(Failure::Rejected("workflow name must be nonempty".into()));
        }
        if self.running.len() >= self.config.max_workflows {
            return Err(Failure::Busy);
        }
        if !self.ids.active() {
            return Err(Failure::Unavailable);
        }
        if !self.recovery_ready && restored.is_none() {
            return Err(Failure::Rejected(
                "recover stored tasks before admitting work".into(),
            ));
        }
        let id = match &restored {
            Some(record) => record.id,
            None => self.ids.allocate(self.pool_uuid, W::NAME)?,
        };
        let journal = match workflow.persistence()? {
            Persistence::Memory => {
                if restored.is_some() {
                    return Err(Failure::Rejected(
                        "restored workflow lost persistence declaration".into(),
                    ));
                }
                None
            }
            Persistence::Durable { version, input } => {
                if version == 0 {
                    return Err(Failure::Rejected("invalid workflow schema version".into()));
                }
                let store = self.store.clone().ok_or_else(|| {
                    Failure::Rejected(
                        "persistent workflow requires configured store and leader epoch".into(),
                    )
                })?;
                let record = restored
                    .clone()
                    .unwrap_or_else(|| crate::persistence::SavedTask {
                        id,
                        version,
                        input,
                        checkpoint: serde_json::Value::Null,
                        objects: workflow.objects(),
                        metadata,
                        details: serde_json::Value::Null,
                        revision: 0,
                        owner_epoch: self.ids.epoch(),
                        user_paused: false,
                        cancel_requested: false,
                        outcome: crate::persistence::StoredOutcome::Pending,
                    });
                Some(crate::persistence::Journal {
                    record: Arc::new(tokio::sync::Mutex::new(record)),
                    store,
                })
            }
        };
        let (run, future) = execution(
            workflow,
            ExecutionEnv {
                id,
                wake: self.wake.clone(),
                scope: self.view.scope.clone(),
                pool: self.pool.clone(),
                broker: self.broker.clone(),
                cancel: self.stop.child_token(),
                config: self.config.clone(),
                start,
                journal,
                restored,
            },
        );
        self.view
            .tasks
            .lock()
            .unwrap()
            .insert(self.pool.clone(), run.control.clone());
        self.running.push(future);
        self.publish();
        Ok(run)
    }
    fn publish(&self) {
        let (waiting, held, fenced) = self.table.counts();
        self.state.send_replace(RuntimeSnapshot {
            pool: self.pool.clone(),
            lifecycle: self.lifecycle,
            workflows: self.running.len(),
            waiting,
            held,
            fenced,
        });
    }
    /// Cancellation-safe. Returns after one resource event or completed workflow.
    /// An idle driver waits for a new event; the enclosing select can still accept submissions.
    pub async fn tick(&mut self) -> Option<WorkflowId> {
        let completed = tokio::select! {
            biased;
            _ = self.wake.notified() => None,
            Some(event) = self.events.recv() => { self.table.event(event, &self.broker); None }
            Some(id) = self.running.next(), if !self.running.is_empty() => {
                self.view.tasks.lock().unwrap().finish(id);
                Some(id)
            }
        };
        self.publish();
        completed
    }
    pub fn begin_shutdown(&mut self) {
        self.view.scope.close();
        self.stop.cancel();
        self.lifecycle = Lifecycle::Stopping;
        self.publish();
    }
    /// Poll remaining workflows before shutting their services down. Safe to retry this wait.
    pub async fn shutdown(&mut self) -> Outcome<()> {
        self.begin_shutdown();
        while !self.is_idle() {
            self.tick().await;
        }
        while let Ok(event) = self.events.try_recv() {
            self.table.event(event, &self.broker);
        }
        for service in &mut self.services {
            if let Err(error) = service.shutdown().await {
                self.failure.get_or_insert(error);
            }
        }
        self.lifecycle = Lifecycle::Stopped;
        self.publish();
        self.failure.clone().map_or(Ok(()), Err)
    }
}
impl Drop for WorkflowDriver {
    fn drop(&mut self) {
        self.view.scope.close();
        self.stop.cancel();
        self.lifecycle = Lifecycle::Stopped;
        self.state.send_modify(|s| {
            s.lifecycle = Lifecycle::Stopped;
            s.workflows = 0;
        });
    }
}
