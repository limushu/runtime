//! Execution mechanics. WorkflowDriver is polled by its owner; it never spawns itself.
//! WorkflowRuntime is a standalone convenience host, not used inside storage Pool.
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
use tokio::sync::{Notify, mpsc, oneshot, watch};
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
    pub fn recheck(&self, id: WorkflowId) -> Outcome<()> {
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
    wake: Arc<Notify>,
    view: ExecutionView,
    state: watch::Sender<RuntimeSnapshot>,
    pool: String,
    config: Config,
    stop: CancellationToken,
    next: u64,
    broker: Broker,
    events: mpsc::UnboundedReceiver<Event>,
    table: LockTable,
    running: FuturesUnordered<BoxFuture<'static, WorkflowId>>,
    services: Vec<ServiceOwner>,
    names: BTreeSet<String>,
    failure: Option<Failure>,
    lifecycle: Lifecycle,
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
            wake: Arc::new(Notify::new()),
            view,
            state,
            pool,
            config,
            stop: CancellationToken::new(),
            next: 1,
            broker,
            events,
            table: LockTable::default(),
            running: FuturesUnordered::new(),
            services: vec![],
            names: BTreeSet::new(),
            failure: None,
            lifecycle: Lifecycle::Running,
        })
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
        self.enqueue(workflow, None, ExecutionOptions::default())
    }
    /// Managed use: the owner retains logical objects and uncertain-result fences.
    /// This is synchronous registration, not a request to another event loop.
    pub fn defer<W: Workflow>(&mut self, workflow: W) -> Outcome<(Run<W::Output>, StartPermit)> {
        self.defer_with_options(workflow, ExecutionOptions::default())
    }
    pub fn defer_with_options<W: Workflow>(
        &mut self,
        workflow: W,
        options: ExecutionOptions,
    ) -> Outcome<(Run<W::Output>, StartPermit)> {
        let (tx, rx) = oneshot::channel();
        Ok((self.enqueue(workflow, Some(rx), options)?, StartPermit(tx)))
    }
    fn enqueue<W: Workflow>(
        &mut self,
        workflow: W,
        start: Option<oneshot::Receiver<Outcome<()>>>,
        options: ExecutionOptions,
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
        let id = WorkflowId(self.next);
        self.next += 1;
        let (run, future) = execution(
            workflow,
            ExecutionEnv {
                id,
                scope: self.view.scope.clone(),
                pool: self.pool.clone(),
                broker: self.broker.clone(),
                cancel: self.stop.child_token(),
                config: self.config.clone(),
                start,
                options,
                wake: self.wake.clone(),
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
            Some(event) = self.events.recv() => { self.table.event(event, &self.broker); None }
            Some(id) = self.running.next(), if !self.running.is_empty() => {
                self.view.tasks.lock().unwrap().finish(id);
                Some(id)
            }
            _ = self.wake.notified() => None,
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
