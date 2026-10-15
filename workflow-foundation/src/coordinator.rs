use crate::{
    admission::Admission,
    locks::{Broker, LockTable},
    model::Scope,
    service::{self, ServiceOwner},
    tasks::TaskRegistry,
    workflow::{ExecutionEnv, execution},
    *,
};
use futures_util::{StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{
    collections::{BTreeMap, BTreeSet},
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

struct Submission {
    future: BoxFuture<'static, WorkflowId>,
    reply: oneshot::Sender<Outcome<()>>,
    control: RunControl,
    admission: Option<Admission>,
}
#[derive(Clone)]
pub struct CoordinatorClient {
    pool: String,
    scope: Arc<Scope>,
    broker: Broker,
    tx: mpsc::Sender<Submission>,
    next: Arc<AtomicU64>,
    stop: CancellationToken,
    config: Config,
    state: watch::Receiver<CoordinatorSnapshot>,
    tasks: Arc<Mutex<TaskRegistry>>,
}
impl CoordinatorClient {
    pub fn tasks(&self) -> Vec<TaskSnapshot> {
        self.tasks.lock().unwrap().snapshots()
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
    pub fn cancel(&self, id: WorkflowId) -> Outcome<()> {
        self.control(id)?.cancel();
        Ok(())
    }
    pub async fn wait_suspended(&self, id: WorkflowId) -> Outcome<()> {
        self.control(id)?.wait_paused().await
    }
    pub fn snapshot(&self) -> CoordinatorSnapshot {
        self.state.borrow().clone()
    }
    pub async fn submit<W: Workflow>(&self, workflow: W) -> Outcome<Run<W::Output>> {
        self.submit_inner(workflow, None).await
    }
    pub async fn submit_with_policy<
        W: Workflow,
        D: Clone + Send + Sync + 'static,
        P: AdmissionPolicy<D> + ?Sized,
    >(
        &self,
        workflow: W,
        descriptor: D,
        policy: Arc<P>,
    ) -> Outcome<Run<W::Output>> {
        self.submit_inner(workflow, Some(Admission::new(descriptor, policy)))
            .await
    }
    async fn submit_inner<W: Workflow>(
        &self,
        workflow: W,
        admission: Option<Admission>,
    ) -> Outcome<Run<W::Output>> {
        if self.scope.is_closed() {
            return Err(Failure::Unavailable);
        }
        if W::NAME.trim().is_empty() {
            return Err(Failure::Rejected("workflow name must be nonempty".into()));
        }
        let id = WorkflowId(self.next.fetch_add(1, Ordering::Relaxed));
        let cancel = self.stop.child_token();
        let (run, future) = execution(
            workflow,
            ExecutionEnv {
                id,
                scope: self.scope.clone(),
                pool: self.pool.clone(),
                broker: self.broker.clone(),
                cancel: cancel.clone(),
                config: self.config.clone(),
            },
        );
        let (reply, accepted) = oneshot::channel();
        self.tx
            .send(Submission {
                future,
                reply,
                control: run.control.clone(),
                admission,
            })
            .await
            .map_err(|_| Failure::Unavailable)?;
        accepted.await.unwrap_or(Err(Failure::Unavailable))?;
        Ok(run)
    }
}

/// Owns one Pool's coordinator and registered domain hosts. No business objects.
pub struct Coordinator {
    client: CoordinatorClient,
    task: Option<JoinHandle<()>>,
    services: Vec<ServiceOwner>,
    names: BTreeSet<String>,
    failure: Option<Failure>,
}

struct StateGuard {
    state: watch::Sender<CoordinatorSnapshot>,
    scope: Arc<Scope>,
    stop: CancellationToken,
}
impl Drop for StateGuard {
    fn drop(&mut self) {
        self.scope.close();
        self.stop.cancel();
        self.state.send_modify(|state| {
            state.lifecycle = Lifecycle::Stopped;
            state.workflows = 0;
        });
    }
}
impl Coordinator {
    pub fn start(pool: impl Into<String>, config: Config) -> Outcome<Self> {
        config.validate()?;
        let pool = pool.into();
        if pool.trim().is_empty() {
            return Err(Failure::Rejected("Pool identity must be nonempty".into()));
        }
        let scope = Scope::new();
        let (broker, mut events) = Broker::new();
        let (tx, mut rx) = mpsc::channel::<Submission>(config.capacity);
        let stop = CancellationToken::new();
        let (state_tx, state) = watch::channel(CoordinatorSnapshot {
            pool: pool.clone(),
            lifecycle: Lifecycle::Running,
            workflows: 0,
            waiting: 0,
            held: 0,
            fenced: 0,
        });
        let tasks = Arc::new(Mutex::new(TaskRegistry::new(config.task_history)));
        let task_records = tasks.clone();
        let task_pool = pool.clone();
        let client = CoordinatorClient {
            pool,
            scope: scope.clone(),
            broker: broker.clone(),
            tx,
            next: Arc::new(AtomicU64::new(1)),
            stop: stop.clone(),
            config: config.clone(),
            state,
            tasks,
        };
        let task = tokio::spawn(async move {
            let state_guard = StateGuard {
                state: state_tx,
                scope: scope.clone(),
                stop: stop.clone(),
            };
            let state_tx = &state_guard.state;
            let mut table = LockTable::default();
            let mut running: FuturesUnordered<BoxFuture<'static, WorkflowId>> =
                FuturesUnordered::new();
            let mut active: BTreeMap<WorkflowId, (RunControl, Option<Admission>)> = BTreeMap::new();
            let mut closing = false;
            loop {
                let (waiting, held, fenced) = table.counts();
                state_tx.send_modify(|s| {
                    s.lifecycle = if closing {
                        Lifecycle::Stopping
                    } else {
                        Lifecycle::Running
                    };
                    s.workflows = running.len();
                    s.waiting = waiting;
                    s.held = held;
                    s.fenced = fenced;
                });
                if closing && running.is_empty() {
                    while let Ok(event) = events.try_recv() {
                        table.event(event, &broker);
                    }
                    let (waiting, held, fenced) = table.counts();
                    state_tx.send_modify(|s| {
                        s.lifecycle = Lifecycle::Stopped;
                        s.workflows = 0;
                        s.waiting = waiting;
                        s.held = held;
                        s.fenced = fenced;
                    });
                    break;
                }
                tokio::select! {
                    biased;
                    _ = stop.cancelled(), if !closing => {
                        scope.close(); closing = true; rx.close();
                        while let Ok(submission) = rx.try_recv() { let _ = submission.reply.send(Err(Failure::Unavailable)); }
                    }
                    event = events.recv() => { if let Some(event) = event { table.event(event, &broker); } }
                    Some(id) = running.next(), if !running.is_empty() => { active.remove(&id); task_records.lock().unwrap().finish(id); }
                    submission = rx.recv(), if !closing => match submission {
                        None => { scope.close(); closing = true; stop.cancel(); }
                        Some(submission) => {
                            if running.len() >= config.max_workflows { let _ = submission.reply.send(Err(Failure::Busy)); }
                            else {
                                // Decide against one serialized admission snapshot. Never cancel
                                // old work if any rule rejects or if the new run cannot be accepted.
                                let decision = std::panic::catch_unwind(AssertUnwindSafe(|| {
                                    let mut victims = Vec::new();
                                    let mut evaluations = Vec::new();
                                    if let Some(incoming) = &submission.admission {
                                        for (id, (control, descriptor)) in &active {
                                            if matches!(control.state(), RunState::Completed | RunState::Failed(_)) { continue; }
                                            if let Some(existing) = descriptor {
                                                let evaluation = incoming.evaluate(existing);
                                                evaluations.push(AdmissionMatch { existing: *id, evaluation: evaluation.clone() });
                                                match evaluation.decision {
                                                    AdmissionDecision::Queue => {}
                                                    AdmissionDecision::CancelExisting => victims.push(*id),
                                                    AdmissionDecision::Reject(reason) => return Err(Failure::Rejected(reason)),
                                                }
                                            }
                                        }
                                    }
                                    Ok((victims, evaluations))
                                })).unwrap_or_else(|_| Err(Failure::Rejected("admission policy panicked".into())));
                                let (victims, evaluations) = match decision {
                                    Ok(result) => result,
                                    Err(failure) => { let _ = submission.reply.send(Err(failure)); continue; }
                                };
                                task_records.lock().unwrap().insert(task_pool.clone(), submission.control.clone(), evaluations);
                                active.insert(submission.control.id, (submission.control, submission.admission));
                                running.push(submission.future);
                                for id in victims { if let Some((control, _)) = active.get(&id) { control.cancel(); } }
                                let _ = submission.reply.send(Ok(()));
                            }
                        }
                    }
                }
            }
        });
        Ok(Self {
            client,
            task: Some(task),
            services: vec![],
            names: BTreeSet::new(),
            failure: None,
        })
    }
    pub fn client(&self) -> CoordinatorClient {
        self.client.clone()
    }
    pub fn register<S: DomainService>(
        &mut self,
        name: impl Into<String>,
        service: S,
    ) -> Outcome<ServiceClient<S>> {
        if self.client.scope.is_closed() {
            return Err(Failure::Unavailable);
        }
        let name = name.into();
        if name.trim().is_empty() || !self.names.insert(name) {
            return Err(Failure::Rejected(
                "service name empty or already registered".into(),
            ));
        }
        let (client, owner) = service::spawn(
            service,
            self.client.scope.clone(),
            self.client.config.capacity,
        );
        self.services.push(owner);
        Ok(client)
    }
    pub async fn submit<W: Workflow>(&self, workflow: W) -> Outcome<Run<W::Output>> {
        self.client.submit(workflow).await
    }
    /// Safe to cancel and retry this wait; ownership is never moved out before await.
    pub async fn shutdown(&mut self) -> Outcome<()> {
        self.client.scope.close();
        self.client.stop.cancel();
        if let Some(task) = &mut self.task {
            if task.await.is_err() {
                self.failure
                    .get_or_insert(Failure::Uncertain("coordinator host terminated".into()));
            }
            self.task.take();
        }
        for service in &mut self.services {
            if let Err(e) = service.shutdown().await {
                self.failure.get_or_insert(e);
            }
        }
        match self.failure.clone() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}
impl Drop for Coordinator {
    fn drop(&mut self) {
        self.client.scope.close();
        self.client.stop.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
