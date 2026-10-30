#[allow(dead_code)]
mod support;
use async_trait::async_trait;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;
use tokio::sync::{Semaphore, mpsc, oneshot};
use workflow_foundation::persistence::{MemoryTaskStore, SavedTask, StoredOutcome, TaskStore};
use workflow_foundation::*;

struct Gate {
    ready: AtomicBool,
    checks: AtomicUsize,
}
impl Gate {
    fn new(ready: bool) -> Arc<Self> {
        Arc::new(Self {
            ready: AtomicBool::new(ready),
            checks: AtomicUsize::new(0),
        })
    }
}
#[async_trait]
impl RunCondition for Gate {
    async fn check(&self) -> Outcome<Readiness> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        Ok(if self.ready.load(Ordering::SeqCst) {
            Readiness::Ready
        } else {
            Readiness::Blocked("topology not ready".into())
        })
    }
}

struct Migration {
    events: mpsc::UnboundedSender<(&'static str, u64)>,
    stopped: Arc<Semaphore>,
}
enum MigrationReply {
    Stopped(u64),
    Done(u64),
}
#[async_trait]
impl ServiceHandler for Migration {
    type Request = u64;
    type Reply = MigrationReply;
    type Query = ();
    type View = ();
    fn resources(_: &u64) -> Vec<ResourceAccess> {
        vec![write("disk")]
    }
    async fn execute(&self, cursor: u64, scope: ActivityContext) -> Outcome<Self::Reply> {
        self.events.send(("started", cursor)).unwrap();
        if cursor != 0 {
            return Ok(MigrationReply::Done(cursor));
        }
        let mut control = scope.control();
        while !control.is_cancelled() && !control.request().paused() {
            let _ = control.changed().await;
        }
        self.events.send(("stop-requested", cursor)).unwrap();
        self.stopped.acquire().await.unwrap().forget();
        if control.is_cancelled() {
            Err(Failure::Cancelled)
        } else {
            Ok(MigrationReply::Stopped(7))
        }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
struct Migrate {
    client: ServiceClient<Migration>,
    cursor: u64,
    durable: bool,
}
#[async_trait]
impl ManagedActivity for Migrate {
    type Reply = MigrationReply;
    type Output = u64;
    fn name(&self) -> &'static str {
        "migrate disks"
    }
    fn resources(&self) -> Vec<ResourceAccess> {
        self.client.resources(&self.cursor)
    }
    async fn execute(&mut self, scope: &mut ActivityScope) -> Outcome<Self::Reply> {
        scope.call(&self.client, self.cursor).await
    }
    fn handle(&mut self, result: Outcome<Self::Reply>) -> Outcome<ActivityAction<u64>> {
        Ok(match result? {
            MigrationReply::Done(value) => ActivityAction::Continue(value),
            MigrationReply::Stopped(cursor) => {
                self.cursor = cursor;
                ActivityAction::Suspend(ActivityWait::Runnable)
            }
        })
    }
    fn checkpoint(&self) -> Outcome<Option<serde_json::Value>> {
        Ok(self
            .durable
            .then(|| serde_json::json!({"stage":"migrate", "cursor":self.cursor})))
    }
}
struct MigrationFlow {
    client: ServiceClient<Migration>,
    advanced: Arc<AtomicBool>,
    durable: bool,
    gate: Option<Arc<Gate>>,
}
#[async_trait]
impl Workflow for MigrationFlow {
    type Output = u64;
    const NAME: &'static str = "managed-migration";
    fn persistence(&self) -> Outcome<Persistence> {
        if self.durable {
            Persistence::durable(1, &())
        } else {
            Ok(Persistence::Memory)
        }
    }
    fn run_condition(&self) -> Option<Arc<dyn RunCondition>> {
        self.gate.clone().map(|gate| gate as Arc<dyn RunCondition>)
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<u64> {
        let cursor = ctx
            .restored_checkpoint()
            .get("cursor")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let result = ctx
            .run_managed_activity(Migrate {
                client: self.client,
                cursor,
                durable: self.durable,
            })
            .await?;
        self.advanced.store(true, Ordering::SeqCst);
        Ok(result)
    }
}
async fn event(events: &mut mpsc::UnboundedReceiver<(&'static str, u64)>) -> (&'static str, u64) {
    tokio::time::timeout(LIMIT, events.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn suspended(control: &RunControl) {
    tokio::time::timeout(LIMIT, control.wait_suspended())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn workflow_stays_at_await_while_topology_runs_then_rechecks_admission_and_continues() {
    let (mut runtime, probe, _) = setup();
    let (events, mut observed) = mpsc::unbounded_channel();
    let stopped = Arc::new(Semaphore::new(0));
    let client = runtime
        .register(
            "migration",
            Migration {
                events,
                stopped: stopped.clone(),
            },
        )
        .unwrap();
    let advanced = Arc::new(AtomicBool::new(false));
    let gate = Gate::new(true);
    let mut run = runtime
        .submit(MigrationFlow {
            client,
            advanced: advanced.clone(),
            durable: false,
            gate: Some(gate.clone()),
        })
        .await
        .unwrap();
    let id = run.id();
    assert_eq!(event(&mut observed).await, ("started", 0));
    run.control
        .set_policy_suspensions(&["topology".into()].into());
    assert_eq!(event(&mut observed).await, ("stop-requested", 0));
    let mut topo = runtime
        .submit(Call::new(&probe, Work::write("topology", "disk")))
        .await
        .unwrap();
    state(&topo.control, RunState::WaitingResources).await;
    assert!(!run.control.suspend_acknowledged());
    assert!(!advanced.load(Ordering::SeqCst));
    gate.ready.store(false, Ordering::SeqCst);
    stopped.add_permits(1);
    suspended(&run.control).await;
    finish(&mut topo).await.unwrap();
    assert!(!advanced.load(Ordering::SeqCst));
    run.control.set_policy_suspensions(&Default::default());
    state(
        &run.control,
        RunState::WaitingConditions("topology not ready".into()),
    )
    .await;
    assert!(observed.try_recv().is_err());
    gate.ready.store(true, Ordering::SeqCst);
    run.control.recheck();
    assert_eq!(finish(&mut run).await.unwrap(), 7);
    assert_eq!(event(&mut observed).await, ("started", 7));
    assert!(advanced.load(Ordering::SeqCst));
    assert_eq!(run.id(), id);
    assert_eq!(run.control.activities().total, 2);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancel_waits_for_native_stop_and_never_advances_the_workflow() {
    let (mut runtime, probe, _) = setup();
    let (events, mut observed) = mpsc::unbounded_channel();
    let stopped = Arc::new(Semaphore::new(0));
    let client = runtime
        .register(
            "migration",
            Migration {
                events,
                stopped: stopped.clone(),
            },
        )
        .unwrap();
    let advanced = Arc::new(AtomicBool::new(false));
    let mut run = runtime
        .submit(MigrationFlow {
            client,
            advanced: advanced.clone(),
            durable: false,
            gate: None,
        })
        .await
        .unwrap();
    event(&mut observed).await;
    run.control.cancel();
    assert_eq!(event(&mut observed).await, ("stop-requested", 0));
    let mut other = runtime
        .submit(Call::new(&probe, Work::write("other", "disk")))
        .await
        .unwrap();
    state(&other.control, RunState::WaitingResources).await;
    stopped.add_permits(1);
    assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
    finish(&mut other).await.unwrap();
    assert!(!advanced.load(Ordering::SeqCst));
    assert!(run.control.resume().is_err());
    assert!(observed.try_recv().is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn resume_racing_the_stop_reply_keeps_the_continuation_in_the_same_step() {
    let (mut runtime, _, _) = setup();
    let (events, mut observed) = mpsc::unbounded_channel();
    let stopped = Arc::new(Semaphore::new(0));
    let client = runtime
        .register(
            "migration",
            Migration {
                events,
                stopped: stopped.clone(),
            },
        )
        .unwrap();
    let mut run = runtime
        .submit(MigrationFlow {
            client,
            advanced: Arc::new(AtomicBool::new(false)),
            durable: false,
            gate: None,
        })
        .await
        .unwrap();
    event(&mut observed).await;
    run.control.suspend().unwrap();
    event(&mut observed).await;
    run.control.resume().unwrap();
    stopped.add_permits(1);
    assert_eq!(finish(&mut run).await.unwrap(), 7);
    assert_eq!(event(&mut observed).await, ("started", 7));
    runtime.shutdown().await.unwrap();
}

struct OneStep<A>(A);
#[async_trait]
impl<A: ManagedActivity> Workflow for OneStep<A> {
    type Output = A::Output;
    const NAME: &'static str = "managed-one-step";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Self::Output> {
        ctx.run_managed_activity(self.0).await
    }
}
struct WaitBusiness {
    gate: Arc<Gate>,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl ManagedActivity for WaitBusiness {
    type Reply = bool;
    type Output = usize;
    fn name(&self) -> &'static str {
        "business wait"
    }
    fn resources(&self) -> Vec<ResourceAccess> {
        vec![write("disk")]
    }
    async fn execute(&mut self, _: &mut ActivityScope) -> Outcome<bool> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(self.gate.ready.load(Ordering::SeqCst))
    }
    fn handle(&mut self, result: Outcome<bool>) -> Outcome<ActivityAction<usize>> {
        Ok(if result? {
            ActivityAction::Continue(self.calls.load(Ordering::SeqCst))
        } else {
            ActivityAction::Suspend(ActivityWait::Condition(self.gate.clone()))
        })
    }
}
#[tokio::test]
async fn business_condition_releases_resources_and_survives_external_pause_without_polling_execute()
{
    let (mut runtime, probe, _) = setup();
    let gate = Gate::new(false);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut run = runtime
        .submit(OneStep(WaitBusiness {
            gate: gate.clone(),
            calls: calls.clone(),
        }))
        .await
        .unwrap();
    state(
        &run.control,
        RunState::WaitingConditions("topology not ready".into()),
    )
    .await;
    let mut other = runtime
        .submit(Call::new(&probe, Work::write("other", "disk")))
        .await
        .unwrap();
    finish(&mut other).await.unwrap();
    run.control.suspend().unwrap();
    suspended(&run.control).await;
    run.control.resume().unwrap();
    state(
        &run.control,
        RunState::WaitingConditions("topology not ready".into()),
    )
    .await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    gate.ready.store(true, Ordering::SeqCst);
    run.control.recheck();
    assert_eq!(finish(&mut run).await.unwrap(), 2);
    runtime.shutdown().await.unwrap();
}

struct Pending {
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl ManagedActivity for Pending {
    type Reply = ();
    type Output = ();
    fn name(&self) -> &'static str {
        "pending"
    }
    fn retry(&self) -> Option<Retry> {
        Some(Retry::interval(Duration::from_millis(100)).max_attempts(2))
    }
    async fn execute(&mut self, _: &mut ActivityScope) -> Outcome<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn handle(&mut self, result: Outcome<()>) -> Outcome<ActivityAction<()>> {
        result?;
        Ok(ActivityAction::Retry {
            reason: "not ready".into(),
        })
    }
}
#[tokio::test]
async fn retry_budget_is_not_reset_by_suspension() {
    let (mut runtime, _, _) = setup();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut run = runtime
        .submit(OneStep(Pending {
            calls: calls.clone(),
        }))
        .await
        .unwrap();
    let mut observer = runtime.client().observe_task(run.id()).unwrap();
    tokio::time::timeout(LIMIT, async {
        while !observer
            .snapshot()
            .activities
            .entries
            .back()
            .is_some_and(|s| s.retry_at.is_some())
        {
            observer.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    run.control.suspend().unwrap();
    suspended(&run.control).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    run.control.resume().unwrap();
    assert!(
        matches!(finish(&mut run).await,Err(Failure::Failed(s)) if s.contains("retry limit exhausted"))
    );
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    runtime.shutdown().await.unwrap();
}

struct Poison {
    client: ServiceClient<Probe>,
    handled: Arc<AtomicBool>,
}
#[async_trait]
impl ManagedActivity for Poison {
    type Reply = ();
    type Output = ();
    fn name(&self) -> &'static str {
        "poison"
    }
    fn resources(&self) -> Vec<ResourceAccess> {
        vec![write("disk")]
    }
    async fn execute(&mut self, scope: &mut ActivityScope) -> Outcome<()> {
        let _ = scope
            .call(
                &self.client,
                Work::write("unknown", "disk").behavior(Behavior::Uncertain),
            )
            .await;
        Ok(())
    }
    fn handle(&mut self, _: Outcome<()>) -> Outcome<ActivityAction<()>> {
        self.handled.store(true, Ordering::SeqCst);
        Ok(ActivityAction::Continue(()))
    }
}
#[tokio::test]
async fn unknown_effects_bypass_handle_even_when_execute_swallows_the_error() {
    let (mut runtime, client, _) = setup();
    let handled = Arc::new(AtomicBool::new(false));
    let mut run = runtime
        .submit(OneStep(Poison {
            client: client.clone(),
            handled: handled.clone(),
        }))
        .await
        .unwrap();
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    assert!(!handled.load(Ordering::SeqCst));
    let mut other = runtime
        .submit(Call::new(&client, Work::write("other", "disk")))
        .await
        .unwrap();
    assert!(matches!(
        finish(&mut other).await,
        Err(Failure::Rejected(_))
    ));
    runtime.shutdown().await.unwrap();
}

struct NoWake;
#[async_trait]
impl ManagedActivity for NoWake {
    type Reply = ();
    type Output = ();
    fn name(&self) -> &'static str {
        "no wake source"
    }
    async fn execute(&mut self, _: &mut ActivityScope) -> Outcome<()> {
        Ok(())
    }
    fn handle(&mut self, _: Outcome<()>) -> Outcome<ActivityAction<()>> {
        Ok(ActivityAction::Suspend(ActivityWait::Runnable))
    }
}
#[tokio::test]
async fn suspension_without_a_control_request_or_condition_does_not_spin() {
    let (mut runtime, _, _) = setup();
    let mut run = runtime.submit(OneStep(NoWake)).await.unwrap();
    assert!(matches!(finish(&mut run).await, Err(Failure::Rejected(_))));
    assert_eq!(run.control.activities().total, 1);
    runtime.shutdown().await.unwrap();
}

struct CheckpointStore {
    inner: MemoryTaskStore,
    fail: bool,
}
impl TaskStore for CheckpointStore {
    fn claim(&self, pool: u128, epoch: u64) -> Outcome<()> {
        self.inner.claim(pool, epoch)
    }
    fn list(&self, pool: u128) -> Outcome<Vec<SavedTask>> {
        self.inner.list(pool)
    }
    fn save(&self, task: SavedTask, previous: Option<u64>) -> Outcome<()> {
        if self.fail && !task.checkpoint.is_null() {
            return Err(Failure::Unavailable);
        }
        self.inner.save(task, previous)
    }
}
#[tokio::test]
async fn checkpoint_must_commit_before_pause_acknowledgement() {
    for fail in [false, true] {
        let store = Arc::new(CheckpointStore {
            inner: MemoryTaskStore::default(),
            fail,
        });
        let mut driver = WorkflowDriver::new("p", Config::default())
            .unwrap()
            .with_task_ids(42, TaskIds::for_leader(1).unwrap())
            .unwrap();
        driver.enable_persistence(store.clone()).await.unwrap();
        driver.restore().await.unwrap();
        let (events, mut observed) = mpsc::unbounded_channel();
        let stopped = Arc::new(Semaphore::new(0));
        let client = driver
            .register(
                "migration",
                Migration {
                    events,
                    stopped: stopped.clone(),
                },
            )
            .unwrap();
        let mut run = driver
            .submit(MigrationFlow {
                client,
                advanced: Arc::new(AtomicBool::new(false)),
                durable: true,
                gate: None,
            })
            .unwrap();
        let driving = tokio::spawn(async move {
            while !driver.is_idle() {
                driver.tick().await;
            }
            driver.shutdown().await.unwrap();
        });
        event(&mut observed).await;
        run.control.suspend().unwrap();
        event(&mut observed).await;
        stopped.add_permits(1);
        if fail {
            assert!(finish(&mut run).await.is_err());
            assert!(!run.control.suspend_acknowledged());
            assert!(store.get(run.id()).unwrap().unwrap().checkpoint.is_null());
            assert!(observed.try_recv().is_err());
        } else {
            suspended(&run.control).await;
            assert_eq!(
                store.get(run.id()).unwrap().unwrap().checkpoint,
                serde_json::json!({"stage":"migrate","cursor":7})
            );
            run.control.resume().unwrap();
            assert_eq!(finish(&mut run).await.unwrap(), 7);
        }
        driving.await.unwrap();
    }
}

struct CancelStep;
#[async_trait]
impl ManagedActivity for CancelStep {
    type Reply = ();
    type Output = ();
    fn name(&self) -> &'static str {
        "cancel task"
    }
    async fn execute(&mut self, _: &mut ActivityScope) -> Outcome<()> {
        Ok(())
    }
    fn handle(&mut self, _: Outcome<()>) -> Outcome<ActivityAction<()>> {
        Ok(ActivityAction::Cancel)
    }
}
struct DurableCancel;
#[async_trait]
impl Workflow for DurableCancel {
    type Output = ();
    const NAME: &'static str = "durable-cancel";
    fn persistence(&self) -> Outcome<Persistence> {
        Persistence::durable(1, &())
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.run_managed_activity(CancelStep).await
    }
}
#[tokio::test]
async fn cancel_action_is_terminal_in_storage_and_is_not_recovered_as_host_interruption() {
    let store = Arc::new(MemoryTaskStore::default());
    let mut driver = WorkflowDriver::new("p", Config::default())
        .unwrap()
        .with_task_ids(42, TaskIds::for_leader(1).unwrap())
        .unwrap();
    driver.enable_persistence(store.clone()).await.unwrap();
    driver.restore().await.unwrap();
    let mut run = driver.submit(DurableCancel).unwrap();
    while !driver.is_idle() {
        driver.tick().await;
    }
    assert_eq!(run.result().await, Err(Failure::Cancelled));
    assert_eq!(
        store.get(run.id()).unwrap().unwrap().outcome,
        StoredOutcome::Cancelled
    );
    assert!(run.control.resume().is_err());
    driver.shutdown().await.unwrap();
    let mut next = WorkflowDriver::new("p", Config::default())
        .unwrap()
        .with_task_ids(42, TaskIds::for_leader(2).unwrap())
        .unwrap();
    next.enable_persistence(store).await.unwrap();
    assert!(next.restore().await.unwrap().is_empty());
    next.shutdown().await.unwrap();
}

struct AbandonFlow {
    step: Migrate,
    abandon: oneshot::Receiver<()>,
}
#[async_trait]
impl Workflow for AbandonFlow {
    type Output = ();
    const NAME: &'static str = "abandon-managed-waiter";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        tokio::select! {
            result=ctx.run_managed_activity(self.step)=>{result?;},
            _=self.abandon=>{},
        }
        Ok(())
    }
}
#[tokio::test]
async fn abandoning_the_outer_waiter_does_not_drop_native_work_or_release_its_lease_early() {
    let (mut runtime, probe, _) = setup();
    let (events, mut observed) = mpsc::unbounded_channel();
    let stopped = Arc::new(Semaphore::new(0));
    let client = runtime
        .register(
            "migration",
            Migration {
                events,
                stopped: stopped.clone(),
            },
        )
        .unwrap();
    let (abandon, abandoned) = oneshot::channel();
    let mut run = runtime
        .submit(AbandonFlow {
            step: Migrate {
                client,
                cursor: 0,
                durable: false,
            },
            abandon: abandoned,
        })
        .await
        .unwrap();
    event(&mut observed).await;
    abandon.send(()).unwrap();
    assert_eq!(event(&mut observed).await, ("stop-requested", 0));
    let mut other = runtime
        .submit(Call::new(&probe, Work::write("other", "disk")))
        .await
        .unwrap();
    state(&other.control, RunState::WaitingResources).await;
    assert_eq!(
        run.control.activities().entries[0].state,
        ActivityState::Running
    );
    stopped.add_permits(1);
    finish(&mut run).await.unwrap();
    finish(&mut other).await.unwrap();
    assert_eq!(
        run.control.activities().entries[0].state,
        ActivityState::Failed(Failure::Cancelled)
    );
    runtime.shutdown().await.unwrap();
}
