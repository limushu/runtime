use async_trait::async_trait;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};
use workflow_foundation::{persistence::*, *};

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("protected step stalled")
}
async fn state(control: &RunControl, expected: RunState) {
    let mut control = control.clone();
    bounded(async {
        while control.state() != expected {
            control.changed().await.unwrap();
        }
    })
    .await;
}
enum Reply {
    Done(u64),
    Busy,
    Stopped(u64),
}
struct Call {
    cursor: u64,
    control: ControlToken,
    finish: oneshot::Sender<Outcome<Reply>>,
}
struct Worker(mpsc::UnboundedSender<Call>);
#[async_trait]
impl ServiceHandler for Worker {
    type Request = u64;
    type Reply = Reply;
    type Query = ();
    type View = ();
    fn resources(_: &u64) -> Vec<ResourceAccess> {
        vec![ResourceAccess::write(ObjectKey::new("disk", "a"))]
    }
    async fn execute(&self, cursor: u64, ctx: ActivityContext) -> Outcome<Reply> {
        let control = ctx.control();
        let (finish, response) = oneshot::channel();
        self.0
            .send(Call {
                cursor,
                control: control.clone(),
                finish,
            })
            .ok()
            .unwrap();
        tokio::select! {
            _ = control.cancelled() => Err(Failure::Cancelled),
            reply = response => reply.map_err(|_| Failure::Unavailable)?,
        }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
struct Ready(AtomicBool);
#[async_trait]
impl RunCondition for Ready {
    async fn check(&self) -> Outcome<Readiness> {
        Ok(if self.0.load(Ordering::SeqCst) {
            Readiness::Ready
        } else {
            Readiness::Blocked("not ready".into())
        })
    }
}
struct Protected {
    client: ServiceClient<Worker>,
    ready: Option<Arc<Ready>>,
    cursor: u64,
    deliberate_cancel: bool,
}
impl Protected {
    fn new(client: &ServiceClient<Worker>) -> Self {
        Self {
            client: client.clone(),
            ready: None,
            cursor: 0,
            deliberate_cancel: false,
        }
    }
}
#[async_trait]
impl Activity for Protected {
    type Reply = Reply;
    type Output = u64;
    fn name(&self) -> &'static str {
        "commit.membership"
    }
    fn cancellable(&self) -> bool {
        false
    }
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::fixed(1, Duration::from_millis(80))
    }
    fn run_condition(&self) -> Option<Arc<dyn RunCondition>> {
        self.ready
            .clone()
            .map(|ready| ready as Arc<dyn RunCondition>)
    }
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<Reply> {
        scope.call(&self.client, self.cursor).await
    }
    fn handle(&mut self, result: Outcome<Reply>) -> Outcome<ActivityAction<u64>> {
        if self.deliberate_cancel {
            result?;
            return Ok(ActivityAction::Cancel);
        }
        Ok(match result? {
            Reply::Done(value) => ActivityAction::Continue(value),
            Reply::Busy => ActivityAction::Retry {
                reason: "busy".into(),
            },
            Reply::Stopped(cursor) => {
                self.cursor = cursor;
                ActivityAction::Suspend(ActivityWait::Resume)
            }
        })
    }
}
struct Flow {
    step: Protected,
    next: bool,
    returned: Arc<AtomicBool>,
    durable: bool,
}
impl Flow {
    fn new(client: &ServiceClient<Worker>) -> Self {
        Self {
            step: Protected::new(client),
            next: false,
            returned: Arc::default(),
            durable: false,
        }
    }
}
#[async_trait]
impl Workflow for Flow {
    type Output = u64;
    const NAME: &'static str = "protected-activity";
    fn persistence(&self) -> Outcome<Persistence> {
        if self.durable {
            Persistence::durable(1, &())
        } else {
            Ok(Persistence::Memory)
        }
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<u64> {
        let client = self.step.client.clone();
        let value = ctx.run_activity(self.step).await?;
        self.returned.store(true, Ordering::SeqCst);
        if self.next {
            ctx.activity(&client, 999).await?;
        }
        Ok(value)
    }
}
fn setup() -> (
    WorkflowRuntime,
    ServiceClient<Worker>,
    mpsc::UnboundedReceiver<Call>,
) {
    let mut runtime = WorkflowRuntime::start("pool", Config::default()).unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    let client = runtime.register("worker", Worker(tx)).unwrap();
    (runtime, client, rx)
}
fn finish(call: Call, reply: Reply) {
    call.finish.send(Ok(reply)).ok().unwrap();
}

#[tokio::test]
async fn a_protected_step_is_cancellable_until_its_first_execute_entry() {
    let (mut runtime, client, mut calls) = setup();
    let mut flow = Flow::new(&client);
    flow.step.ready = Some(Arc::new(Ready(AtomicBool::new(false))));
    let mut run = runtime.submit(flow).await.unwrap();
    state(
        &run.control,
        RunState::WaitingConditions("not ready".into()),
    )
    .await;
    run.control.cancel();
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    let root = run.control.activities().entries[0].clone();
    assert!(!root.cancellable);
    assert_eq!(root.attempts, 0);
    assert!(calls.try_recv().is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_is_deferred_but_pause_and_condition_checks_still_work() {
    let (mut runtime, client, mut calls) = setup();
    let ready = Arc::new(Ready(AtomicBool::new(true)));
    let mut flow = Flow::new(&client);
    flow.step.ready = Some(ready.clone());
    flow.next = true;
    let returned = flow.returned.clone();
    let mut run = runtime.submit(flow).await.unwrap();
    let first = bounded(calls.recv()).await.unwrap();
    run.control.cancel();
    assert!(run.control.cancellation_requested());
    assert!(!first.control.is_cancelled());
    run.control.suspend().unwrap();
    assert!(first.control.request().paused());
    assert!(!run.control.suspend_acknowledged());
    finish(first, Reply::Stopped(7));
    bounded(run.control.wait_suspended()).await.unwrap();
    ready.0.store(false, Ordering::SeqCst);
    run.control.resume().unwrap();
    state(
        &run.control,
        RunState::WaitingConditions("not ready".into()),
    )
    .await;
    assert!(calls.try_recv().is_err());
    ready.0.store(true, Ordering::SeqCst);
    run.control.recheck();
    let second = bounded(calls.recv()).await.unwrap();
    assert_eq!(second.cursor, 7);
    assert!(!second.control.is_cancelled());
    finish(second, Reply::Done(12));
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert!(
        returned.load(Ordering::SeqCst),
        "WF receives the actual completed result"
    );
    assert!(
        calls.try_recv().is_err(),
        "the next ordinary step must not start"
    );
    let root = run.control.activities().entries[0].clone();
    assert!(!root.cancellable);
    assert_eq!(root.state, ActivityState::Completed);
    assert_eq!(root.attempts, 2);
    assert_eq!(root.retries, 0);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_during_backoff_does_not_skip_the_protected_retry() {
    let (mut runtime, client, mut calls) = setup();
    let mut run = runtime.submit(Flow::new(&client)).await.unwrap();
    finish(bounded(calls.recv()).await.unwrap(), Reply::Busy);
    state(&run.control, RunState::WaitingTimer).await;
    run.control.cancel();
    let retry = bounded(calls.recv()).await.unwrap();
    assert!(!retry.control.is_cancelled());
    finish(retry, Reply::Done(12));
    assert_eq!(bounded(run.result()).await, Ok(12));
    assert!(run.control.cancellation_requested());
    assert_eq!(run.control.activities().entries[0].retries, 1);
    runtime.shutdown().await.unwrap();
}

struct Ordinary(ServiceClient<Worker>);
#[async_trait]
impl Workflow for Ordinary {
    type Output = ();
    const NAME: &'static str = "ordinary";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.activity(&self.0, 999).await.map(|_| ())
    }
}
#[tokio::test]
async fn cancellation_after_entry_does_not_skip_a_call_waiting_for_resources() {
    let (mut runtime, client, mut calls) = setup();
    let mut holder = runtime.submit(Ordinary(client.clone())).await.unwrap();
    let holding = bounded(calls.recv()).await.unwrap();
    let mut flow = Flow::new(&client);
    flow.next = true;
    let mut run = runtime.submit(flow).await.unwrap();
    state(&run.control, RunState::WaitingResources).await;
    run.control.cancel();
    assert!(calls.try_recv().is_err());
    finish(holding, Reply::Done(1));
    bounded(holder.result()).await.unwrap();
    let protected = bounded(calls.recv()).await.unwrap();
    assert!(!protected.control.is_cancelled());
    finish(protected, Reply::Done(12));
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert!(calls.try_recv().is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn host_shutdown_still_reaches_a_noncancellable_activity() {
    let (mut runtime, client, mut calls) = setup();
    let mut run = runtime.submit(Flow::new(&client)).await.unwrap();
    let call = bounded(calls.recv()).await.unwrap();
    bounded(runtime.shutdown()).await.unwrap();
    assert!(call.control.is_cancelled());
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
}

struct Abandon(Protected, Arc<Semaphore>);
#[async_trait]
impl Workflow for Abandon {
    type Output = u64;
    const NAME: &'static str = "abandon-protected";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<u64> {
        tokio::select! {
            result = ctx.run_activity(self.0) => result,
            _ = self.1.acquire() => Err(Failure::Cancelled),
        }
    }
}
#[tokio::test]
async fn dropping_the_waiter_does_not_cancel_the_native_call_or_release_its_resource() {
    let (mut runtime, client, mut calls) = setup();
    let abandon = Arc::new(Semaphore::new(0));
    let mut run = runtime
        .submit(Abandon(Protected::new(&client), abandon.clone()))
        .await
        .unwrap();
    let call = bounded(calls.recv()).await.unwrap();
    abandon.add_permits(1);
    state(&run.control, RunState::Settling).await;
    assert!(!call.control.is_cancelled());
    let mut other = runtime.submit(Ordinary(client.clone())).await.unwrap();
    state(&other.control, RunState::WaitingResources).await;
    assert!(calls.try_recv().is_err());
    finish(call, Reply::Done(12));
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    finish(bounded(calls.recv()).await.unwrap(), Reply::Done(1));
    bounded(other.result()).await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_protected_activity_can_still_decide_to_cancel_itself() {
    let (mut runtime, client, mut calls) = setup();
    let mut flow = Flow::new(&client);
    flow.step.deliberate_cancel = true;
    let mut run = runtime.submit(flow).await.unwrap();
    finish(bounded(calls.recv()).await.unwrap(), Reply::Done(12));
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert!(run.control.cancellation_requested());
    runtime.shutdown().await.unwrap();
}

#[derive(Default)]
struct NotifiedStore {
    inner: MemoryTaskStore,
    cancelled: Notify,
    reject_cancel: bool,
}
impl TaskStore for NotifiedStore {
    fn claim(&self, pool: u128, epoch: u64) -> Outcome<()> {
        self.inner.claim(pool, epoch)
    }
    fn list(&self, pool: u128) -> Outcome<Vec<SavedTask>> {
        self.inner.list(pool)
    }
    fn save(&self, task: SavedTask, previous: Option<u64>) -> Outcome<()> {
        let cancelled = task.cancel_requested;
        if cancelled && self.reject_cancel {
            return Err(Failure::Failed("cancel write failed".into()));
        }
        self.inner.save(task, previous)?;
        if cancelled {
            self.cancelled.notify_one();
        }
        Ok(())
    }
}

#[tokio::test]
async fn durable_cancel_intent_is_persisted_while_delivery_is_deferred() {
    let store = Arc::new(NotifiedStore::default());
    let mut driver = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(7, TaskIds::for_leader(1).unwrap())
        .unwrap();
    driver.enable_persistence(store.clone()).await.unwrap();
    driver.restore().await.unwrap();
    let (events, mut calls) = mpsc::unbounded_channel();
    let client = driver.register("worker", Worker(events)).unwrap();
    let mut flow = Flow::new(&client);
    flow.durable = true;
    flow.next = true;
    let mut run = driver.submit(flow).unwrap();
    let call = bounded(async {
        loop {
            tokio::select! {
                call = calls.recv() => break call.unwrap(),
                _ = driver.tick() => {},
            }
        }
    })
    .await;
    run.control.cancel();
    bounded(async {
        loop {
            tokio::select! {
                _ = store.cancelled.notified() => break,
                _ = driver.tick() => {},
            }
        }
    })
    .await;
    assert!(!call.control.is_cancelled());
    assert_eq!(store.list(7).unwrap()[0].outcome, StoredOutcome::Pending);
    finish(call, Reply::Done(12));
    let result = bounded(async {
        loop {
            tokio::select! {
                result = run.result() => break result,
                _ = driver.tick() => {},
            }
        }
    })
    .await;
    assert_eq!(result, Err(Failure::Cancelled));
    assert_eq!(store.list(7).unwrap()[0].outcome, StoredOutcome::Cancelled);
    assert!(calls.try_recv().is_err());
    driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn protection_does_not_hide_leader_revocation_or_a_control_persistence_failure() {
    for fail_write in [false, true] {
        let store = Arc::new(NotifiedStore {
            reject_cancel: fail_write,
            ..Default::default()
        });
        let ids = TaskIds::for_leader(1).unwrap();
        let mut driver = WorkflowDriver::new("pool", Config::default())
            .unwrap()
            .with_task_ids(7, ids.clone())
            .unwrap();
        driver.enable_persistence(store.clone()).await.unwrap();
        driver.restore().await.unwrap();
        let (events, mut calls) = mpsc::unbounded_channel();
        let client = driver.register("worker", Worker(events)).unwrap();
        let mut flow = Flow::new(&client);
        flow.durable = true;
        let mut run = driver.submit(flow).unwrap();
        let call = bounded(async {
            loop {
                tokio::select! {
                    call = calls.recv() => break call.unwrap(),
                    _ = driver.tick() => {},
                }
            }
        })
        .await;
        if fail_write {
            run.control.cancel();
        } else {
            ids.revoke();
        }
        let result = bounded(async {
            loop {
                tokio::select! {
                    result = run.result() => break result,
                    _ = driver.tick() => {},
                }
            }
        })
        .await;
        assert!(call.control.is_cancelled());
        if fail_write {
            assert!(matches!(result, Err(Failure::Uncertain(_))));
        } else {
            assert_eq!(result, Err(Failure::Cancelled));
            assert_eq!(store.list(7).unwrap()[0].outcome, StoredOutcome::Pending);
        }
        driver.shutdown().await.unwrap();
    }
}
