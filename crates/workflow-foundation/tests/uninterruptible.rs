use async_trait::async_trait;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, mpsc, oneshot};
use workflow_foundation::*;

async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), f)
        .await
        .expect("stalled protected activity")
}
type Event = (&'static str, ControlToken, oneshot::Sender<()>);
struct Worker(mpsc::UnboundedSender<Event>);
#[async_trait]
impl ServiceHandler for Worker {
    type Request = &'static str;
    type Reply = ();
    type Query = ();
    type View = ();
    fn resources(_: &&'static str) -> Vec<ResourceAccess> {
        vec![ResourceAccess::write(ObjectKey::new("link", "one"))]
    }
    async fn execute(&self, name: &'static str, ctx: ActivityContext) -> Outcome<()> {
        let (done, reply) = oneshot::channel();
        self.0.send((name, ctx.control(), done)).unwrap();
        tokio::select! {
            _ = ctx.cancellation().cancelled() => Err(Failure::Cancelled),
            r = reply => r.map_err(|_| Failure::Unavailable),
        }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
struct Protected {
    client: ServiceClient<Worker>,
    before: Arc<Semaphore>,
    returned: Arc<AtomicBool>,
}
#[async_trait]
impl Workflow for Protected {
    type Output = ();
    const NAME: &'static str = "protected";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        self.before.acquire().await.unwrap().forget();
        ctx.activity(&self.client, "disconnect")
            .uninterruptible()
            .await?;
        self.returned.store(true, Ordering::SeqCst);
        ctx.activity(&self.client, "rebuild").await
    }
}
struct Ordinary(ServiceClient<Worker>);
#[async_trait]
impl Workflow for Ordinary {
    type Output = ();
    const NAME: &'static str = "ordinary";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.activity(&self.0, "holder").await
    }
}
fn setup() -> (
    WorkflowRuntime,
    ServiceClient<Worker>,
    mpsc::UnboundedReceiver<Event>,
) {
    let mut runtime = WorkflowRuntime::start("test", Config::default()).unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    let client = runtime.register("worker", Worker(tx)).unwrap();
    (runtime, client, rx)
}
async fn entered(run: &Run<()>) {
    bounded(async {
        while !run.control.has_started() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
#[tokio::test]
async fn cancellation_before_dispatch_still_runs_required_call_and_returns_its_result() {
    let (mut runtime, client, mut events) = setup();
    let before = Arc::new(Semaphore::new(0));
    let returned = Arc::new(AtomicBool::new(false));
    let mut run = runtime
        .submit(Protected {
            client,
            before: before.clone(),
            returned: returned.clone(),
        })
        .await
        .unwrap();
    entered(&run).await;
    run.control.cancel();
    before.add_permits(1);
    let (name, control, finish) = bounded(events.recv()).await.unwrap();
    assert_eq!(name, "disconnect");
    assert!(!control.is_cancelled());
    assert!(!returned.load(Ordering::SeqCst));
    finish.send(()).unwrap();
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert!(returned.load(Ordering::SeqCst));
    assert!(events.try_recv().is_err(), "rebuild must not dispatch");
    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn pause_is_deferred_through_reply_then_acknowledged_and_resumed() {
    let (mut runtime, client, mut events) = setup();
    let before = Arc::new(Semaphore::new(0));
    let returned = Arc::new(AtomicBool::new(false));
    let mut run = runtime
        .submit(Protected {
            client,
            before: before.clone(),
            returned: returned.clone(),
        })
        .await
        .unwrap();
    entered(&run).await;
    run.control.suspend().unwrap();
    before.add_permits(1);
    let (_, control, finish) = bounded(events.recv()).await.unwrap();
    assert!(!control.request().paused());
    assert!(!run.control.suspend_acknowledged());
    finish.send(()).unwrap();
    bounded(run.control.wait_suspended()).await.unwrap();
    assert!(returned.load(Ordering::SeqCst));
    assert!(events.try_recv().is_err());
    run.control.resume().unwrap();
    let (name, _, finish) = bounded(events.recv()).await.unwrap();
    assert_eq!(name, "rebuild");
    finish.send(()).unwrap();
    bounded(run.result()).await.unwrap();
    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn cancellation_during_resource_wait_does_not_skip_required_dispatch() {
    let (mut runtime, client, mut events) = setup();
    let mut holder = runtime.submit(Ordinary(client.clone())).await.unwrap();
    let (_, _, release) = bounded(events.recv()).await.unwrap();
    let returned = Arc::new(AtomicBool::new(false));
    let mut run = runtime
        .submit(Protected {
            client,
            before: Arc::new(Semaphore::new(1)),
            returned: returned.clone(),
        })
        .await
        .unwrap();
    bounded(async {
        let mut control = run.control.clone();
        while control.state() != RunState::WaitingResources {
            control.changed().await.unwrap();
        }
    })
    .await;
    run.control.cancel();
    assert!(events.try_recv().is_err());
    release.send(()).unwrap();
    bounded(holder.result()).await.unwrap();
    let (name, token, finish) = bounded(events.recv()).await.unwrap();
    assert_eq!(name, "disconnect");
    assert!(!token.is_cancelled());
    finish.send(()).unwrap();
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert!(returned.load(Ordering::SeqCst));
    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn protected_call_does_not_mask_host_shutdown() {
    let (mut runtime, client, mut events) = setup();
    let mut run = runtime
        .submit(Protected {
            client,
            before: Arc::new(Semaphore::new(1)),
            returned: Arc::new(AtomicBool::new(false)),
        })
        .await
        .unwrap();
    let (_, token, _finish) = bounded(events.recv()).await.unwrap();
    bounded(runtime.shutdown()).await.unwrap();
    assert!(token.is_cancelled());
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
}
