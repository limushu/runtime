use async_trait::async_trait;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Semaphore, mpsc};
use workflow_foundation::*;

pub const LIMIT: Duration = Duration::from_secs(3);
pub fn key(id: &str) -> ObjectKey {
    ObjectKey::new("test-object", id)
}
pub fn write(id: &str) -> ResourceAccess {
    ResourceAccess::write(key(id))
}
pub fn read(id: &str) -> ResourceAccess {
    ResourceAccess::read(key(id))
}
pub async fn next(rx: &mut mpsc::UnboundedReceiver<&'static str>) -> &'static str {
    tokio::time::timeout(LIMIT, rx.recv())
        .await
        .expect("event timeout")
        .expect("event channel closed")
}
pub async fn finish<T>(run: &mut Run<T>) -> Outcome<T> {
    tokio::time::timeout(LIMIT, run.result())
        .await
        .expect("workflow timeout")
}
pub async fn state(control: &RunControl, expected: RunState) {
    let mut control = control.clone();
    tokio::time::timeout(LIMIT, async {
        loop {
            if control.state() == expected {
                break;
            }
            control.changed().await.unwrap();
        }
    })
    .await
    .expect("state timeout");
}

#[derive(Clone, Copy)]
pub enum Behavior {
    Success,
    Failed,
    Uncertain,
    Panic,
    CancelThenSettle,
}
pub struct Work {
    pub name: &'static str,
    pub access: Vec<ResourceAccess>,
    pub gate: Option<Arc<Semaphore>>,
    pub behavior: Behavior,
}
impl Work {
    pub fn write(name: &'static str, object: &str) -> Self {
        Self {
            name,
            access: vec![write(object)],
            gate: None,
            behavior: Behavior::Success,
        }
    }
    pub fn gated(mut self, gate: &Arc<Semaphore>) -> Self {
        self.gate = Some(gate.clone());
        self
    }
    pub fn behavior(mut self, behavior: Behavior) -> Self {
        self.behavior = behavior;
        self
    }
}
pub struct Probe {
    pub entered: mpsc::UnboundedSender<&'static str>,
    effects: AtomicUsize,
    calls: Mutex<Vec<ActivityId>>,
}
impl Probe {
    pub fn new(entered: mpsc::UnboundedSender<&'static str>) -> Self {
        Self {
            entered,
            effects: AtomicUsize::new(0),
            calls: Mutex::new(vec![]),
        }
    }
}
#[async_trait]
impl DomainService for Probe {
    type Request = Work;
    type Reply = usize;
    type Query = ();
    type View = (usize, Vec<ActivityId>);
    fn resources(work: &Work) -> Vec<ResourceAccess> {
        work.access.clone()
    }
    async fn execute(&self, work: Work, ctx: ActivityContext) -> Outcome<usize> {
        self.calls.lock().unwrap().push(ctx.id);
        let _ = self.entered.send(work.name);
        if matches!(work.behavior, Behavior::CancelThenSettle) {
            ctx.cancellation().cancelled().await;
            let _ = self.entered.send("cancel-observed");
        }
        if let Some(gate) = work.gate {
            gate.acquire().await.unwrap().forget();
        }
        match work.behavior {
            Behavior::Success => Ok(self.effects.fetch_add(1, Ordering::SeqCst) + 1),
            Behavior::Failed => Err(Failure::Failed("known failure".into())),
            Behavior::Uncertain => Err(Failure::Uncertain("unknown external result".into())),
            Behavior::Panic => panic!("test provider panic"),
            Behavior::CancelThenSettle => Err(Failure::Cancelled),
        }
    }
    fn query(&self, _: ()) -> Outcome<Self::View> {
        Ok((
            self.effects.load(Ordering::SeqCst),
            self.calls.lock().unwrap().clone(),
        ))
    }
}
pub fn setup() -> (
    Coordinator,
    ServiceClient<Probe>,
    mpsc::UnboundedReceiver<&'static str>,
) {
    let mut pool = Coordinator::start("test-pool", Config::default()).unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    let service = pool.register("probe", Probe::new(tx)).unwrap();
    (pool, service, rx)
}
pub struct Call {
    pub service: ServiceClient<Probe>,
    pub objects: Vec<ObjectKey>,
    pub work: Work,
}
impl Call {
    pub fn new(service: &ServiceClient<Probe>, work: Work) -> Self {
        Self {
            service: service.clone(),
            objects: vec![],
            work,
        }
    }
    pub fn owning(mut self, objects: &[&str]) -> Self {
        self.objects = objects.iter().map(|k| key(k)).collect();
        self
    }
}
#[async_trait]
impl Workflow for Call {
    type Output = usize;
    const NAME: &'static str = "test.call";
    fn objects(&self) -> Vec<ObjectKey> {
        self.objects.clone()
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<usize> {
        ctx.activity(&self.service, self.work).await
    }
}
