use async_trait::async_trait;
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, mpsc, oneshot};
use workflow_foundation::{persistence::*, *};

async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), f)
        .await
        .expect("operation stalled")
}
async fn state(control: &RunControl, predicate: impl Fn(&RunState) -> bool) {
    let mut control = control.clone();
    bounded(async {
        loop {
            if predicate(&control.state()) {
                break;
            }
            control.changed().await.unwrap();
        }
    })
    .await;
}
struct Gate(AtomicBool);
#[async_trait]
impl ContinuationGuard for Gate {
    async fn check(&self) -> Outcome<ContinueDecision> {
        Ok(if self.0.load(Ordering::SeqCst) {
            ContinueDecision::Ready
        } else {
            ContinueDecision::Wait("placement changed".into())
        })
    }
}
struct Command {
    name: &'static str,
    reply: oneshot::Sender<()>,
}
struct Job {
    commands: mpsc::UnboundedSender<Command>,
    finished: Notify,
}
#[async_trait]
impl ActivityControl for Job {
    async fn suspend(&self) -> Outcome<()> {
        self.command("suspend").await
    }
    async fn resume(&self) -> Outcome<()> {
        self.command("resume").await
    }
}
impl Job {
    async fn command(&self, name: &'static str) -> Outcome<()> {
        let (reply, rx) = oneshot::channel();
        self.commands.send(Command { name, reply }).unwrap();
        rx.await.map_err(|_| Failure::Unavailable)
    }
}
struct Migration {
    job: Arc<Job>,
    started: mpsc::UnboundedSender<()>,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl ServiceHandler for Migration {
    // Same command protocol and same Activity for both control contracts.
    type Request = bool; // true: settle with a partial result; false: retain invocation.
    type Reply = u64;
    type Query = ();
    type View = ();
    fn resources(_: &bool) -> Vec<ResourceAccess> {
        vec![]
    }
    async fn execute(&self, stop_returns: bool, context: ActivityContext) -> Outcome<u64> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !stop_returns {
            context.register_control(self.job.clone())?;
        }
        self.started.send(()).unwrap();
        let mut control = context.control();
        loop {
            if stop_returns && control.request().paused() {
                return Ok(7);
            }
            tokio::select! {
                _ = self.job.finished.notified() => return Ok(100),
                changed = control.changed() => { changed?; },
            }
        }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
struct Flow {
    client: ServiceClient<Migration>,
    gate: Arc<Gate>,
    stop_returns: bool,
    progress: mpsc::UnboundedSender<u64>,
}
#[async_trait]
impl Workflow for Flow {
    type Output = u64;
    const NAME: &'static str = "shrink";
    fn continuation_guard(&self) -> Option<Arc<dyn ContinuationGuard>> {
        Some(self.gate.clone())
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<u64> {
        let first = ctx.activity(&self.client, self.stop_returns).await?;
        self.progress.send(first).unwrap();
        ctx.report_details(&json!({"migrated": first, "source": "disk-a"}))?;
        if first == 7 {
            // The partial business reply is consumed BEFORE acknowledging a safe pause.
            ctx.safe_point().await?;
            ctx.activity(&self.client, false).await
        } else {
            Ok(first)
        }
    }
}
struct Fixture {
    runtime: WorkflowRuntime,
    client: ServiceClient<Migration>,
    gate: Arc<Gate>,
    job: Arc<Job>,
    commands: mpsc::UnboundedReceiver<Command>,
    started: mpsc::UnboundedReceiver<()>,
    calls: Arc<AtomicUsize>,
}
impl Fixture {
    fn new() -> Self {
        let mut runtime = WorkflowRuntime::start("pool", Config::default()).unwrap();
        let (commands, rx) = mpsc::unbounded_channel();
        let (started, sx) = mpsc::unbounded_channel();
        let job = Arc::new(Job {
            commands,
            finished: Notify::new(),
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let client = runtime
            .register(
                "migration",
                Migration {
                    job: job.clone(),
                    started,
                    calls: calls.clone(),
                },
            )
            .unwrap();
        Self {
            runtime,
            client,
            job,
            commands: rx,
            started: sx,
            calls,
            gate: Arc::new(Gate(AtomicBool::new(true))),
        }
    }
    async fn submit(&self, stop_returns: bool) -> (Run<u64>, mpsc::UnboundedReceiver<u64>) {
        let (progress, rx) = mpsc::unbounded_channel();
        let run = self
            .runtime
            .client()
            .submit(Flow {
                client: self.client.clone(),
                gate: self.gate.clone(),
                stop_returns,
                progress,
            })
            .await
            .unwrap();
        (run, rx)
    }
    async fn command(&mut self, name: &str) -> Command {
        let command = bounded(self.commands.recv()).await.unwrap();
        assert_eq!(command.name, name);
        command
    }
}
#[tokio::test]
async fn retained_activity_rechecks_workflow_conditions_and_never_returns_a_pause_as_success() {
    let mut f = Fixture::new();
    let (mut run, mut progress) = f.submit(false).await;
    bounded(f.started.recv()).await.unwrap();
    run.control.pause().unwrap();
    let suspend = f.command("suspend").await;
    assert!(!run.control.pause_acknowledged());
    suspend.reply.send(()).unwrap();
    bounded(run.control.wait_paused()).await.unwrap();
    assert!(progress.try_recv().is_err());
    f.gate.0.store(false, Ordering::SeqCst);
    run.control.resume().unwrap();
    state(&run.control, |s| {
        matches!(s, RunState::WaitingConditions(_))
    })
    .await;
    assert!(f.commands.try_recv().is_err());
    assert!(progress.try_recv().is_err());
    f.gate.0.store(true, Ordering::SeqCst);
    run.control.recheck();
    f.command("resume").await.reply.send(()).unwrap();
    f.job.finished.notify_one();
    assert_eq!(bounded(run.result()).await.unwrap(), 100);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        f.runtime.client().task(run.id()).unwrap().details["migrated"],
        100
    );
    f.runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn stopping_activity_returns_partial_result_before_the_same_task_pauses_and_reissues() {
    let mut f = Fixture::new();
    let (mut run, mut progress) = f.submit(true).await;
    let id = run.id();
    let mut observer = f.runtime.client().observe_task(id).unwrap();
    bounded(f.started.recv()).await.unwrap();
    run.control.pause().unwrap();
    assert_eq!(bounded(progress.recv()).await.unwrap(), 7);
    bounded(run.control.wait_paused()).await.unwrap();
    let snapshot = observer.snapshot();
    assert_eq!(snapshot.details["migrated"], 7);
    assert_eq!(
        snapshot.activities.entries[0].state,
        ActivityState::Completed
    );
    assert_eq!(snapshot.state, RunState::Paused);
    assert!(f.commands.try_recv().is_err());
    f.gate.0.store(false, Ordering::SeqCst);
    run.control.resume().unwrap();
    state(&run.control, |s| {
        matches!(s, RunState::WaitingConditions(_))
    })
    .await;
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.gate.0.store(true, Ordering::SeqCst);
    run.control.recheck();
    bounded(f.started.recv()).await.unwrap();
    f.job.finished.notify_one();
    assert_eq!(bounded(run.result()).await.unwrap(), 100);
    assert_eq!(run.id(), id);
    assert_eq!(observer.snapshot().activities.total, 2);
    let _ = observer.changed().await.unwrap();
    f.runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn new_pause_during_resume_requires_fresh_stop_and_preserves_user_reason() {
    let mut f = Fixture::new();
    let (mut run, _) = f.submit(false).await;
    bounded(f.started.recv()).await.unwrap();
    run.control.pause().unwrap();
    f.command("suspend").await.reply.send(()).unwrap();
    bounded(run.control.wait_paused()).await.unwrap();
    run.control.resume().unwrap();
    let resume = f.command("resume").await;
    run.control.set_policy_pauses(&["topo-1".into()].into());
    run.control.pause().unwrap();
    assert!(!run.control.pause_acknowledged());
    resume.reply.send(()).unwrap();
    let again = f.command("suspend").await;
    assert!(!run.control.pause_acknowledged());
    again.reply.send(()).unwrap();
    bounded(run.control.wait_paused()).await.unwrap();
    run.control.set_policy_pauses(&Default::default());
    assert!(run.control.pause_requested());
    run.control.cancel();
    assert!(matches!(
        bounded(run.result()).await,
        Err(Failure::Cancelled)
    ));
    f.runtime.shutdown().await.unwrap();
}

struct Checkpoint {
    progress: mpsc::UnboundedSender<serde_json::Value>,
    persistent: bool,
}
#[async_trait]
impl Workflow for Checkpoint {
    type Output = ();
    const NAME: &'static str = "checkpoint";
    fn persistence(&self) -> Outcome<Persistence> {
        if self.persistent {
            Persistence::durable(1, &json!({"disk": "a"}))
        } else {
            Ok(Persistence::Memory)
        }
    }
    fn restore(r: &SavedTask, deps: &Dependencies) -> Outcome<Self> {
        if r.version != 1 {
            return Err(Failure::Rejected("unsupported version".into()));
        }
        Ok(Self {
            progress: (*deps.get::<mpsc::UnboundedSender<serde_json::Value>>()?).clone(),
            persistent: true,
        })
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        self.progress
            .send(ctx.restored_checkpoint().clone())
            .unwrap();
        ctx.report_details(&json!({"disk": "a", "copied": 7}))?;
        if self.persistent {
            ctx.save_checkpoint(&json!({"cursor":7})).await?;
        }
        ctx.next_signal().await?;
        Ok(())
    }
}
async fn host(
    mut driver: WorkflowDriver,
) -> (
    tokio_util::sync::CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let stop = tokio_util::sync::CancellationToken::new();
    let stopping = stop.clone();
    let join = tokio::spawn(async move {
        loop {
            tokio::select! { _ = stopping.cancelled() => break, _ = driver.tick() => {} }
        }
        driver.shutdown().await.unwrap();
    });
    (stop, join)
}
#[tokio::test]
async fn same_task_restores_checkpoint_details_user_pause_and_new_owner_epoch() {
    let store = Arc::new(MemoryTaskStore::default());
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut driver = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(55, TaskIds::for_leader(10).unwrap())
        .unwrap();
    driver
        .define::<Checkpoint>(Dependencies::default().with(tx.clone()))
        .unwrap();
    driver.enable_persistence(store.clone()).await.unwrap();
    assert!(driver.restore().await.unwrap().is_empty());
    let mut run = driver
        .submit(Checkpoint {
            progress: tx.clone(),
            persistent: true,
        })
        .unwrap();
    let id = run.id();
    let (stop, join) = host(driver).await;
    bounded(run.accepted()).await.unwrap();
    bounded(rx.recv()).await.unwrap();
    run.control.pause().unwrap();
    bounded(run.control.wait_paused()).await.unwrap();
    stop.cancel();
    bounded(join).await.unwrap();
    let old = store.list(55).unwrap().remove(0);
    assert_eq!(old.outcome, StoredOutcome::Pending);
    assert!(old.user_paused);
    let mut next = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(55, TaskIds::for_leader(11).unwrap())
        .unwrap();
    next.define::<Checkpoint>(Dependencies::default().with(tx.clone()))
        .unwrap();
    next.enable_persistence(store.clone()).await.unwrap();
    let restored = next.restore().await.unwrap().remove(0);
    assert_eq!(restored.record.id, id);
    restored.start.start();
    let view = next.view();
    let (stop, join) = host(next).await;
    bounded(restored.control.wait_paused()).await.unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(view.task(id).unwrap().details["copied"], 7);
    assert_eq!(store.list(55).unwrap()[0].owner_epoch, 11);
    let mut stale = old.clone();
    stale.revision += 1;
    assert!(store.save(stale, Some(old.revision)).is_err());
    restored.control.resume().unwrap();
    assert_eq!(bounded(rx.recv()).await.unwrap()["cursor"], 7);
    restored
        .control
        .signal(Signal::new("finish", ""))
        .await
        .unwrap();
    state(&restored.control, |s| matches!(s, RunState::Completed)).await;
    assert_eq!(store.list(55).unwrap()[0].outcome, StoredOutcome::Completed);
    stop.cancel();
    bounded(join).await.unwrap();
}
#[tokio::test]
async fn ids_are_global_within_a_leader_epoch_and_persistence_is_workflow_defined() {
    let ids = TaskIds::for_leader(4).unwrap();
    let (tx, _) = mpsc::unbounded_channel();
    let mut a = WorkflowDriver::new("a", Config::default())
        .unwrap()
        .with_task_ids(1, ids.clone())
        .unwrap();
    let mut b = WorkflowDriver::new("b", Config::default())
        .unwrap()
        .with_task_ids(2, ids.clone())
        .unwrap();
    assert!(
        a.submit(Checkpoint {
            progress: tx.clone(),
            persistent: true
        })
        .is_err()
    );
    let first = a
        .submit(Checkpoint {
            progress: tx.clone(),
            persistent: false,
        })
        .unwrap()
        .id();
    let second = b
        .submit(Checkpoint {
            progress: tx.clone(),
            persistent: false,
        })
        .unwrap()
        .id();
    assert!(second > first);
    assert_eq!(second.pool, 2);
    assert_eq!(first.business(), "checkpoint");
    assert_eq!(first.to_string().parse::<TaskId>().unwrap(), first);
    ids.revoke();
    assert!(
        b.submit(Checkpoint {
            progress: tx.clone(),
            persistent: false
        })
        .is_err()
    );
    let mut next = WorkflowDriver::new("a", Config::default())
        .unwrap()
        .with_task_ids(1, TaskIds::for_leader(5).unwrap())
        .unwrap();
    let third = next
        .submit(Checkpoint {
            progress: tx,
            persistent: false,
        })
        .unwrap()
        .id();
    assert!(third > second);
    assert_eq!(third.sequence, 1);
}
#[tokio::test]
async fn file_journal_locks_reopens_fences_old_owner_and_recovers_incomplete_tail() {
    let path = std::env::temp_dir().join(format!("workflow-journal-{}", uuid::Uuid::new_v4()));
    let store = FileTaskStore::open(&path).unwrap();
    store.claim(7, 1).unwrap();
    let record = SavedTask {
        id: "1/00000000000000000000000000000007/checkpoint/1"
            .parse()
            .unwrap(),
        version: 1,
        input: json!({"disk":"a"}),
        checkpoint: json!({"cursor":7}),
        objects: vec![],
        metadata: json!(null),
        details: json!({"copied":7}),
        revision: 0,
        owner_epoch: 1,
        user_paused: true,
        cancel_requested: false,
        outcome: StoredOutcome::Pending,
    };
    store.save(record.clone(), None).unwrap();
    assert!(FileTaskStore::open(&path).is_err());
    drop(store);
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[1, 2])
        .unwrap();
    let store = FileTaskStore::open(&path).unwrap();
    assert_eq!(store.list(7).unwrap()[0].checkpoint["cursor"], 7);
    assert!(store.claim(7, 1).is_err());
    store.claim(7, 2).unwrap();
    let mut stale = record;
    stale.revision = 1;
    assert!(store.save(stale, Some(0)).is_err());
    drop(store);
    let mut bytes = std::fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    std::fs::write(&path, bytes).unwrap();
    assert!(FileTaskStore::open(&path).is_err());
    std::fs::remove_file(path).unwrap();
}

struct ExpansionControl(Arc<Job>);
#[async_trait]
impl ActivityControl for ExpansionControl {
    async fn suspend(&self) -> Outcome<()> {
        self.0.command("drain-batch").await
    }
    async fn resume(&self) -> Outcome<()> {
        self.0.command("replan").await
    }
}
struct Expansion {
    client: ServiceClient<Migration>,
    control: Arc<ExpansionControl>,
}
#[async_trait]
impl Workflow for Expansion {
    type Output = u64;
    const NAME: &'static str = "expansion";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<u64> {
        ctx.set_activity_control(self.control);
        ctx.activity(&self.client, false).await
    }
}
#[tokio::test]
async fn workflow_can_choose_different_domain_control_commands_for_the_same_service() {
    let mut f = Fixture::new();
    let (commands, mut rx) = mpsc::unbounded_channel();
    let control = Arc::new(ExpansionControl(Arc::new(Job {
        commands,
        finished: Notify::new(),
    })));
    let mut run = f
        .runtime
        .client()
        .submit(Expansion {
            client: f.client.clone(),
            control,
        })
        .await
        .unwrap();
    bounded(f.started.recv()).await.unwrap();
    run.control.pause().unwrap();
    let command = bounded(rx.recv()).await.unwrap();
    assert_eq!(command.name, "drain-batch");
    command.reply.send(()).unwrap();
    bounded(run.control.wait_paused()).await.unwrap();
    assert!(f.commands.try_recv().is_err());
    run.control.resume().unwrap();
    let command = bounded(rx.recv()).await.unwrap();
    assert_eq!(command.name, "replan");
    command.reply.send(()).unwrap();
    f.job.finished.notify_one();
    assert_eq!(bounded(run.result()).await.unwrap(), 100);
    f.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn unknown_durable_workflow_keeps_admission_closed_until_its_definition_is_available() {
    let store = Arc::new(MemoryTaskStore::default());
    store.claim(7, 1).unwrap();
    let id = "1/00000000000000000000000000000007/checkpoint/1"
        .parse()
        .unwrap();
    store
        .save(
            SavedTask {
                id,
                version: 1,
                input: json!({"disk":"a"}),
                checkpoint: json!(null),
                objects: vec![],
                metadata: json!(null),
                details: json!(null),
                revision: 0,
                owner_epoch: 1,
                user_paused: false,
                cancel_requested: false,
                outcome: StoredOutcome::Pending,
            },
            None,
        )
        .unwrap();
    let (tx, _rx) = mpsc::unbounded_channel();
    let mut driver = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(7, TaskIds::for_leader(2).unwrap())
        .unwrap();
    driver.enable_persistence(store.clone()).await.unwrap();
    assert!(driver.restore().await.is_err());
    assert!(
        driver
            .submit(Checkpoint {
                progress: tx.clone(),
                persistent: false
            })
            .is_err()
    );
    assert_eq!(store.list(7).unwrap()[0].revision, 0);
    driver
        .define::<Checkpoint>(Dependencies::default().with(tx))
        .unwrap();
    let restored = driver.restore().await.unwrap();
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].record.id, id);
}

struct SharedObject(Checkpoint);
#[async_trait]
impl Workflow for SharedObject {
    type Output = ();
    const NAME: &'static str = "shared-object";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("disk", "a")]
    }
    fn persistence(&self) -> Outcome<Persistence> {
        self.0.persistence()
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        self.0.run(ctx).await
    }
}
#[tokio::test]
async fn durable_cancel_while_waiting_for_objects_does_not_wait_for_the_owner() {
    let store = Arc::new(MemoryTaskStore::default());
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut driver = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(7, TaskIds::for_leader(1).unwrap())
        .unwrap();
    driver.enable_persistence(store.clone()).await.unwrap();
    driver.restore().await.unwrap();
    let mut owner = driver
        .submit(SharedObject(Checkpoint {
            progress: tx.clone(),
            persistent: false,
        }))
        .unwrap();
    let mut waiter = driver
        .submit(SharedObject(Checkpoint {
            progress: tx,
            persistent: true,
        }))
        .unwrap();
    let (stop, join) = host(driver).await;
    bounded(owner.accepted()).await.unwrap();
    bounded(rx.recv()).await.unwrap();
    bounded(waiter.accepted()).await.unwrap();
    waiter.control.cancel();
    assert_eq!(bounded(waiter.result()).await, Err(Failure::Cancelled));
    assert_eq!(store.list(7).unwrap()[0].outcome, StoredOutcome::Cancelled);
    assert!(rx.try_recv().is_err());
    assert!(!owner.control.cancellation_requested());
    owner.control.cancel();
    bounded(owner.result()).await.unwrap_err();
    stop.cancel();
    bounded(join).await.unwrap();
}

struct CancelProbe {
    store: Arc<MemoryTaskStore>,
    observed: mpsc::UnboundedSender<(Option<CancellationReason>, bool)>,
    started: mpsc::UnboundedSender<()>,
}
#[async_trait]
impl Workflow for CancelProbe {
    type Output = ();
    const NAME: &'static str = "cancel-probe";
    fn persistence(&self) -> Outcome<Persistence> {
        Persistence::durable(1, &())
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let control = ctx.control();
        self.started.send(()).unwrap();
        control.cancelled().await;
        let record = self.store.list(7)?.remove(0);
        self.observed
            .send((control.cancellation_reason(), record.cancel_requested))
            .unwrap();
        Err(Failure::Cancelled)
    }
}

#[tokio::test]
async fn user_cancel_is_durable_before_delivery_but_leader_revocation_is_recoverable() {
    for user_cancel in [true, false] {
        let ids = TaskIds::for_leader(3).unwrap();
        let store = Arc::new(MemoryTaskStore::default());
        let mut driver = WorkflowDriver::new("pool", Config::default())
            .unwrap()
            .with_task_ids(7, ids.clone())
            .unwrap();
        driver.enable_persistence(store.clone()).await.unwrap();
        driver.restore().await.unwrap();
        let (observed, mut rx) = mpsc::unbounded_channel();
        let (started, mut started_rx) = mpsc::unbounded_channel();
        let mut run = driver
            .submit(CancelProbe {
                store: store.clone(),
                observed,
                started,
            })
            .unwrap();
        let (stop, join) = host(driver).await;
        bounded(run.accepted()).await.unwrap();
        bounded(started_rx.recv()).await.unwrap();
        if user_cancel {
            run.control.cancel();
        } else {
            ids.revoke();
        }
        let expected_reason = if user_cancel {
            CancellationReason::Requested
        } else {
            CancellationReason::Interrupted
        };
        assert_eq!(
            bounded(rx.recv()).await.unwrap(),
            (Some(expected_reason), user_cancel)
        );
        assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
        assert_eq!(
            store.list(7).unwrap()[0].outcome,
            if user_cancel {
                StoredOutcome::Cancelled
            } else {
                StoredOutcome::Pending
            }
        );
        stop.cancel();
        bounded(join).await.unwrap();
    }
}

struct FailingStore;
impl TaskStore for FailingStore {
    fn claim(&self, _: u128, _: u64) -> Outcome<()> {
        Ok(())
    }
    fn list(&self, _: u128) -> Outcome<Vec<SavedTask>> {
        Ok(vec![])
    }
    fn save(&self, _: SavedTask, _: Option<u64>) -> Outcome<()> {
        Err(Failure::Failed("disk full".into()))
    }
}
#[tokio::test]
async fn durable_acceptance_failure_never_runs_business_but_memory_workflows_still_work() {
    let mut driver = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(7, TaskIds::for_leader(3).unwrap())
        .unwrap();
    driver
        .enable_persistence(Arc::new(FailingStore))
        .await
        .unwrap();
    driver.restore().await.unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut durable = driver
        .submit(Checkpoint {
            progress: tx.clone(),
            persistent: true,
        })
        .unwrap();
    let mut memory = driver
        .submit(Checkpoint {
            progress: tx,
            persistent: false,
        })
        .unwrap();
    let (stop, join) = host(driver).await;
    assert!(bounded(durable.accepted()).await.is_err());
    assert!(matches!(
        bounded(durable.result()).await,
        Err(Failure::Failed(_))
    ));
    bounded(memory.accepted()).await.unwrap();
    bounded(rx.recv()).await.unwrap();
    assert!(rx.try_recv().is_err());
    memory.control.cancel();
    assert_eq!(bounded(memory.result()).await, Err(Failure::Cancelled));
    stop.cancel();
    bounded(join).await.unwrap();
}
