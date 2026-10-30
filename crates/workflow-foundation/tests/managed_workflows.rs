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
impl RunCondition for Gate {
    async fn check(&self) -> Outcome<Readiness> {
        Ok(if self.0.load(Ordering::SeqCst) {
            Readiness::Ready
        } else {
            Readiness::Blocked("placement changed".into())
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
    // The request expresses domain-specific stopping behavior; no framework hook.
    type Request = &'static str;
    type Reply = u64;
    type Query = ();
    type View = ();
    fn resources(_: &&'static str) -> Vec<ResourceAccess> {
        vec![]
    }
    async fn execute(&self, mode: &'static str, context: ActivityContext) -> Outcome<u64> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.send(()).unwrap();
        let mut control = context.control();
        loop {
            if control.is_cancelled() {
                self.job.command("cancel").await?;
                return Err(Failure::Cancelled);
            }
            if mode != "finish-current" && control.request().paused() {
                // The test controls real stop completion separately from request delivery.
                let stopping = self.job.command(mode);
                tokio::pin!(stopping);
                return tokio::select! {
                    r = &mut stopping => { r?; Ok(7) },
                    _ = control.cancelled() => {
                        self.job.command("cancel").await?;
                        Err(Failure::Cancelled)
                    },
                    _ = self.job.finished.notified() => Ok(100),
                };
            }
            tokio::select! {
                _ = self.job.finished.notified() => return Ok(100),
                _ = control.changed() => {},
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
    fn run_condition(&self) -> Option<Arc<dyn RunCondition>> {
        Some(self.gate.clone())
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<u64> {
        let first = ctx
            .activity(
                &self.client,
                if self.stop_returns {
                    "suspend"
                } else {
                    "finish-current"
                },
            )
            .await?;
        self.progress.send(first).unwrap();
        ctx.report_details(&json!({"migrated": first, "source": "disk-a"}))?;
        if first == 7 {
            // The partial business reply is consumed BEFORE acknowledging a safe pause.
            ctx.wait_until_runnable().await?;
            ctx.activity(&self.client, "suspend").await
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
async fn non_interruptible_activity_finishes_before_task_can_pause() {
    let mut f = Fixture::new();
    let (mut run, mut progress) = f.submit(false).await;
    bounded(f.started.recv()).await.unwrap();
    run.control.suspend().unwrap();
    state(&run.control, |s| matches!(s, RunState::Suspending)).await;
    assert!(!run.control.suspend_acknowledged());
    assert!(progress.try_recv().is_err());
    assert!(f.commands.try_recv().is_err());
    f.job.finished.notify_one();
    assert_eq!(bounded(run.result()).await.unwrap(), 100);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn stopping_activity_returns_partial_result_before_the_same_task_pauses_and_reissues() {
    let mut f = Fixture::new();
    let (mut run, mut progress) = f.submit(true).await;
    let id = run.id();
    let mut observer = f.runtime.client().observe_task(id).unwrap();
    bounded(f.started.recv()).await.unwrap();
    run.control.suspend().unwrap();
    let stop = f.command("suspend").await;
    assert!(!run.control.suspend_acknowledged());
    assert!(progress.try_recv().is_err());
    stop.reply.send(()).unwrap();
    assert_eq!(bounded(progress.recv()).await.unwrap(), 7);
    bounded(run.control.wait_suspended()).await.unwrap();
    let snapshot = observer.snapshot();
    assert_eq!(snapshot.details["migrated"], 7);
    assert_eq!(
        snapshot.activities.entries[0].state,
        ActivityState::Completed
    );
    assert_eq!(snapshot.state, RunState::Suspended);
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
async fn cancellation_during_stopping_reaches_service_and_waits_for_its_result() {
    let mut f = Fixture::new();
    let (mut run, _) = f.submit(true).await;
    bounded(f.started.recv()).await.unwrap();
    run.control.suspend().unwrap();
    let _stop = f.command("suspend").await;
    run.control.cancel();
    let cancel = f.command("cancel").await;
    assert!(!matches!(
        run.control.state(),
        RunState::Completed | RunState::Failed(_)
    ));
    cancel.reply.send(()).unwrap();
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn completed_reply_racing_pause_is_not_reissued() {
    let mut f = Fixture::new();
    let (mut run, _progress) = f.submit(true).await;
    bounded(f.started.recv()).await.unwrap();
    run.control.suspend().unwrap();
    let _stop = f.command("suspend").await;
    f.job.finished.notify_one();
    assert_eq!(bounded(run.result()).await.unwrap(), 100);
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    f.runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn changed_pause_reasons_need_fresh_ack_and_cancel_while_parked_never_reissues() {
    let mut f = Fixture::new();
    let (mut run, mut progress) = f.submit(true).await;
    bounded(f.started.recv()).await.unwrap();
    run.control.suspend().unwrap();
    let stop = f.command("suspend").await;
    run.control.resume().unwrap();
    run.control
        .set_policy_suspensions(&["topology".into()].into());
    run.control.suspend().unwrap();
    assert!(!run.control.suspend_acknowledged());
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    stop.reply.send(()).unwrap();
    assert_eq!(bounded(progress.recv()).await.unwrap(), 7);
    bounded(run.control.wait_suspended()).await.unwrap();
    run.control.set_policy_suspensions(&Default::default());
    bounded(run.control.wait_suspended()).await.unwrap();
    assert!(run.control.suspend_reasons().contains("user"));
    run.control.cancel();
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert_eq!(f.calls.load(Ordering::SeqCst), 1);
    assert!(f.commands.try_recv().is_err());
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
    run.control.suspend().unwrap();
    bounded(run.control.wait_suspended()).await.unwrap();
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
    bounded(restored.control.wait_suspended()).await.unwrap();
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

struct Expansion {
    client: ServiceClient<Migration>,
}
#[async_trait]
impl Workflow for Expansion {
    type Output = u64;
    const NAME: &'static str = "expansion";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<u64> {
        let first = ctx.activity(&self.client, "drain-batch").await?;
        if first == 7 {
            ctx.wait_until_runnable().await?;
            ctx.activity(&self.client, "drain-batch").await
        } else {
            Ok(first)
        }
    }
}
#[tokio::test]
async fn requests_choose_domain_stopping_behavior_without_control_hooks() {
    let mut f = Fixture::new();
    let mut run = f
        .runtime
        .client()
        .submit(Expansion {
            client: f.client.clone(),
        })
        .await
        .unwrap();
    bounded(f.started.recv()).await.unwrap();
    run.control.suspend().unwrap();
    f.command("drain-batch").await.reply.send(()).unwrap();
    bounded(run.control.wait_suspended()).await.unwrap();
    run.control.resume().unwrap();
    bounded(f.started.recv()).await.unwrap();
    f.job.finished.notify_one();
    assert_eq!(bounded(run.result()).await.unwrap(), 100);
    assert_eq!(f.calls.load(Ordering::SeqCst), 2);
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
    fn restore(record: &SavedTask, dependencies: &Dependencies) -> Outcome<Self> {
        Checkpoint::restore(record, dependencies).map(Self)
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        self.0.run(ctx).await
    }
}
fn saved_checkpoint(sequence: u64) -> SavedTask {
    SavedTask {
        id: format!("1/00000000000000000000000000000007/checkpoint/{sequence}")
            .parse()
            .unwrap(),
        version: 1,
        input: json!({"disk":"a"}),
        checkpoint: json!({"cursor":7}),
        objects: vec![],
        metadata: json!({"source":"recovery-test"}),
        details: json!({"copied":7}),
        revision: 0,
        owner_epoch: 1,
        user_paused: false,
        cancel_requested: false,
        outcome: StoredOutcome::Pending,
    }
}

#[tokio::test]
async fn restored_task_is_observable_before_first_poll_without_acknowledging_pause() {
    let store = Arc::new(MemoryTaskStore::default());
    store.claim(7, 1).unwrap();
    let mut record = saved_checkpoint(1);
    record.id = "1/00000000000000000000000000000007/shared-object/1"
        .parse()
        .unwrap();
    record.objects = vec![ObjectKey::new("disk", "a")];
    record.user_paused = true;
    store.save(record.clone(), None).unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel::<serde_json::Value>();
    let mut driver = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(7, TaskIds::for_leader(2).unwrap())
        .unwrap();
    driver
        .define::<SharedObject>(Dependencies::default().with(tx))
        .unwrap();
    driver.enable_persistence(store).await.unwrap();
    let restored = driver.restore().await.unwrap().remove(0);

    let snapshot = driver.view().task(record.id).unwrap();
    assert_eq!(snapshot.id, record.id);
    assert_eq!(snapshot.details, record.details);
    assert_eq!(snapshot.objects, record.objects);
    assert!(snapshot.suspend_requested);
    assert!(!snapshot.suspend_acknowledged);
    assert_eq!(restored.record.metadata, record.metadata);
    assert_eq!(restored.record.checkpoint, record.checkpoint);
    assert_eq!(restored.record.owner_epoch, 2);
    assert!(rx.try_recv().is_err());
    // Registration and even polling cannot bypass the owner's start gate.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), async {
            loop {
                driver.tick().await;
            }
        })
        .await
        .is_err()
    );
    assert!(rx.try_recv().is_err());
    assert!(!restored.control.suspend_acknowledged());
}

#[tokio::test]
async fn recovery_validates_the_whole_batch_before_claiming_any_record() {
    let store = Arc::new(MemoryTaskStore::default());
    store.claim(7, 1).unwrap();
    store.save(saved_checkpoint(1), None).unwrap();
    let mut invalid = saved_checkpoint(2);
    invalid.input = json!({"disk":"changed"});
    store.save(invalid, None).unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel::<serde_json::Value>();
    let mut driver = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(7, TaskIds::for_leader(2).unwrap())
        .unwrap();
    driver
        .define::<Checkpoint>(Dependencies::default().with(tx))
        .unwrap();
    driver.enable_persistence(store.clone()).await.unwrap();
    assert!(matches!(driver.restore().await, Err(Failure::Rejected(_))));
    assert!(driver.is_idle());
    for record in store.list(7).unwrap() {
        assert_eq!((record.owner_epoch, record.revision), (1, 0));
        assert!(driver.view().task(record.id).is_err());
    }
    assert!(rx.try_recv().is_err());
}

#[derive(Default)]
struct FailingClaimStore {
    inner: MemoryTaskStore,
    fail: AtomicBool,
}
impl TaskStore for FailingClaimStore {
    fn claim(&self, pool: u128, epoch: u64) -> Outcome<()> {
        self.inner.claim(pool, epoch)
    }
    fn list(&self, pool: u128) -> Outcome<Vec<SavedTask>> {
        self.inner.list(pool)
    }
    fn save(&self, task: SavedTask, previous: Option<u64>) -> Outcome<()> {
        if task.id == saved_checkpoint(2).id
            && task.owner_epoch == 2
            && self.fail.load(Ordering::SeqCst)
        {
            return Err(Failure::Unavailable);
        }
        self.inner.save(task, previous)
    }
}

#[tokio::test]
async fn partial_claim_failure_registers_nothing_and_retry_keeps_task_ids() {
    let store = Arc::new(FailingClaimStore::default());
    store.claim(7, 1).unwrap();
    store.save(saved_checkpoint(1), None).unwrap();
    store.save(saved_checkpoint(2), None).unwrap();
    store.fail.store(true, Ordering::SeqCst);
    let (tx, mut rx) = mpsc::unbounded_channel::<serde_json::Value>();
    let mut driver = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(7, TaskIds::for_leader(2).unwrap())
        .unwrap();
    driver
        .define::<Checkpoint>(Dependencies::default().with(tx.clone()))
        .unwrap();
    driver.enable_persistence(store.clone()).await.unwrap();
    assert!(matches!(driver.restore().await, Err(Failure::Unavailable)));
    assert!(driver.is_idle());
    let records = store.list(7).unwrap();
    assert_eq!((records[0].owner_epoch, records[0].revision), (2, 1));
    assert_eq!((records[1].owner_epoch, records[1].revision), (1, 0));
    for record in &records {
        assert!(driver.view().task(record.id).is_err());
    }
    assert!(
        driver
            .submit(Checkpoint {
                progress: tx,
                persistent: false
            })
            .is_err()
    );

    store.fail.store(false, Ordering::SeqCst);
    let restored = driver.restore().await.unwrap();
    assert_eq!(
        restored
            .iter()
            .map(|task| task.record.id)
            .collect::<Vec<_>>(),
        records.iter().map(|task| task.id).collect::<Vec<_>>()
    );
    assert_eq!(restored[0].record.revision, 2);
    assert_eq!(restored[1].record.revision, 1);
    assert!(rx.try_recv().is_err());
}

// The second workflow rejects a repeated declaration. Previously recovery called
// persistence() again while registering, leaving the first Task partially installed.
struct OnceDeclared {
    index: usize,
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl Workflow for OnceDeclared {
    type Output = ();
    const NAME: &'static str = "once-declared";
    fn persistence(&self) -> Outcome<Persistence> {
        let previous = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.index == 1 && previous != 0 {
            return Err(Failure::Rejected(
                "declaration repeated during registration".into(),
            ));
        }
        Persistence::durable(1, &json!({"index": self.index}))
    }
    fn restore(record: &SavedTask, deps: &Dependencies) -> Outcome<Self> {
        let index = record.input["index"].as_u64().unwrap() as usize;
        let calls = deps.get::<[Arc<AtomicUsize>; 2]>()?[index].clone();
        Ok(Self { index, calls })
    }
    async fn run(self, _: &mut WorkflowContext) -> Outcome<()> {
        Ok(())
    }
}

#[tokio::test]
async fn recovery_registers_the_batch_without_repeating_workflow_declarations() {
    let store = Arc::new(MemoryTaskStore::default());
    store.claim(7, 1).unwrap();
    for index in 0..2 {
        let mut record = saved_checkpoint(index + 1);
        record.id = format!(
            "1/00000000000000000000000000000007/once-declared/{}",
            index + 1
        )
        .parse()
        .unwrap();
        record.input = json!({"index": index});
        store.save(record, None).unwrap();
    }
    let calls = [Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0))];
    let mut driver = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(7, TaskIds::for_leader(2).unwrap())
        .unwrap();
    // Both old import paths remain valid and refer to the same public types.
    let dependencies: workflow_foundation::persistence::Dependencies =
        Dependencies::default().with(calls.clone());
    driver.define::<OnceDeclared>(dependencies).unwrap();
    driver.enable_persistence(store.clone()).await.unwrap();
    let restored: Vec<workflow_foundation::persistence::Restored> = driver.restore().await.unwrap();
    assert_eq!(restored.len(), 2);
    assert_eq!(driver.view().tasks().len(), 2);
    for (index, task) in restored.into_iter().enumerate() {
        assert_eq!(calls[index].load(Ordering::SeqCst), 1);
        assert_eq!(task.record.owner_epoch, 2);
        task.start.start();
    }
    bounded(async {
        while !driver.is_idle() {
            driver.tick().await;
        }
    })
    .await;
    for task in store.list(7).unwrap() {
        assert_eq!(task.outcome, StoredOutcome::Completed);
    }
    assert!(calls.iter().all(|count| count.load(Ordering::SeqCst) == 1));
}

struct RevokingClaimStore {
    inner: MemoryTaskStore,
    ids: TaskIds,
}
impl TaskStore for RevokingClaimStore {
    fn claim(&self, pool: u128, epoch: u64) -> Outcome<()> {
        self.inner.claim(pool, epoch)
    }
    fn list(&self, pool: u128) -> Outcome<Vec<SavedTask>> {
        self.inner.list(pool)
    }
    fn save(&self, task: SavedTask, previous: Option<u64>) -> Outcome<()> {
        let revoke = task.owner_epoch == 2 && task.id == saved_checkpoint(2).id;
        self.inner.save(task, previous)?;
        if revoke {
            self.ids.revoke();
        }
        Ok(())
    }
}

#[tokio::test]
async fn leader_revoked_during_recovery_claims_registers_no_tasks() {
    let ids = TaskIds::for_leader(2).unwrap();
    let store = Arc::new(RevokingClaimStore {
        inner: MemoryTaskStore::default(),
        ids: ids.clone(),
    });
    store.claim(7, 1).unwrap();
    store.save(saved_checkpoint(1), None).unwrap();
    store.save(saved_checkpoint(2), None).unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel::<serde_json::Value>();
    let mut driver = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(7, ids)
        .unwrap();
    driver
        .define::<Checkpoint>(Dependencies::default().with(tx.clone()))
        .unwrap();
    driver.enable_persistence(store.clone()).await.unwrap();
    assert!(matches!(driver.restore().await, Err(Failure::Unavailable)));
    assert!(driver.is_idle());
    assert!(driver.view().tasks().is_empty());
    assert!(rx.try_recv().is_err());
    assert!(
        driver
            .submit(Checkpoint {
                progress: tx.clone(),
                persistent: false
            })
            .is_err()
    );
    // The next leader can recover the claimed records under the original TaskIds.
    let mut next = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(7, TaskIds::for_leader(3).unwrap())
        .unwrap();
    next.define::<Checkpoint>(Dependencies::default().with(tx))
        .unwrap();
    next.enable_persistence(store).await.unwrap();
    let restored = next.restore().await.unwrap();
    assert_eq!(restored.len(), 2);
    assert_eq!(restored[0].record.id, saved_checkpoint(1).id);
    assert_eq!(restored[1].record.id, saved_checkpoint(2).id);
    assert!(restored.iter().all(|task| task.record.owner_epoch == 3));
    assert!(rx.try_recv().is_err());
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
