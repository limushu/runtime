//! Command/reply domain integration: one whole evacuation Activity, never BG batches.
use async_trait::async_trait;
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use storage_pool::{
    task_center::{TaskCenter, persistence::*},
    *,
};
use tokio::sync::{mpsc, oneshot};
use workflow_foundation::*;

const LIMIT: Duration = Duration::from_secs(3);
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, future)
        .await
        .expect("timed out")
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Command {
    Start,
    Pause,
    Resume,
    Cancel,
    Finalize,
}
struct Call {
    command: Command,
    job: String,
    reply: oneshot::Sender<Outcome<()>>,
}
#[derive(Clone)]
struct Balance {
    job: String,
    tx: mpsc::UnboundedSender<Call>,
    controls: Arc<tokio::sync::Mutex<()>>,
}
impl Balance {
    async fn control(&self, command: Command) -> Outcome<()> {
        let _guard = self.controls.lock().await;
        self.send(command).await
    }
    async fn send(&self, command: Command) -> Outcome<()> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Call {
                command,
                job: self.job.clone(),
                reply,
            })
            .map_err(|_| Failure::Unavailable)?;
        response
            .await
            .map_err(|_| Failure::Uncertain("domain reply lost".into()))?
    }
}
#[async_trait]
impl ActivityPause for Balance {
    async fn pause(&self) -> Outcome<()> {
        self.control(Command::Pause).await
    }
    async fn resume(&self) -> Outcome<()> {
        self.control(Command::Resume).await
    }
}
struct Steps(mpsc::UnboundedSender<Call>);
struct Request {
    job: String,
    controlled: bool,
    finalize: bool,
}
#[async_trait]
impl ServiceHandler for Steps {
    type Request = Request;
    type Reply = ();
    type Query = ();
    type View = ();
    fn resources(request: &Request) -> Vec<ResourceAccess> {
        // Waiting on the external job holds only the job identity, not topo's disk locks.
        vec![ResourceAccess::write(ObjectKey::new(
            if request.finalize {
                "disk"
            } else {
                "balance-job"
            },
            &request.job,
        ))]
    }
    fn activity_name(request: &Request) -> &'static str {
        if request.finalize {
            "shrink.finalize"
        } else {
            "shrink.evacuate-all"
        }
    }
    async fn execute(&self, request: Request, context: ActivityContext) -> Outcome<()> {
        let balance = Balance {
            job: request.job,
            tx: self.0.clone(),
            controls: Arc::default(),
        };
        if request.controlled && !request.finalize {
            context.register_pause(Arc::new(balance.clone()))?;
            context.report_progress("evacuating", 37, Some(100));
        }
        if request.finalize {
            // An irreversible short commit can finish successfully despite cancellation.
            return balance.send(Command::Finalize).await;
        }
        let operation = balance.send(Command::Start);
        tokio::pin!(operation);
        tokio::select! {
            biased;
            result = &mut operation => result,
            _ = context.control().cancelled() => {
                // Sole cancellation path. Serialize with pause/resume commands and
                // wait for the original job reply before releasing its resources.
                let cancel = balance.control(Command::Cancel).await;
                let settled = operation.await;
                match cancel {
                    Ok(()) => settled,
                    Err(error) => Err(Failure::Uncertain(format!("cancel outcome unknown: {error}"))),
                }
            }
        }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
struct Conditions {
    allow: AtomicBool,
    checks: AtomicUsize,
}
impl Conditions {
    fn new(allow: bool) -> Arc<Self> {
        Arc::new(Self {
            allow: AtomicBool::new(allow),
            checks: AtomicUsize::new(0),
        })
    }
    fn set(&self, allow: bool) {
        self.allow.store(allow, Ordering::SeqCst);
    }
}
#[async_trait]
impl ContinuationGuard for Conditions {
    async fn check(&self) -> Outcome<ContinueDecision> {
        self.checks.fetch_add(1, Ordering::SeqCst);
        Ok(if self.allow.load(Ordering::SeqCst) {
            ContinueDecision::Ready
        } else {
            ContinueDecision::Wait("insufficient healthy capacity".into())
        })
    }
}
struct Shrink {
    client: ServiceClient<Steps>,
    job: String,
    controlled: bool,
    conditions: Arc<dyn ContinuationGuard>,
}
#[async_trait]
impl Workflow for Shrink {
    type Output = ();
    const NAME: &'static str = "test.shrink";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("disk", &self.job)]
    }
    fn continuation_guard(&self) -> Option<Arc<dyn ContinuationGuard>> {
        Some(self.conditions.clone())
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.report_progress("evacuating", 0, Some(1));
        ctx.activity(
            &self.client,
            Request {
                job: self.job.clone(),
                controlled: self.controlled,
                finalize: false,
            },
        )
        .await?;
        ctx.report_progress("verifying", 1, Some(1));
        ctx.activity(
            &self.client,
            Request {
                job: self.job,
                controlled: false,
                finalize: true,
            },
        )
        .await
    }
}
struct Topo {
    key: String,
    started: mpsc::UnboundedSender<WorkflowId>,
    uncertain: bool,
}
#[async_trait]
impl Workflow for Topo {
    type Output = ();
    const NAME: &'static str = "test.topology";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("disk", &self.key)]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        self.started.send(ctx.id()).unwrap();
        ctx.next_signal().await?;
        if self.uncertain {
            Err(Failure::Uncertain("topo commit unknown".into()))
        } else {
            Ok(())
        }
    }
}
struct Fixture {
    center: TaskCenter,
    client: ServiceClient<Steps>,
    calls: mpsc::UnboundedReceiver<Call>,
    started: mpsc::UnboundedSender<WorkflowId>,
    topo: mpsc::UnboundedReceiver<WorkflowId>,
    store: Arc<MemoryTaskStore>,
    pool: PoolUuid,
}
impl Fixture {
    async fn new() -> Self {
        Self::open(PoolUuid::new(), Arc::default()).await
    }
    async fn open(pool: PoolUuid, store: Arc<MemoryTaskStore>) -> Self {
        let config = Config::default();
        let mut driver = WorkflowDriver::new("pool", config.clone()).unwrap();
        let (tx, calls) = mpsc::unbounded_channel();
        let client = driver.register("balance", Steps(tx)).unwrap();
        let center = TaskCenter::with_task_store(
            driver,
            &config,
            pool,
            PoolUuid::new(),
            store.clone(),
            PersistenceConfig::default(),
        )
        .unwrap();
        center.recover_tasks().await.unwrap();
        let (started, topo) = mpsc::unbounded_channel();
        Self {
            center,
            client,
            calls,
            started,
            topo,
            store,
            pool,
        }
    }
    fn work(&self, job: &str, controlled: bool, conditions: Arc<dyn ContinuationGuard>) -> Shrink {
        Shrink {
            client: self.client.clone(),
            job: job.into(),
            controlled,
            conditions,
        }
    }
    async fn topology(&self, key: &str, uncertain: bool) -> Run<()> {
        bounded(self.center.submit_topology(Topo {
            key: key.into(),
            started: self.started.clone(),
            uncertain,
        }))
        .await
        .unwrap()
    }
    async fn call(&mut self, command: Command) -> Call {
        let call = bounded(self.calls.recv()).await.unwrap();
        assert_eq!(call.command, command);
        call
    }
    async fn entered(&mut self, id: WorkflowId) {
        assert_eq!(bounded(self.topo.recv()).await, Some(id));
    }
    async fn finish_topo(&mut self, run: &mut Run<()>) -> Outcome<()> {
        run.control.signal(Signal::new("finish", "")).await.unwrap();
        bounded(run.result()).await
    }
    async fn finish_shrink(&mut self, run: &mut Run<()>, start: Call) {
        start.reply.send(Ok(())).unwrap();
        self.call(Command::Finalize)
            .await
            .reply
            .send(Ok(()))
            .unwrap();
        bounded(run.result()).await.unwrap();
    }
    async fn cancel_shrink(&mut self, run: &mut Run<()>, start: Call) {
        run.control.cancel();
        self.call(Command::Cancel).await.reply.send(Ok(())).unwrap();
        start.reply.send(Err(Failure::Cancelled)).unwrap();
        assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    }
    async fn close(self) {
        bounded(self.center.shutdown()).await.unwrap();
    }
}
async fn state(run: &Run<()>, expected: impl Fn(&RunState) -> bool) {
    let mut control = run.control.clone();
    bounded(async {
        while !expected(&control.state()) {
            control.changed().await.unwrap();
        }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn topology_waits_for_real_pause_then_all_blockers_and_conditions_before_same_job_resumes() {
    let mut f = Fixture::new().await;
    let conditions = Conditions::new(true);
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, conditions.clone()))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    let original = shrink.control.activities().entries[0].id;
    assert_eq!(shrink.control.progress().completed, 37);
    let mut t1 = f.topology("other-disk", false).await;
    let pause = f.call(Command::Pause).await;
    state(&shrink, |s| *s == RunState::Pausing).await;
    assert!(f.topo.try_recv().is_err());
    let mut t2 = f.topology("third-disk", false).await;
    pause.reply.send(Ok(())).unwrap();
    bounded(shrink.control.wait_paused()).await.unwrap();
    let mut entered = vec![
        bounded(f.topo.recv()).await.unwrap(),
        bounded(f.topo.recv()).await.unwrap(),
    ];
    entered.sort();
    assert_eq!(entered, vec![t1.id(), t2.id()]);
    assert_eq!(
        f.center.task(shrink.id()).unwrap().topology_blockers,
        vec![t1.id(), t2.id()]
    );
    assert_eq!(
        shrink.control.activities().entries[0].state,
        ActivityState::Paused
    );
    f.center.flush_history().await.unwrap();
    let persistent_id = f.center.persistent_task_id(shrink.id()).unwrap();
    let paused = f.center.task_detail(persistent_id).await.unwrap();
    assert!(paused.system_pause_requested);
    assert!(!paused.pause_requested);
    assert_eq!(
        paused.executions[0].activities[&1].state,
        RecordedActivityState::Paused
    );
    f.finish_topo(&mut t1).await.unwrap();
    assert!(f.calls.try_recv().is_err());
    assert!(shrink.control.system_pause_requested());
    conditions.set(false);
    f.finish_topo(&mut t2).await.unwrap();
    state(&shrink, |s| matches!(s, RunState::WaitingConditions(_))).await;
    assert!(f.calls.try_recv().is_err());
    f.center.flush_history().await.unwrap();
    let id = f.center.persistent_task_id(shrink.id()).unwrap();
    assert_eq!(
        f.center.task_detail(id).await.unwrap().status,
        TaskStatus::WaitingConditions
    );
    conditions.set(true);
    f.center.recheck(shrink.id()).unwrap();
    let resume = f.call(Command::Resume).await;
    assert_eq!(resume.job, "j1");
    resume.reply.send(Ok(())).unwrap();
    f.finish_shrink(&mut shrink, start).await;
    assert_eq!(shrink.control.activities().entries[0].id, original);
    f.center.flush_history().await.unwrap();
    let saved = f.center.task_detail(id).await.unwrap();
    assert_eq!(saved.class, TaskClass::Shrink);
    assert_eq!(saved.executions.len(), 1);
    assert_eq!(saved.executions[0].activities.len(), 2);
    assert!(
        saved.executions[0]
            .activities
            .values()
            .all(|a| a.state == RecordedActivityState::Completed)
    );
    f.close().await;
}

#[tokio::test]
async fn no_hook_waits_for_completion_and_does_not_dispatch_the_next_step() {
    let mut f = Fixture::new().await;
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", false, Conditions::new(true)))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    let mut topo = f.topology("other", false).await;
    state(&shrink, |s| *s == RunState::Pausing).await;
    assert!(f.calls.try_recv().is_err());
    assert!(f.topo.try_recv().is_err());
    start.reply.send(Ok(())).unwrap();
    f.entered(topo.id()).await;
    assert_eq!(shrink.control.state(), RunState::Paused);
    assert!(f.calls.try_recv().is_err());
    f.finish_topo(&mut topo).await.unwrap();
    f.call(Command::Finalize).await.reply.send(Ok(())).unwrap();
    bounded(shrink.result()).await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn topology_release_never_clears_a_user_pause() {
    let mut f = Fixture::new().await;
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, Conditions::new(true)))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    shrink.control.pause().unwrap();
    f.call(Command::Pause).await.reply.send(Ok(())).unwrap();
    bounded(shrink.control.wait_paused()).await.unwrap();
    let mut topo = f.topology("other", false).await;
    f.entered(topo.id()).await;
    f.finish_topo(&mut topo).await.unwrap();
    // Wait for the actor to consume terminal topo and publish a new scheduling snapshot.
    bounded(async {
        while shrink.control.system_pause_requested() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(shrink.control.user_pause_requested());
    assert_eq!(shrink.control.state(), RunState::Paused);
    assert!(f.calls.try_recv().is_err());
    shrink.control.resume().unwrap();
    f.call(Command::Resume).await.reply.send(Ok(())).unwrap();
    f.finish_shrink(&mut shrink, start).await;
    f.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn new_topology_during_resume_waits_for_a_new_pause_ack() {
    let mut f = Fixture::new().await;
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, Conditions::new(true)))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    let mut t1 = f.topology("other", false).await;
    f.call(Command::Pause).await.reply.send(Ok(())).unwrap();
    f.entered(t1.id()).await;
    f.finish_topo(&mut t1).await.unwrap();
    let resume = f.call(Command::Resume).await;
    let mut t2 = f.topology("another", false).await;
    assert!(f.topo.try_recv().is_err());
    resume.reply.send(Ok(())).unwrap();
    let pause = f.call(Command::Pause).await;
    assert!(f.topo.try_recv().is_err());
    pause.reply.send(Ok(())).unwrap();
    f.entered(t2.id()).await;
    f.finish_topo(&mut t2).await.unwrap();
    f.call(Command::Resume).await.reply.send(Ok(())).unwrap();
    f.finish_shrink(&mut shrink, start).await;
    f.close().await;
}

#[tokio::test]
async fn first_start_waits_for_topology_and_conditions_and_second_shrink_keeps_its_order() {
    let mut f = Fixture::new().await;
    let mut topo = f.topology("other", false).await;
    f.entered(topo.id()).await;
    let conditions = Conditions::new(false);
    let mut s1 = f
        .center
        .submit_shrink(f.work("j1", true, conditions.clone()))
        .await
        .unwrap();
    let mut s2 = f
        .center
        .submit_shrink(f.work("j2", true, Conditions::new(true)))
        .await
        .unwrap();
    assert!(f.calls.try_recv().is_err());
    f.finish_topo(&mut topo).await.unwrap();
    state(&s1, |s| matches!(s, RunState::WaitingConditions(_))).await;
    assert_eq!(s2.control.state(), RunState::WaitingObjects);
    conditions.set(true);
    f.center.recheck(s1.id()).unwrap();
    let first = f.call(Command::Start).await;
    assert_eq!(first.job, "j1");
    let mut t2 = f.topology("third", false).await;
    f.call(Command::Pause).await.reply.send(Ok(())).unwrap();
    f.entered(t2.id()).await;
    assert_eq!(s2.control.state(), RunState::WaitingObjects);
    f.finish_topo(&mut t2).await.unwrap();
    f.call(Command::Resume).await.reply.send(Ok(())).unwrap();
    f.finish_shrink(&mut s1, first).await;
    let second = f.call(Command::Start).await;
    assert_eq!(second.job, "j2");
    f.finish_shrink(&mut s2, second).await;
    f.close().await;
}

#[tokio::test]
async fn uncertain_topology_keeps_shrink_suspended_but_cancel_still_works() {
    let mut f = Fixture::new().await;
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, Conditions::new(true)))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    let mut topo = f.topology("other", true).await;
    f.call(Command::Pause).await.reply.send(Ok(())).unwrap();
    f.entered(topo.id()).await;
    assert!(matches!(
        f.finish_topo(&mut topo).await,
        Err(Failure::Uncertain(_))
    ));
    assert!(f.calls.try_recv().is_err());
    assert_eq!(shrink.control.state(), RunState::Paused);
    f.cancel_shrink(&mut shrink, start).await;
    f.close().await;
}

#[tokio::test]
async fn failed_pause_is_not_acknowledged_and_fences_topology() {
    let mut f = Fixture::new().await;
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, Conditions::new(true)))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    let mut topo = f.topology("other", false).await;
    f.call(Command::Pause)
        .await
        .reply
        .send(Err(Failure::Uncertain("pause outcome unknown".into())))
        .unwrap();
    assert!(f.topo.try_recv().is_err());
    f.call(Command::Cancel).await.reply.send(Ok(())).unwrap();
    start.reply.send(Err(Failure::Cancelled)).unwrap();
    assert!(matches!(
        bounded(shrink.result()).await,
        Err(Failure::Uncertain(_))
    ));
    assert!(matches!(
        bounded(topo.result()).await,
        Err(Failure::Rejected(_))
    ));
    assert!(f.topo.try_recv().is_err());
    f.close().await;
}

#[tokio::test]
async fn completion_racing_pause_does_not_advance_past_the_gate() {
    let mut f = Fixture::new().await;
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, Conditions::new(true)))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    let mut topo = f.topology("other", false).await;
    let pause = f.call(Command::Pause).await;
    start.reply.send(Ok(())).unwrap();
    assert!(f.topo.try_recv().is_err());
    pause.reply.send(Ok(())).unwrap();
    f.entered(topo.id()).await;
    assert!(f.calls.try_recv().is_err());
    f.finish_topo(&mut topo).await.unwrap();
    f.call(Command::Finalize).await.reply.send(Ok(())).unwrap();
    bounded(shrink.result()).await.unwrap();
    f.close().await;
}

#[tokio::test]
async fn recoverable_shrink_restores_pool_wide_slot_and_rechecks_before_starting() {
    let f = Fixture::new().await;
    let conditions = Conditions::new(false);
    let run = f
        .center
        .submit_resumable_shrink(
            f.work("j1", true, conditions),
            ResumeSpec {
                kind: "shrink".into(),
                version: 1,
                input: vec![],
                checkpoint: vec![],
            },
        )
        .await
        .unwrap();
    let id = run.task_id;
    state(&run.run, |s| matches!(s, RunState::WaitingConditions(_))).await;
    bounded(f.center.shutdown()).await.unwrap();
    let pool = f.pool;
    let store = f.store.clone();
    drop(f);
    let mut next = Fixture::open(pool, store).await;
    assert_eq!(
        next.center.task_detail(id).await.unwrap().class,
        TaskClass::Shrink
    );
    // Even disjoint targets cannot take the outstanding shrink's slot after takeover.
    assert!(
        next.center
            .submit_shrink(next.work("j2", true, Conditions::new(true)))
            .await
            .is_err()
    );
    let client = next.client.clone();
    next.center
        .register_recovery("shrink", 1, move |_| {
            let client = client.clone();
            async move {
                Ok(Shrink {
                    client,
                    job: "j1".into(),
                    controlled: true,
                    conditions: Conditions::new(true),
                })
            }
        })
        .unwrap();
    let workflow = next.center.continue_task(id).await.unwrap();
    let start = next.call(Command::Start).await;
    start.reply.send(Ok(())).unwrap();
    next.call(Command::Finalize)
        .await
        .reply
        .send(Ok(()))
        .unwrap();
    let mut observer = next.center.observe_task(workflow).unwrap();
    bounded(async {
        while observer.snapshot().state != RunState::Completed {
            observer.changed().await.unwrap();
        }
    })
    .await;
    next.center.flush_history().await.unwrap();
    assert_eq!(
        next.center.task_detail(id).await.unwrap().executions.len(),
        2
    );
    next.close().await;
}

#[tokio::test]
async fn coalesced_resume_and_pause_reacknowledges_the_still_paused_job() {
    let mut f = Fixture::new().await;
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, Conditions::new(true)))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    shrink.control.pause().unwrap();
    f.call(Command::Pause).await.reply.send(Ok(())).unwrap();
    bounded(shrink.control.wait_paused()).await.unwrap();
    // Both writes happen before the executor is polled: watch coalesces them.
    shrink.control.resume().unwrap();
    shrink.control.pause().unwrap();
    let mut topo = f.topology("other", false).await;
    f.entered(topo.id()).await;
    assert!(f.calls.try_recv().is_err());
    f.finish_topo(&mut topo).await.unwrap();
    shrink.control.resume().unwrap();
    f.call(Command::Resume).await.reply.send(Ok(())).unwrap();
    f.finish_shrink(&mut shrink, start).await;
    f.close().await;
}

#[tokio::test]
async fn cancelling_a_paused_job_revokes_ack_until_cancel_and_work_have_settled() {
    let mut f = Fixture::new().await;
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, Conditions::new(true)))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    shrink.control.pause().unwrap();
    f.call(Command::Pause).await.reply.send(Ok(())).unwrap();
    bounded(shrink.control.wait_paused()).await.unwrap();
    shrink.control.cancel();
    let cancel = f.call(Command::Cancel).await;
    let mut topo = f.topology("other", false).await;
    assert!(f.topo.try_recv().is_err());
    cancel.reply.send(Ok(())).unwrap();
    assert!(f.topo.try_recv().is_err());
    start.reply.send(Err(Failure::Cancelled)).unwrap();
    assert_eq!(bounded(shrink.result()).await, Err(Failure::Cancelled));
    f.entered(topo.id()).await;
    f.finish_topo(&mut topo).await.unwrap();
    f.close().await;
}

struct GatedCheck {
    gated: AtomicBool,
    checks: mpsc::UnboundedSender<oneshot::Sender<Outcome<ContinueDecision>>>,
}
#[async_trait]
impl ContinuationGuard for GatedCheck {
    async fn check(&self) -> Outcome<ContinueDecision> {
        if self.gated.swap(false, Ordering::SeqCst) {
            let (reply, response) = oneshot::channel();
            self.checks.send(reply).unwrap();
            response.await.map_err(|_| Failure::Unavailable)?
        } else {
            Ok(ContinueDecision::Ready)
        }
    }
}

#[tokio::test]
async fn new_topology_invalidates_a_continuation_check_in_flight() {
    let mut f = Fixture::new().await;
    let (checks, mut rx) = mpsc::unbounded_channel();
    let guard = Arc::new(GatedCheck {
        gated: AtomicBool::new(false),
        checks,
    });
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, guard.clone()))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    let mut t1 = f.topology("other", false).await;
    f.call(Command::Pause).await.reply.send(Ok(())).unwrap();
    f.entered(t1.id()).await;
    guard.gated.store(true, Ordering::SeqCst);
    f.finish_topo(&mut t1).await.unwrap();
    let old_check = bounded(rx.recv()).await.unwrap();
    let mut t2 = f.topology("another", false).await;
    f.entered(t2.id()).await;
    assert!(old_check.send(Ok(ContinueDecision::Ready)).is_err());
    assert!(f.calls.try_recv().is_err());
    f.finish_topo(&mut t2).await.unwrap();
    f.call(Command::Resume).await.reply.send(Ok(())).unwrap();
    f.finish_shrink(&mut shrink, start).await;
    f.close().await;
}

#[tokio::test]
async fn uncertain_cancel_does_not_release_the_shrink_slot_as_a_known_cancellation() {
    let mut f = Fixture::new().await;
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, Conditions::new(true)))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    shrink.control.cancel();
    f.call(Command::Cancel)
        .await
        .reply
        .send(Err(Failure::Uncertain("cancel reply ambiguous".into())))
        .unwrap();
    start.reply.send(Err(Failure::Cancelled)).unwrap();
    assert!(matches!(
        bounded(shrink.result()).await,
        Err(Failure::Uncertain(_))
    ));
    let mut next = f
        .center
        .submit_shrink(f.work("j2", true, Conditions::new(true)))
        .await
        .unwrap();
    assert!(matches!(
        bounded(next.result()).await,
        Err(Failure::Rejected(_))
    ));
    assert!(f.calls.try_recv().is_err());
    f.close().await;
}

#[tokio::test]
async fn service_shutdown_cancels_a_controlled_job_even_while_waiting_for_conditions() {
    let mut f = Fixture::new().await;
    let conditions = Conditions::new(true);
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, conditions.clone()))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    shrink.control.pause().unwrap();
    f.call(Command::Pause).await.reply.send(Ok(())).unwrap();
    bounded(shrink.control.wait_paused()).await.unwrap();
    conditions.set(false);
    shrink.control.resume().unwrap();
    state(&shrink, |s| matches!(s, RunState::WaitingConditions(_))).await;
    let client = f.client.clone();
    let shutdown = tokio::spawn(async move { client.shutdown().await });
    f.call(Command::Cancel).await.reply.send(Ok(())).unwrap();
    start.reply.send(Err(Failure::Cancelled)).unwrap();
    assert_eq!(bounded(shrink.result()).await, Err(Failure::Cancelled));
    bounded(shutdown).await.unwrap().unwrap();
    f.close().await;
}

struct IgnoreControlFailure(Shrink);
#[async_trait]
impl Workflow for IgnoreControlFailure {
    type Output = ();
    const NAME: &'static str = "test.ignore-control-failure";
    fn objects(&self) -> Vec<ObjectKey> {
        self.0.objects()
    }
    fn continuation_guard(&self) -> Option<Arc<dyn ContinuationGuard>> {
        self.0.continuation_guard()
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let _ignored = ctx
            .activity(
                &self.0.client,
                Request {
                    job: self.0.job.clone(),
                    controlled: true,
                    finalize: false,
                },
            )
            .await;
        ctx.activity(
            &self.0.client,
            Request {
                job: self.0.job,
                controlled: false,
                finalize: true,
            },
        )
        .await
    }
}

#[tokio::test]
async fn workflow_cannot_hide_control_uncertainty_and_dispatch_another_effect() {
    let mut f = Fixture::new().await;
    let mut shrink = f
        .center
        .submit_shrink(IgnoreControlFailure(f.work(
            "j1",
            true,
            Conditions::new(true),
        )))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    shrink.control.pause().unwrap();
    f.call(Command::Pause)
        .await
        .reply
        .send(Err(Failure::Uncertain("pause outcome unknown".into())))
        .unwrap();
    f.call(Command::Cancel).await.reply.send(Ok(())).unwrap();
    start.reply.send(Err(Failure::Cancelled)).unwrap();
    assert!(matches!(
        bounded(shrink.result()).await,
        Err(Failure::Uncertain(_))
    ));
    assert!(f.calls.try_recv().is_err());
    f.close().await;
}

#[tokio::test]
async fn service_shutdown_invalidates_its_activity_pause_ack_before_topology_can_start() {
    let mut f = Fixture::new().await;
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, Conditions::new(true)))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    shrink.control.pause().unwrap();
    f.call(Command::Pause).await.reply.send(Ok(())).unwrap();
    bounded(shrink.control.wait_paused()).await.unwrap();
    let client = f.client.clone();
    let shutdown = tokio::spawn(async move { client.shutdown().await });
    let cancel = f.call(Command::Cancel).await;
    assert!(!shrink.control.pause_acknowledged());
    let mut topo = f.topology("other", false).await;
    assert!(f.topo.try_recv().is_err());
    cancel.reply.send(Ok(())).unwrap();
    assert!(f.topo.try_recv().is_err());
    start.reply.send(Err(Failure::Cancelled)).unwrap();
    assert_eq!(bounded(shrink.result()).await, Err(Failure::Cancelled));
    f.entered(topo.id()).await;
    f.finish_topo(&mut topo).await.unwrap();
    bounded(shutdown).await.unwrap().unwrap();
    f.close().await;
}

#[tokio::test]
async fn known_pause_rejection_waits_for_cleanup_then_releases_without_an_uncertain_fence() {
    let mut f = Fixture::new().await;
    let mut shrink = f
        .center
        .submit_shrink(f.work("j1", true, Conditions::new(true)))
        .await
        .unwrap();
    let start = f.call(Command::Start).await;
    let mut topo = f.topology("other", false).await;
    let error = Failure::Rejected("job cannot pause at this stage".into());
    f.call(Command::Pause)
        .await
        .reply
        .send(Err(error.clone()))
        .unwrap();
    let cancel = f.call(Command::Cancel).await;
    assert!(f.topo.try_recv().is_err());
    cancel.reply.send(Ok(())).unwrap();
    assert!(f.topo.try_recv().is_err());
    start.reply.send(Err(Failure::Cancelled)).unwrap();
    assert_eq!(bounded(shrink.result()).await, Err(error));
    f.entered(topo.id()).await;
    assert_eq!(f.center.scheduling().fenced, 0);
    f.finish_topo(&mut topo).await.unwrap();
    f.close().await;
}
