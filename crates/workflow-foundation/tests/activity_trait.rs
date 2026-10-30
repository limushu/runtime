#[allow(dead_code)]
mod support;
use async_trait::async_trait;
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use support::*;
use tokio::sync::{Semaphore, mpsc};
use workflow_foundation::{persistence::*, *};

struct Gate(AtomicBool);
#[async_trait]
impl RunCondition for Gate {
    async fn check(&self) -> Outcome<Readiness> {
        Ok(if self.0.load(Ordering::SeqCst) {
            Readiness::Ready
        } else {
            Readiness::Blocked("topology changed".into())
        })
    }
}
struct Flow<A> {
    step: A,
    gate: Option<Arc<Gate>>,
    durable: bool,
}
impl<A> Flow<A> {
    fn new(step: A) -> Self {
        Self {
            step,
            gate: None,
            durable: false,
        }
    }
}
#[async_trait]
impl<A: Activity + 'static> Workflow for Flow<A>
where
    A::Output: 'static,
{
    type Output = A::Output;
    const NAME: &'static str = "activity.trait";
    fn run_condition(&self) -> Option<Arc<dyn RunCondition>> {
        self.gate.clone().map(|g| g as Arc<dyn RunCondition>)
    }
    fn persistence(&self) -> Outcome<Persistence> {
        if self.durable {
            Persistence::durable(1, &())
        } else {
            Ok(Persistence::Memory)
        }
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Self::Output> {
        ctx.run_activity(self.step).await
    }
}

struct Step<F> {
    policy: RetryPolicy,
    client: ServiceClient<Probe>,
    work: VecDeque<Work>,
    handler: F,
}
#[async_trait]
impl<F: FnMut(Outcome<usize>) -> Outcome<ActivityAction<usize>> + Send> Activity for Step<F> {
    type Reply = usize;
    type Output = usize;
    fn name(&self) -> &'static str {
        "check.replication"
    }
    fn retry_policy(&self) -> RetryPolicy {
        self.policy
    }
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<usize> {
        scope
            .call(
                &self.client,
                self.work.pop_front().expect("unexpected retry"),
            )
            .await
    }
    fn handle(&mut self, result: Outcome<usize>) -> Outcome<ActivityAction<usize>> {
        (self.handler)(result)
    }
}

#[tokio::test]
async fn framework_retry_budget_survives_pause_and_links_child_calls() {
    let (mut runtime, client, mut entered) = setup();
    let step = Step {
        policy: RetryPolicy::fixed(1, Duration::from_millis(30)),
        client,
        work: (0..2).map(|_| Work::write("check", "disk")).collect(),
        handler: move |result: Outcome<usize>| {
            assert!(result? < 3); // Successful reply, but business condition is not met.
            Ok(ActivityAction::Retry {
                reason: "replication incomplete".into(),
            })
        },
    };
    let mut run = runtime.submit(Flow::new(step)).await.unwrap();
    assert_eq!(next(&mut entered).await, "check");
    state(&run.control, RunState::WaitingTimer).await;
    run.control.suspend().unwrap();
    state(&run.control, RunState::Suspended).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(entered.try_recv().is_err());
    run.control.resume().unwrap();
    assert_eq!(
        finish(&mut run).await,
        Err(Failure::Failed(
            "Activity retry limit exhausted (1 retries): replication incomplete".into()
        ))
    );
    let history = run.control.activities();
    assert_eq!(history.total, 3);
    assert_eq!(history.entries[0].name, "check.replication");
    assert_eq!(history.entries[0].retries, 1);
    assert_eq!(history.entries[0].attempts, 2);
    assert!(
        history
            .entries
            .iter()
            .skip(1)
            .all(|entry| entry.parent_id == Some(history.entries[0].id))
    );
    runtime.shutdown().await.unwrap();
}

struct Migration {
    events: mpsc::UnboundedSender<&'static str>,
    stopped: Arc<Semaphore>,
}
#[async_trait]
impl ServiceHandler for Migration {
    type Request = u64;
    type Reply = ActivityExit<u64, u64>;
    type Query = ();
    type View = ();
    fn resources(_: &u64) -> Vec<ResourceAccess> {
        vec![write("disk")]
    }
    async fn execute(&self, cursor: u64, ctx: ActivityContext) -> Outcome<Self::Reply> {
        self.events.send("migrating").unwrap();
        if cursor == 7 {
            return Ok(ActivityExit::Completed(100));
        }
        let mut control = ctx.control();
        while !control.request().paused() && !control.is_cancelled() {
            let _ = control.changed().await;
        }
        self.events.send("stop-requested").unwrap();
        self.stopped.acquire().await.unwrap().forget(); // Native stop acknowledgement.
        if control.is_cancelled() {
            Err(Failure::Cancelled)
        } else {
            Ok(ActivityExit::Suspended(7))
        }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
struct Migrate {
    client: ServiceClient<Migration>,
    cursor: u64,
}
#[async_trait]
impl Activity for Migrate {
    type Reply = ActivityExit<u64, u64>;
    type Output = u64;
    fn name(&self) -> &'static str {
        "shrink.migrate-disks"
    }
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<Self::Reply> {
        scope.call(&self.client, self.cursor).await
    }
    fn handle(&mut self, result: Outcome<Self::Reply>) -> Outcome<ActivityAction<u64>> {
        Ok(match result? {
            ActivityExit::Completed(value) => ActivityAction::Continue(value),
            ActivityExit::Suspended(cursor) => {
                self.cursor = cursor;
                ActivityAction::Suspend(ActivityWait::Resume)
            }
        })
    }
}

#[tokio::test]
async fn pause_waits_for_native_stop_releases_resources_and_rechecks_admission() {
    let (mut runtime, probe, mut probe_events) = setup();
    let (events, mut rx) = mpsc::unbounded_channel();
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
    let gate = Arc::new(Gate(AtomicBool::new(true)));
    let mut flow = Flow::new(Migrate { client, cursor: 0 });
    flow.gate = Some(gate.clone());
    let mut run = runtime.submit(flow).await.unwrap();
    assert_eq!(next(&mut rx).await, "migrating");
    run.control.suspend().unwrap();
    assert_eq!(next(&mut rx).await, "stop-requested");
    let mut topo = runtime
        .submit(Call::new(&probe, Work::write("topology", "disk")))
        .await
        .unwrap();
    state(&topo.control, RunState::WaitingResources).await;
    assert!(!run.control.suspend_acknowledged());
    assert!(probe_events.try_recv().is_err());
    stopped.add_permits(1);
    state(&run.control, RunState::Suspended).await;
    assert_eq!(next(&mut probe_events).await, "topology");
    finish(&mut topo).await.unwrap();
    gate.0.store(false, Ordering::SeqCst);
    run.control.resume().unwrap();
    state(
        &run.control,
        RunState::WaitingConditions("topology changed".into()),
    )
    .await;
    assert!(rx.try_recv().is_err());
    gate.0.store(true, Ordering::SeqCst);
    run.control.recheck();
    assert_eq!(finish(&mut run).await, Ok(100));
    assert_eq!(run.control.activities().total, 3);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn resume_racing_stop_reply_continues_same_step_and_cancel_is_terminal() {
    for cancel in [false, true] {
        let mut runtime = WorkflowRuntime::start("pool", Config::default()).unwrap();
        let (events, mut rx) = mpsc::unbounded_channel();
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
            .submit(Flow::new(Migrate { client, cursor: 0 }))
            .await
            .unwrap();
        assert_eq!(next(&mut rx).await, "migrating");
        run.control.suspend().unwrap();
        assert_eq!(next(&mut rx).await, "stop-requested");
        if cancel {
            run.control.cancel();
        } else {
            run.control.resume().unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(10), run.result())
                .await
                .is_err()
        );
        stopped.add_permits(1);
        assert_eq!(
            finish(&mut run).await,
            if cancel {
                Err(Failure::Cancelled)
            } else {
                Ok(100)
            }
        );
        if cancel {
            assert!(run.control.resume().is_err());
            assert!(rx.try_recv().is_err());
        }
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn uncertainty_cannot_be_reclassified_or_retried() {
    let (mut runtime, client, mut entered) = setup();
    let handled = Arc::new(AtomicBool::new(false));
    let observed = handled.clone();
    let step = Step {
        policy: RetryPolicy::default(),
        client: client.clone(),
        work: [Work::write("uncertain", "disk").behavior(Behavior::Uncertain)].into(),
        handler: move |_| {
            observed.store(true, Ordering::SeqCst);
            Ok(ActivityAction::Continue(1))
        },
    };
    let mut run = runtime.submit(Flow::new(step)).await.unwrap();
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    assert!(!handled.load(Ordering::SeqCst));
    assert_eq!(next(&mut entered).await, "uncertain");
    let mut next_run = runtime
        .submit(Call::new(&client, Work::write("must-not-run", "disk")))
        .await
        .unwrap();
    assert!(matches!(
        finish(&mut next_run).await,
        Err(Failure::Rejected(_))
    ));
    assert!(entered.try_recv().is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn suspend_without_request_is_rejected_instead_of_spinning() {
    let (mut runtime, client, _) = setup();
    let step = Step {
        policy: RetryPolicy::default(),
        client,
        work: [Work::write("check", "disk")].into(),
        handler: |_| Ok(ActivityAction::Suspend(ActivityWait::Resume)),
    };
    let mut run = runtime.submit(Flow::new(step)).await.unwrap();
    assert!(matches!(finish(&mut run).await, Err(Failure::Rejected(_))));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn business_cancel_is_terminal_even_for_existing_durable_workflows() {
    let store = Arc::new(MemoryTaskStore::default());
    let mut driver = WorkflowDriver::new("pool", Config::default())
        .unwrap()
        .with_task_ids(7, TaskIds::for_leader(1).unwrap())
        .unwrap();
    driver.enable_persistence(store.clone()).await.unwrap();
    driver.restore().await.unwrap();
    let (tx, _) = mpsc::unbounded_channel();
    let client = driver.register("probe", Probe::new(tx)).unwrap();
    let step = Step {
        policy: RetryPolicy::default(),
        client,
        work: [Work::write("check", "disk")].into(),
        handler: |_| Ok(ActivityAction::Cancel),
    };
    let mut flow = Flow::new(step);
    flow.durable = true;
    let mut run = driver.submit(flow).unwrap();
    let result = tokio::time::timeout(LIMIT, async {
        loop {
            tokio::select! {
                result = run.result() => break result,
                _ = driver.tick() => {},
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(result, Err(Failure::Cancelled));
    let saved = store.list(7).unwrap().remove(0);
    assert!(saved.cancel_requested);
    assert_eq!(saved.outcome, StoredOutcome::Cancelled);
    driver.shutdown().await.unwrap();
}

struct Abandon<A>(A, Arc<Semaphore>);
#[async_trait]
impl<A: Activity + 'static> Workflow for Abandon<A>
where
    A::Output: 'static,
{
    type Output = A::Output;
    const NAME: &'static str = "abandon.activity";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Self::Output> {
        tokio::select! {
            result = ctx.run_activity(self.0) => result,
            _ = self.1.acquire() => Err(Failure::Cancelled),
        }
    }
}

#[tokio::test]
async fn abandoned_trait_waiter_keeps_native_operation_and_lease_until_settled() {
    let (mut runtime, client, mut entered) = setup();
    let stop = Arc::new(Semaphore::new(0));
    let settled = Arc::new(Semaphore::new(0));
    let step = Step {
        policy: RetryPolicy::default(),
        client: client.clone(),
        work: [Work::write("native", "disk")
            .gated(&settled)
            .behavior(Behavior::CancelThenSettle)]
        .into(),
        handler: |result: Outcome<usize>| result.map(ActivityAction::Continue),
    };
    let mut run = runtime.submit(Abandon(step, stop.clone())).await.unwrap();
    assert_eq!(next(&mut entered).await, "native");
    stop.add_permits(1);
    assert_eq!(next(&mut entered).await, "cancel-observed");
    let root = run.control.activities().entries[0].clone();
    assert_eq!(root.kind, ActivityKind::Step);
    assert!(matches!(root.state, ActivityState::Settling(_)));
    assert!(root.finished_at.is_none());
    let mut other = runtime
        .submit(Call::new(&client, Work::write("other", "disk")))
        .await
        .unwrap();
    state(&other.control, RunState::WaitingResources).await;
    assert!(entered.try_recv().is_err());
    settled.add_permits(1);
    assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
    assert!(run.control.activities().entries[0].finished_at.is_some());
    assert_eq!(next(&mut entered).await, "other");
    finish(&mut other).await.unwrap();
    runtime.shutdown().await.unwrap();
}

struct Reconfigure {
    client: ServiceClient<Probe>,
    change: Arc<Semaphore>,
    settled: Arc<Semaphore>,
    protect: bool,
}
#[async_trait]
impl Workflow for Reconfigure {
    type Output = usize;
    const NAME: &'static str = "reconfigure.activity";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<usize> {
        let mut call = ctx.activity(
            &self.client,
            Work::write("native", "disk")
                .gated(&self.settled)
                .behavior(Behavior::CancelThenSettle),
        );
        tokio::select! {
            result = &mut call => return result,
            _ = self.change.acquire() => {},
        }
        if self.protect {
            call.uninterruptible().await
        } else {
            call.name("too-late").await
        }
    }
}

#[tokio::test]
async fn late_configuration_returns_error_and_still_waits_for_native_settlement() {
    for protect in [false, true] {
        let (mut runtime, client, mut entered) = setup();
        let change = Arc::new(Semaphore::new(0));
        let settled = Arc::new(Semaphore::new(0));
        let mut run = runtime
            .submit(Reconfigure {
                client: client.clone(),
                change: change.clone(),
                settled: settled.clone(),
                protect,
            })
            .await
            .unwrap();
        assert_eq!(next(&mut entered).await, "native");
        change.add_permits(1);
        assert_eq!(next(&mut entered).await, "cancel-observed");
        let mut other = runtime
            .submit(Call::new(&client, Work::write("other", "disk")))
            .await
            .unwrap();
        state(&other.control, RunState::WaitingResources).await;
        assert!(entered.try_recv().is_err());
        settled.add_permits(1);
        assert_eq!(
            finish(&mut run).await,
            Err(Failure::Rejected(
                "configure an Activity before polling it".into()
            ))
        );
        assert_eq!(next(&mut entered).await, "other");
        finish(&mut other).await.unwrap();
        runtime.shutdown().await.unwrap();
    }
}

struct Repoll(ServiceClient<Probe>);
#[async_trait]
impl Workflow for Repoll {
    type Output = usize;
    const NAME: &'static str = "repoll.activity";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<usize> {
        let mut call = ctx.activity(&self.0, Work::write("once", "disk"));
        (&mut call).await?;
        (&mut call).await
    }
}
#[tokio::test]
async fn consumed_call_returns_error_without_reexecuting_or_panicking() {
    let (mut runtime, client, mut entered) = setup();
    let mut run = runtime.submit(Repoll(client)).await.unwrap();
    assert_eq!(
        finish(&mut run).await,
        Err(Failure::Rejected("Activity already consumed".into()))
    );
    assert_eq!(next(&mut entered).await, "once");
    assert!(entered.try_recv().is_err());
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn framework_applies_capped_exponential_backoff_and_rejects_unconfigured_retry() {
    for configured in [false, true] {
        let (mut runtime, client, _) = setup();
        let step = Step {
            policy: if configured {
                RetryPolicy::exponential(3, Duration::from_millis(30), Duration::from_millis(60))
            } else {
                RetryPolicy::default()
            },
            client,
            work: (0..4).map(|_| Work::write("replicate", "disk")).collect(),
            handler: |result: Outcome<usize>| {
                let value = result?;
                Ok(if value == 4 {
                    ActivityAction::Continue(value)
                } else {
                    ActivityAction::Retry {
                        reason: "replicas not ready".into(),
                    }
                })
            },
        };
        let mut run = runtime.submit(Flow::new(step)).await.unwrap();
        let result = finish(&mut run).await;
        let history = run.control.activities();
        let root = &history.entries[0];
        assert_eq!(root.retry_reason.as_deref(), Some("replicas not ready"));
        assert!(root.retry_at.is_none());
        if configured {
            assert_eq!(result, Ok(4));
            assert_eq!(root.attempts, 4);
            assert_eq!(root.retries, 3);
            let calls: Vec<_> = history
                .entries
                .iter()
                .filter(|e| e.parent_id == Some(root.id))
                .collect();
            for (pair, delay) in calls.windows(2).zip([30, 60, 60]) {
                let gap = pair[1]
                    .started_at
                    .unwrap()
                    .duration_since(pair[0].finished_at.unwrap())
                    .unwrap();
                assert!(gap >= Duration::from_millis(delay - 2));
            }
        } else {
            assert!(matches!(result, Err(Failure::Failed(_))));
            assert_eq!(root.attempts, 1);
            assert_eq!(root.retries, 0);
        }
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn business_wait_is_observed_releases_resources_and_survives_user_pause() {
    let (mut runtime, client, mut entered) = setup();
    let gate = Arc::new(Gate(AtomicBool::new(false)));
    let condition = gate.clone();
    let mut first = true;
    let step = Step {
        policy: RetryPolicy::default(),
        client: client.clone(),
        work: (0..2).map(|_| Work::write("check", "disk")).collect(),
        handler: move |result: Outcome<usize>| {
            let value = result?;
            if first {
                first = false;
                Ok(ActivityAction::Suspend(ActivityWait::Condition(
                    condition.clone(),
                )))
            } else {
                Ok(ActivityAction::Continue(value))
            }
        },
    };
    let mut run = runtime.submit(Flow::new(step)).await.unwrap();
    assert_eq!(next(&mut entered).await, "check");
    state(
        &run.control,
        RunState::WaitingConditions("topology changed".into()),
    )
    .await;
    let root = run.control.activities().entries[0].clone();
    assert_eq!(
        root.state,
        ActivityState::WaitingConditions("topology changed".into())
    );
    assert!(root.finished_at.is_none());
    let mut other = runtime
        .submit(Call::new(&client, Work::write("topology", "disk")))
        .await
        .unwrap();
    finish(&mut other).await.unwrap();
    assert_eq!(next(&mut entered).await, "topology");
    run.control.suspend().unwrap();
    state(&run.control, RunState::Suspended).await;
    gate.0.store(true, Ordering::SeqCst);
    run.control.recheck();
    assert_eq!(
        run.control.activities().entries[0].state,
        ActivityState::Suspended
    );
    assert!(entered.try_recv().is_err());
    run.control.resume().unwrap();
    finish(&mut run).await.unwrap();
    let completed = run.control.activities().entries[0].clone();
    assert_eq!(completed.id, root.id);
    assert_eq!(completed.attempts, 2);
    assert_eq!(completed.retries, 0);
    assert_eq!(completed.state, ActivityState::Completed);
    runtime.shutdown().await.unwrap();
}

struct LocalLoop(mpsc::UnboundedSender<ActivityId>);
#[async_trait]
impl Activity for LocalLoop {
    type Reply = ();
    type Output = ();
    fn name(&self) -> &'static str {
        "local.loop"
    }
    fn description(&self) -> Option<&str> {
        Some("local cooperative work")
    }
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<()> {
        self.0.send(scope.id()).unwrap();
        scope.report_progress(1, Some(2));
        scope.sleep(Duration::from_millis(50)).await?;
        scope.report_progress(2, Some(2));
        Ok(())
    }
    fn handle(&mut self, result: Outcome<()>) -> Outcome<ActivityAction<()>> {
        result.map(ActivityAction::Continue)
    }
}
#[tokio::test]
async fn cooperative_loop_parks_without_reentering_execute_and_cancel_wakes_it() {
    for cancel in [false, true] {
        let mut runtime = WorkflowRuntime::start("pool", Config::default()).unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut run = runtime.submit(Flow::new(LocalLoop(tx))).await.unwrap();
        let id = rx.recv().await.unwrap();
        run.control.suspend().unwrap();
        state(&run.control, RunState::Suspended).await;
        let root = run.control.activities().entries[0].clone();
        assert_eq!(root.id, id);
        assert_eq!(root.description.as_deref(), Some("local cooperative work"));
        assert_eq!(root.progress.completed, 1);
        assert_eq!(root.state, ActivityState::Suspended);
        if cancel {
            run.control.cancel();
        } else {
            run.control.resume().unwrap();
        }
        assert_eq!(
            finish(&mut run).await,
            if cancel {
                Err(Failure::Cancelled)
            } else {
                Ok(())
            }
        );
        let root = run.control.activities().entries[0].clone();
        assert_eq!(root.attempts, 1);
        assert_eq!(root.retries, 0);
        assert!(rx.try_recv().is_err());
        if !cancel {
            assert_eq!(root.progress.completed, 2);
        }
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn small_history_keeps_live_parent_and_child_then_retains_logical_summary() {
    let mut runtime = WorkflowRuntime::start(
        "pool",
        Config {
            activity_history: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let (events, mut rx) = mpsc::unbounded_channel();
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
        .submit(Flow::new(Migrate { client, cursor: 0 }))
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await, "migrating");
    let live = run.control.activities();
    assert_eq!(live.entries.len(), 2);
    let id = live.entries[0].id;
    assert_eq!(live.entries[1].parent_id, Some(id));
    run.control.suspend().unwrap();
    assert_eq!(next(&mut rx).await, "stop-requested");
    state(&run.control, RunState::Suspending).await;
    assert_eq!(
        run.control.activities().entries[0].state,
        ActivityState::Suspending
    );
    stopped.add_permits(1);
    state(&run.control, RunState::Suspended).await;
    let paused = run.control.activities();
    assert_eq!(paused.entries.len(), 1);
    assert_eq!(paused.entries[0].state, ActivityState::Suspended);
    run.control.resume().unwrap();
    assert_eq!(finish(&mut run).await, Ok(100));
    let history = run.control.activities();
    assert_eq!(history.total, 3);
    assert_eq!(history.omitted, 2);
    assert_eq!(history.entries.len(), 1);
    assert_eq!(history.entries[0].id, id);
    assert_eq!(history.entries[0].state, ActivityState::Completed);
    runtime.shutdown().await.unwrap();
}

struct Ignore<A>(A);
#[async_trait]
impl<A: Activity + 'static> Workflow for Ignore<A> {
    type Output = ();
    const NAME: &'static str = "ignore.activity.failure";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let _ = ctx.run_activity(self.0).await;
        Ok(())
    }
}
#[tokio::test]
async fn result_handler_panic_remains_uncertain_even_if_workflow_ignores_error() {
    let (mut runtime, client, _) = setup();
    let step = Step {
        policy: RetryPolicy::default(),
        client,
        work: [Work::write("prepare", "disk")].into(),
        handler: |_| panic!("handler bug"),
    };
    let mut run = runtime.submit(Ignore(step)).await.unwrap();
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    let root = run.control.activities().entries[0].clone();
    assert!(matches!(
        root.state,
        ActivityState::Failed(Failure::Uncertain(_))
    ));
    assert!(root.finished_at.is_some());
    runtime.shutdown().await.unwrap();
}

struct Terminal;
#[async_trait]
impl ServiceHandler for Terminal {
    type Request = Failure;
    type Reply = ();
    type Query = ();
    type View = ();
    fn resources(_: &Failure) -> Vec<ResourceAccess> {
        vec![]
    }
    async fn execute(&self, failure: Failure, _: ActivityContext) -> Outcome<()> {
        Err(failure)
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
struct Swallow {
    client: ServiceClient<Terminal>,
    failure: Failure,
    handled: Arc<AtomicBool>,
    after_call: Option<mpsc::UnboundedSender<()>>,
}
#[async_trait]
impl Activity for Swallow {
    type Reply = ();
    type Output = ();
    fn name(&self) -> &'static str {
        "swallow"
    }
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<()> {
        let _ = scope.call(&self.client, self.failure.clone()).await;
        if let Some(ready) = &self.after_call {
            ready.send(()).unwrap();
            std::future::pending::<()>().await;
        }
        Ok(())
    }
    fn handle(&mut self, _: Outcome<()>) -> Outcome<ActivityAction<()>> {
        self.handled.store(true, Ordering::SeqCst);
        Ok(ActivityAction::Continue(()))
    }
}
#[tokio::test]
async fn swallowed_cancelled_or_uncertain_service_result_never_reaches_handler() {
    for failure in [
        Failure::Cancelled,
        Failure::Uncertain("lost native reply".into()),
    ] {
        let mut runtime = WorkflowRuntime::start("pool", Config::default()).unwrap();
        let client = runtime.register("terminal", Terminal).unwrap();
        let handled = Arc::new(AtomicBool::new(false));
        let mut run = runtime
            .submit(Flow::new(Swallow {
                client,
                failure: failure.clone(),
                handled: handled.clone(),
                after_call: None,
            }))
            .await
            .unwrap();
        let result = finish(&mut run).await;
        if failure == Failure::Cancelled {
            assert_eq!(result, Err(Failure::Cancelled));
        } else {
            assert!(matches!(result, Err(Failure::Uncertain(_))));
        }
        assert!(!handled.load(Ordering::SeqCst));
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn abandoning_after_uncertain_reply_does_not_downgrade_parent_to_cancelled() {
    let mut runtime = WorkflowRuntime::start("pool", Config::default()).unwrap();
    let client = runtime.register("terminal", Terminal).unwrap();
    let (ready, mut rx) = mpsc::unbounded_channel();
    let stop = Arc::new(Semaphore::new(0));
    let mut run = runtime
        .submit(Abandon(
            Swallow {
                client,
                failure: Failure::Uncertain("lost native reply".into()),
                handled: Arc::new(AtomicBool::new(false)),
                after_call: Some(ready),
            },
            stop.clone(),
        ))
        .await
        .unwrap();
    tokio::time::timeout(LIMIT, rx.recv())
        .await
        .unwrap()
        .unwrap();
    stop.add_permits(1);
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    assert!(matches!(
        run.control.activities().entries[0].state,
        ActivityState::Failed(Failure::Uncertain(_))
    ));
    runtime.shutdown().await.unwrap();
}

struct PanicInExecute {
    client: ServiceClient<Probe>,
    trigger: Arc<Semaphore>,
    stopped: Arc<Semaphore>,
}
#[async_trait]
impl Activity for PanicInExecute {
    type Reply = usize;
    type Output = usize;
    fn name(&self) -> &'static str {
        "panic.during.native.call"
    }
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<usize> {
        tokio::select! {
            result = scope.call(&self.client, Work::write("native", "disk")
                .gated(&self.stopped).behavior(Behavior::CancelThenSettle)) => result,
            _ = self.trigger.acquire() => panic!("execute bug"),
        }
    }
    fn handle(&mut self, result: Outcome<usize>) -> Outcome<ActivityAction<usize>> {
        result.map(ActivityAction::Continue)
    }
}
struct ObserveReturn(PanicInExecute, Arc<AtomicBool>);
#[async_trait]
impl Workflow for ObserveReturn {
    type Output = ();
    const NAME: &'static str = "observe.activity.return";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let _ = ctx.run_activity(self.0).await;
        self.1.store(true, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn execute_panic_does_not_return_to_workflow_until_actual_native_stop() {
    let (mut runtime, client, mut entered) = setup();
    let trigger = Arc::new(Semaphore::new(0));
    let stopped = Arc::new(Semaphore::new(0));
    let returned = Arc::new(AtomicBool::new(false));
    let mut run = runtime
        .submit(ObserveReturn(
            PanicInExecute {
                client: client.clone(),
                trigger: trigger.clone(),
                stopped: stopped.clone(),
            },
            returned.clone(),
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut entered).await, "native");
    trigger.add_permits(1);
    assert_eq!(next(&mut entered).await, "cancel-observed");
    assert!(!returned.load(Ordering::SeqCst));
    let root = run.control.activities().entries[0].clone();
    assert!(matches!(
        root.state,
        ActivityState::Settling(Failure::Uncertain(_))
    ));
    assert!(root.finished_at.is_none());
    stopped.add_permits(1);
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    assert!(returned.load(Ordering::SeqCst));
    runtime.shutdown().await.unwrap();
}
