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
async fn business_retry_keeps_local_budget_across_pause_and_names_calls() {
    let (mut runtime, client, mut entered) = setup();
    let mut remaining = 2;
    let step = Step {
        client,
        work: (0..2).map(|_| Work::write("check", "disk")).collect(),
        handler: move |result: Outcome<usize>| {
            assert!(result? < 3); // Successful reply, but business condition is not met.
            remaining -= 1;
            if remaining == 0 {
                return Err(Failure::Failed("replication retry limit".into()));
            }
            Ok(ActivityAction::Retry(Duration::from_millis(30)))
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
        Err(Failure::Failed("replication retry limit".into()))
    );
    let history = run.control.activities();
    assert_eq!(history.total, 2);
    assert!(
        history
            .entries
            .iter()
            .all(|entry| entry.name == "check.replication")
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
                ActivityAction::Suspend
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
    assert_eq!(run.control.activities().total, 2);
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
        client,
        work: [Work::write("check", "disk")].into(),
        handler: |_| Ok(ActivityAction::Suspend),
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
impl<A: Activity<Output = usize> + 'static> Workflow for Abandon<A> {
    type Output = usize;
    const NAME: &'static str = "abandon.activity";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<usize> {
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
    let mut other = runtime
        .submit(Call::new(&client, Work::write("other", "disk")))
        .await
        .unwrap();
    state(&other.control, RunState::WaitingResources).await;
    assert!(entered.try_recv().is_err());
    settled.add_permits(1);
    assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
    assert_eq!(next(&mut entered).await, "other");
    finish(&mut other).await.unwrap();
    runtime.shutdown().await.unwrap();
}
