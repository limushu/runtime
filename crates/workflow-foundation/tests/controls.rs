#[allow(dead_code)]
mod support;
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use support::*;
use tokio::sync::Semaphore;
use workflow_foundation::*;

struct Steps {
    service: ServiceClient<Probe>,
    gate: Arc<Semaphore>,
}
#[async_trait]
impl Workflow for Steps {
    type Output = usize;
    const NAME: &'static str = "two-steps";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![key("lifecycle")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<usize> {
        ctx.activity(
            &self.service,
            Work::write("first", "data").gated(&self.gate),
        )
        .await?;
        ctx.activity(&self.service, Work::write("second", "data"))
            .await
    }
}

#[tokio::test]
async fn pause_waits_for_inflight_step_and_retains_only_workflow_ownership() {
    let (mut pool, service, mut events) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut run = pool
        .submit(Steps {
            service: service.clone(),
            gate: gate.clone(),
        })
        .await
        .unwrap();
    assert_eq!(next(&mut events).await, "first");
    run.control.suspend().unwrap();
    assert!(run.control.suspend_requested());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), run.control.wait_suspended())
            .await
            .is_err()
    );
    gate.add_permits(1);
    tokio::time::timeout(LIMIT, run.control.wait_suspended())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(service.query(()).await.unwrap().0, 1);
    let mut conflicting = pool
        .submit(Call::new(&service, Work::write("same-lifecycle", "other")).owning(&["lifecycle"]))
        .await
        .unwrap();
    let mut disjoint = pool
        .submit(Call::new(&service, Work::write("same-resource", "data")))
        .await
        .unwrap();
    assert_eq!(next(&mut events).await, "same-resource");
    finish(&mut disjoint).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), conflicting.result())
            .await
            .is_err()
    );
    run.control.resume().unwrap();
    finish(&mut run).await.unwrap();
    finish(&mut conflicting).await.unwrap();
    assert!(run.control.resume().is_err());
    pool.shutdown().await.unwrap();
}

struct Wait {
    timer: bool,
}
#[async_trait]
impl Workflow for Wait {
    type Output = ();
    const NAME: &'static str = "wait";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        if self.timer {
            ctx.sleep(Duration::from_millis(100)).await
        } else {
            ctx.next_signal().await.map(|_| ())
        }
    }
}
#[tokio::test]
async fn signal_is_buffered_while_paused_and_resume_does_not_restart_run() {
    let (mut pool, _, _) = setup();
    let mut run = pool.submit(Wait { timer: false }).await.unwrap();
    state(&run.control, RunState::WaitingSignal).await;
    assert!(run.control.wait_suspended().await.is_err());
    let id = run.id();
    run.control.suspend().unwrap();
    tokio::time::timeout(LIMIT, run.control.wait_suspended())
        .await
        .unwrap()
        .unwrap();
    run.control
        .signal(Signal::new("continue", ""))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), run.result())
            .await
            .is_err()
    );
    run.control.resume().unwrap();
    finish(&mut run).await.unwrap();
    assert_eq!(run.id(), id);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn expired_timer_waits_for_resume_and_cancellation_wakes_paused_run() {
    let (mut pool, _, _) = setup();
    let mut timer = pool.submit(Wait { timer: true }).await.unwrap();
    state(&timer.control, RunState::WaitingTimer).await;
    timer.control.suspend().unwrap();
    tokio::time::timeout(LIMIT, timer.control.wait_suspended())
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(140), timer.result())
            .await
            .is_err()
    );
    timer.control.resume().unwrap();
    tokio::time::timeout(Duration::from_millis(50), timer.result())
        .await
        .unwrap()
        .unwrap();
    let mut paused = pool.submit(Wait { timer: false }).await.unwrap();
    state(&paused.control, RunState::WaitingSignal).await;
    paused.control.suspend().unwrap();
    tokio::time::timeout(LIMIT, paused.control.wait_suspended())
        .await
        .unwrap()
        .unwrap();
    paused.control.cancel();
    assert_eq!(finish(&mut paused).await, Err(Failure::Cancelled));
    assert!(paused.control.resume().is_err());
    pool.shutdown().await.unwrap();
}
#[tokio::test]
async fn pause_withdraws_pending_resource_request_and_shutdown_cancels_pause() {
    let (mut pool, service, mut events) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut holder = pool
        .submit(Call::new(
            &service,
            Work::write("holder", "data").gated(&gate),
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut events).await, "holder");
    let mut waiter = pool
        .submit(Call::new(&service, Work::write("waiter", "data")))
        .await
        .unwrap();
    state(&waiter.control, RunState::WaitingResources).await;
    waiter.control.suspend().unwrap();
    tokio::time::timeout(LIMIT, waiter.control.wait_suspended())
        .await
        .unwrap()
        .unwrap();
    gate.add_permits(1);
    finish(&mut holder).await.unwrap();
    assert!(events.try_recv().is_err());
    pool.shutdown().await.unwrap();
    assert_eq!(finish(&mut waiter).await, Err(Failure::Cancelled));
}

#[tokio::test]
async fn deferred_execution_requires_explicit_start_and_obeys_pause() {
    let (mut runtime, _, _) = setup();
    let (mut run, permit) = runtime.client().defer(Wait { timer: false }).await.unwrap();
    assert_eq!(run.control.state(), RunState::WaitingObjects);
    run.control.suspend().unwrap();
    permit.start();
    tokio::time::timeout(LIMIT, run.control.wait_suspended())
        .await
        .unwrap()
        .unwrap();
    run.control.signal(Signal::new("finish", "")).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), run.result())
            .await
            .is_err()
    );
    run.control.resume().unwrap();
    finish(&mut run).await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn deferred_execution_can_be_cancelled_rejected_or_abandoned_without_running() {
    let (mut runtime, _, _) = setup();
    let (mut cancelled, permit) = runtime.client().defer(Wait { timer: false }).await.unwrap();
    cancelled.control.cancel();
    assert_eq!(finish(&mut cancelled).await, Err(Failure::Cancelled));
    permit.start();
    let (mut rejected, permit) = runtime.client().defer(Wait { timer: false }).await.unwrap();
    permit.reject(Failure::Rejected("fenced".into()));
    assert_eq!(
        finish(&mut rejected).await,
        Err(Failure::Rejected("fenced".into()))
    );
    let (mut abandoned, permit) = runtime.client().defer(Wait { timer: false }).await.unwrap();
    drop(permit);
    assert_eq!(finish(&mut abandoned).await, Err(Failure::Unavailable));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn deferred_execution_counts_toward_capacity_and_shutdown_cancels_it() {
    let mut runtime = WorkflowRuntime::start(
        "limited",
        Config {
            max_workflows: 1,
            ..Config::default()
        },
    )
    .unwrap();
    let (mut first, _permit) = runtime.client().defer(Wait { timer: false }).await.unwrap();
    assert!(matches!(
        runtime.client().defer(Wait { timer: false }).await,
        Err(Failure::Busy)
    ));
    assert!(!first.control.cancellation_requested());
    runtime.shutdown().await.unwrap();
    assert_eq!(finish(&mut first).await, Err(Failure::Cancelled));
}

struct MarkExecution(Arc<std::sync::atomic::AtomicBool>);
#[async_trait]
impl Workflow for MarkExecution {
    type Output = ();
    const NAME: &'static str = "passive-driver-proof";
    async fn run(self, _: &mut WorkflowContext) -> Outcome<()> {
        self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn driver_executes_only_when_polled_by_its_owner() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let ran = Arc::new(AtomicBool::new(false));
    let mut driver = WorkflowDriver::new("embedded", Config::default()).unwrap();
    let (mut run, permit) = driver.defer(MarkExecution(ran.clone())).unwrap();
    permit.start();
    // A spawned host would execute during this yield. The passive driver cannot.
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(!ran.load(Ordering::SeqCst));
    assert_eq!(run.control.state(), RunState::WaitingObjects);
    assert_eq!(
        tokio::time::timeout(LIMIT, driver.tick()).await.unwrap(),
        Some(run.id())
    );
    assert!(ran.load(Ordering::SeqCst));
    finish(&mut run).await.unwrap();
    driver.shutdown().await.unwrap();
}
