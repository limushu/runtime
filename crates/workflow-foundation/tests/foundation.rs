mod support;
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use support::*;
use tokio::sync::{Semaphore, mpsc};
use workflow_foundation::*;

struct Pipeline {
    first: ServiceClient<Probe>,
    second: ServiceClient<Probe>,
}
#[async_trait]
impl Workflow for Pipeline {
    type Output = String;
    const NAME: &'static str = "test.pipeline";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<String> {
        let first = ctx
            .activity(&self.first, Work::write("first", "source"))
            .await?;
        ctx.sleep(Duration::from_millis(1)).await?;
        let signal = ctx.next_signal().await?;
        let second = ctx
            .activity(&self.second, Work::write("second", "target"))
            .await?;
        Ok(format!("{first}:{}:{second}", signal.payload))
    }
}
#[tokio::test]
async fn typed_pipeline_calls_two_services_and_accepts_an_early_signal() {
    let (mut pool, first, mut rx) = setup();
    let (tx2, mut rx2) = mpsc::unbounded_channel();
    let second = pool.register("second", Probe::new(tx2)).unwrap();
    let mut run = pool
        .submit(Pipeline {
            first: first.clone(),
            second: second.clone(),
        })
        .await
        .unwrap();
    run.control
        .signal(Signal::new("continue", "approved"))
        .await
        .unwrap();
    assert_eq!(finish(&mut run).await.unwrap(), "1:approved:1");
    assert_eq!(next(&mut rx).await, "first");
    assert_eq!(next(&mut rx2).await, "second");
    let first_id = first.query(()).await.unwrap().1[0];
    let second_id = second.query(()).await.unwrap().1[0];
    assert_eq!(first_id.workflow, run.id());
    assert_eq!(second_id.workflow, run.id());
    assert_eq!((first_id.sequence, second_id.sequence), (1, 2));
    assert_eq!(run.control.state(), RunState::Completed);
    assert!(matches!(run.result().await, Err(Failure::Rejected(_))));
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn logical_ownership_serializes_workflows_but_other_objects_progress() {
    let (mut pool, service, mut rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut a = pool
        .submit(Call::new(&service, Work::write("a", "r1").gated(&gate)).owning(&["logical-a"]))
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await, "a");
    let mut b = pool
        .submit(Call::new(&service, Work::write("b", "r2")).owning(&["logical-a"]))
        .await
        .unwrap();
    let mut c = pool
        .submit(Call::new(&service, Work::write("c", "r3")).owning(&["logical-c"]))
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await, "c");
    finish(&mut c).await.unwrap();
    assert_eq!(b.control.state(), RunState::WaitingObjects);
    gate.add_permits(1);
    finish(&mut a).await.unwrap();
    finish(&mut b).await.unwrap();
    assert_eq!(next(&mut rx).await, "b");
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn resource_writers_conflict_even_with_different_workflow_objects() {
    let (mut pool, service, mut rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut a = pool
        .submit(Call::new(&service, Work::write("a", "shared").gated(&gate)).owning(&["a"]))
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await, "a");
    let mut b = pool
        .submit(Call::new(&service, Work::write("b", "shared")).owning(&["b"]))
        .await
        .unwrap();
    state(&b.control, RunState::WaitingResources).await;
    assert!(rx.try_recv().is_err());
    gate.add_permits(1);
    finish(&mut a).await.unwrap();
    finish(&mut b).await.unwrap();
    assert_eq!(next(&mut rx).await, "b");
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn resource_sets_are_atomic_and_conflicting_waiters_are_fifo() {
    let (mut pool, service, mut rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut a = pool
        .submit(Call::new(&service, Work::write("a", "a").gated(&gate)))
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await, "a");
    let mut both = Work::write("both", "b");
    both.access.push(write("a"));
    let mut ab = pool
        .submit(Call::new(&service, both.gated(&gate)))
        .await
        .unwrap();
    state(&ab.control, RunState::WaitingResources).await;
    // Ensure the earlier acquisition has actually reached the central table.
    wait_counts(&pool.client(), 1).await;
    let mut b = pool
        .submit(Call::new(&service, Work::write("b", "b")))
        .await
        .unwrap();
    wait_counts(&pool.client(), 2).await;
    assert!(rx.try_recv().is_err());
    gate.add_permits(1);
    finish(&mut a).await.unwrap();
    assert_eq!(next(&mut rx).await, "both");
    assert!(rx.try_recv().is_err());
    gate.add_permits(1);
    finish(&mut ab).await.unwrap();
    finish(&mut b).await.unwrap();
    assert_eq!(next(&mut rx).await, "b");
    pool.shutdown().await.unwrap();
}
async fn wait_counts(client: &RuntimeHandle, waiting: usize) {
    tokio::time::timeout(LIMIT, async {
        while client.snapshot().waiting < waiting {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn shared_readers_run_together_and_writer_waits() {
    let (mut pool, service, mut rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let make = |name| Work {
        name,
        access: vec![read("resource")],
        gate: Some(gate.clone()),
        behavior: Behavior::Success,
    };
    let mut a = pool.submit(Call::new(&service, make("r1"))).await.unwrap();
    let mut b = pool.submit(Call::new(&service, make("r2"))).await.unwrap();
    assert_eq!(next(&mut rx).await, "r1");
    assert_eq!(next(&mut rx).await, "r2");
    assert_eq!(service.snapshot().running, 2);
    let mut w = pool
        .submit(Call::new(&service, Work::write("writer", "resource")))
        .await
        .unwrap();
    wait_counts(&pool.client(), 1).await;
    assert!(rx.try_recv().is_err());
    gate.add_permits(2);
    finish(&mut a).await.unwrap();
    finish(&mut b).await.unwrap();
    finish(&mut w).await.unwrap();
    assert_eq!(next(&mut rx).await, "writer");
    pool.shutdown().await.unwrap();
}

struct WaitAfterStep {
    service: ServiceClient<Probe>,
}
#[async_trait]
impl Workflow for WaitAfterStep {
    type Output = ();
    const NAME: &'static str = "test.wait-after-step";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![key("logical")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.activity(&self.service, Work::write("first-step", "resource"))
            .await?;
        ctx.next_signal().await?;
        Ok(())
    }
}
#[tokio::test]
async fn event_wait_holds_workflow_ownership_but_not_completed_step_resources() {
    let (mut pool, service, mut rx) = setup();
    let mut wait = pool
        .submit(WaitAfterStep {
            service: service.clone(),
        })
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await, "first-step");
    state(&wait.control, RunState::WaitingSignal).await;
    let mut same_object = pool
        .submit(Call::new(&service, Work::write("same-object", "other")).owning(&["logical"]))
        .await
        .unwrap();
    let mut resource = pool
        .submit(Call::new(
            &service,
            Work::write("resource-reused", "resource"),
        ))
        .await
        .unwrap();
    finish(&mut resource).await.unwrap();
    assert_eq!(next(&mut rx).await, "resource-reused");
    assert_eq!(same_object.control.state(), RunState::WaitingObjects);
    wait.control
        .signal(Signal::new("continue", ""))
        .await
        .unwrap();
    finish(&mut wait).await.unwrap();
    finish(&mut same_object).await.unwrap();
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_waits_for_activity_settlement_before_object_handoff() {
    let (mut pool, service, mut rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut a = pool
        .submit(
            Call::new(
                &service,
                Work::write("old", "resource")
                    .gated(&gate)
                    .behavior(Behavior::CancelThenSettle),
            )
            .owning(&["logical"]),
        )
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await, "old");
    let mut b = pool
        .submit(Call::new(&service, Work::write("replacement", "resource")).owning(&["logical"]))
        .await
        .unwrap();
    a.control.cancel();
    assert_eq!(next(&mut rx).await, "cancel-observed");
    assert_eq!(b.control.state(), RunState::WaitingObjects);
    assert!(rx.try_recv().is_err());
    gate.add_permits(1);
    assert_eq!(finish(&mut a).await, Err(Failure::Cancelled));
    finish(&mut b).await.unwrap();
    assert_eq!(next(&mut rx).await, "replacement");
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelling_waiting_acquisition_removes_it_without_blocking_followers() {
    let (mut pool, service, mut rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut a = pool
        .submit(Call::new(
            &service,
            Work::write("a", "resource").gated(&gate),
        ))
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await, "a");
    let mut b = pool
        .submit(Call::new(&service, Work::write("never", "resource")))
        .await
        .unwrap();
    wait_counts(&pool.client(), 1).await;
    b.control.cancel();
    assert_eq!(finish(&mut b).await, Err(Failure::Cancelled));
    let mut c = pool
        .submit(Call::new(&service, Work::write("c", "resource")))
        .await
        .unwrap();
    gate.add_permits(1);
    finish(&mut a).await.unwrap();
    finish(&mut c).await.unwrap();
    assert_eq!(next(&mut rx).await, "c");
    pool.shutdown().await.unwrap();
}

struct Waiting {
    timer: bool,
}
#[async_trait]
impl Workflow for Waiting {
    type Output = ();
    const NAME: &'static str = "test.waiting";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        if self.timer {
            ctx.sleep(Duration::from_secs(3600)).await
        } else {
            ctx.next_signal().await.map(|_| ())
        }
    }
}
#[tokio::test]
async fn timer_and_signal_waits_are_cancelable_without_activity_execution() {
    let mut pool = WorkflowRuntime::start("p", Config::default()).unwrap();
    for timer in [true, false] {
        let mut run = pool.submit(Waiting { timer }).await.unwrap();
        state(
            &run.control,
            if timer {
                RunState::WaitingTimer
            } else {
                RunState::WaitingSignal
            },
        )
        .await;
        run.control.cancel();
        assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
    }
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_run_handle_does_not_cancel_accepted_execution() {
    let (mut pool, service, mut rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let run = pool
        .submit(Call::new(
            &service,
            Work::write("detached", "resource").gated(&gate),
        ))
        .await
        .unwrap();
    let control = run.control.clone();
    drop(run);
    assert_eq!(next(&mut rx).await, "detached");
    assert!(!control.cancellation_requested());
    gate.add_permits(1);
    state(&control, RunState::Completed).await;
    assert_eq!(service.query(()).await.unwrap().0, 1);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn canceling_result_wait_does_not_lose_the_result_or_cancel_execution() {
    let (mut pool, service, mut rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut run = pool
        .submit(Call::new(&service, Work::write("stable", "r").gated(&gate)))
        .await
        .unwrap();
    assert_eq!(next(&mut rx).await, "stable");
    assert!(
        tokio::time::timeout(Duration::from_millis(10), run.result())
            .await
            .is_err()
    );
    assert!(!run.control.cancellation_requested());
    gate.add_permits(1);
    assert_eq!(finish(&mut run).await.unwrap(), 1);
    pool.shutdown().await.unwrap();
}

struct DropResponse {
    service: ServiceClient<Probe>,
    work: Work,
    entered: mpsc::UnboundedReceiver<&'static str>,
    returned: mpsc::UnboundedSender<&'static str>,
    panic: bool,
}
#[async_trait]
impl Workflow for DropResponse {
    type Output = ();
    const NAME: &'static str = "test.drop-response";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![key("logical")]
    }
    async fn run(mut self, ctx: &mut WorkflowContext) -> Outcome<()> {
        {
            let call = ctx.activity(&self.service, self.work);
            tokio::pin!(call);
            tokio::select! { _ = &mut call => panic!("expected blocked activity"), _ = self.entered.recv() => {} }
        }
        let _ = self.returned.send("body-returned");
        if self.panic {
            panic!("workflow panic after dispatch");
        }
        // A dropped response is not permission to dispatch another conflicting step.
        assert!(matches!(
            ctx.activity(&self.service, Work::write("never", "r")).await,
            Err(Failure::Rejected(_))
        ));
        Ok(())
    }
}
#[tokio::test]
async fn dropped_activity_response_keeps_resource_and_typed_result_waiting_until_settlement() {
    let (mut pool, service, rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let (tx, mut returned) = mpsc::unbounded_channel();
    let mut run = pool
        .submit(DropResponse {
            service: service.clone(),
            work: Work::write("started", "r").gated(&gate),
            entered: rx,
            returned: tx,
            panic: false,
        })
        .await
        .unwrap();
    assert_eq!(next(&mut returned).await, "body-returned");
    state(&run.control, RunState::Settling).await;
    assert_eq!(
        pool.client().task(run.id()).unwrap().activities.entries[0].state,
        ActivityState::Running
    );
    let mut next_run = pool
        .submit(Call::new(&service, Work::write("next", "r")))
        .await
        .unwrap();
    wait_counts(&pool.client(), 1).await;
    assert_eq!(service.query(()).await.unwrap().1.len(), 1);
    gate.add_permits(1);
    finish(&mut run).await.unwrap();
    finish(&mut next_run).await.unwrap();
    assert_eq!(
        pool.client().task(run.id()).unwrap().activities.entries[0].state,
        ActivityState::Completed
    );
    assert_eq!(service.query(()).await.unwrap().0, 2);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn workflow_panic_waits_for_outstanding_activity_and_fences_ownership() {
    let (mut pool, service, rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let (tx, mut returned) = mpsc::unbounded_channel();
    let mut run = pool
        .submit(DropResponse {
            service: service.clone(),
            work: Work::write("started", "r").gated(&gate),
            entered: rx,
            returned: tx,
            panic: true,
        })
        .await
        .unwrap();
    next(&mut returned).await;
    state(&run.control, RunState::Settling).await;
    let mut next_run = pool
        .submit(Call::new(&service, Work::write("never", "other")).owning(&["logical"]))
        .await
        .unwrap();
    wait_counts(&pool.client(), 1).await;
    gate.add_permits(1);
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    assert!(matches!(
        finish(&mut next_run).await,
        Err(Failure::Rejected(_))
    ));
    // The actual activity had a known success, so its resource is not uncertain.
    let mut resource = pool
        .submit(Call::new(&service, Work::write("resource", "r")))
        .await
        .unwrap();
    finish(&mut resource).await.unwrap();
    pool.shutdown().await.unwrap();
}

struct IgnoreFailure {
    service: ServiceClient<Probe>,
    work: Work,
}
#[async_trait]
impl Workflow for IgnoreFailure {
    type Output = ();
    const NAME: &'static str = "test.ignore-error";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let _ = ctx.activity(&self.service, self.work).await;
        assert!(matches!(
            ctx.activity(&self.service, Work::write("never", "other"))
                .await,
            Err(Failure::Uncertain(_))
        ));
        Ok(())
    }
}
#[tokio::test]
async fn uncertain_and_panicking_activities_cannot_be_hidden_or_spread_by_waiters() {
    for behavior in [Behavior::Uncertain, Behavior::Panic] {
        let (mut pool, service, _) = setup();
        let mut run = pool
            .submit(IgnoreFailure {
                service: service.clone(),
                work: Work::write("uncertain", "r").behavior(behavior),
            })
            .await
            .unwrap();
        assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
        let mut waiter = pool
            .submit(Call::new(&service, Work::write("never", "r")).owning(&["unrelated-owner"]))
            .await
            .unwrap();
        assert!(matches!(
            finish(&mut waiter).await,
            Err(Failure::Rejected(_))
        ));
        let mut independent = pool
            .submit(Call::new(&service, Work::write("safe", "other")).owning(&["unrelated-owner"]))
            .await
            .unwrap();
        finish(&mut independent).await.unwrap();
        pool.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn known_failure_releases_resources_for_later_execution() {
    let (mut pool, service, _) = setup();
    let mut failed = pool
        .submit(Call::new(
            &service,
            Work::write("failed", "r").behavior(Behavior::Failed),
        ))
        .await
        .unwrap();
    assert!(matches!(finish(&mut failed).await, Err(Failure::Failed(_))));
    let mut success = pool
        .submit(Call::new(&service, Work::write("success", "r")))
        .await
        .unwrap();
    finish(&mut success).await.unwrap();
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn pools_and_instances_are_isolated_even_with_equal_names_and_keys() {
    let (mut a, a_service, mut rx) = setup();
    let (mut b, b_service, _) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut first = a
        .submit(
            Call::new(&a_service, Work::write("blocked", "same").gated(&gate)).owning(&["same"]),
        )
        .await
        .unwrap();
    next(&mut rx).await;
    let mut second = b
        .submit(Call::new(&b_service, Work::write("independent", "same")).owning(&["same"]))
        .await
        .unwrap();
    assert_eq!(finish(&mut second).await.unwrap(), 1);
    let mut wrong_pool = b
        .submit(Call::new(&a_service, Work::write("never", "same")))
        .await
        .unwrap();
    assert!(matches!(
        finish(&mut wrong_pool).await,
        Err(Failure::Rejected(_))
    ));
    assert_eq!(a_service.query(()).await.unwrap().1.len(), 1);
    gate.add_permits(1);
    finish(&mut first).await.unwrap();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn shutdown_wait_is_cancelable_and_final_commit_still_wins() {
    let (mut pool, service, mut rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let client = pool.client();
    let mut run = pool
        .submit(Call::new(&service, Work::write("commit", "r").gated(&gate)))
        .await
        .unwrap();
    next(&mut rx).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(10), pool.shutdown())
            .await
            .is_err()
    );
    assert_eq!(service.snapshot().lifecycle, Lifecycle::Running);
    assert!(matches!(
        client.submit(Waiting { timer: true }).await,
        Err(Failure::Unavailable)
    ));
    assert!(run.control.cancellation_requested());
    gate.add_permits(1);
    assert_eq!(finish(&mut run).await.unwrap(), 1);
    tokio::time::timeout(LIMIT, pool.shutdown())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(service.snapshot().lifecycle, Lifecycle::Stopped);
    assert_eq!(client.snapshot().lifecycle, Lifecycle::Stopped);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn independent_service_shutdown_waits_for_its_activity_without_stopping_pool() {
    let (mut pool, service, mut rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let mut run = pool
        .submit(Call::new(
            &service,
            Work::write("old", "r")
                .gated(&gate)
                .behavior(Behavior::CancelThenSettle),
        ))
        .await
        .unwrap();
    next(&mut rx).await;
    let control = service.clone();
    let shutdown = tokio::spawn(async move { control.shutdown().await });
    assert_eq!(next(&mut rx).await, "cancel-observed");
    assert!(!shutdown.is_finished());
    gate.add_permits(1);
    assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
    shutdown.await.unwrap().unwrap();
    assert_eq!(pool.client().snapshot().lifecycle, Lifecycle::Running);
    let mut unavailable = pool
        .submit(Call::new(&service, Work::write("never", "r")))
        .await
        .unwrap();
    assert_eq!(finish(&mut unavailable).await, Err(Failure::Unavailable));
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_configuration_names_objects_and_signal_payloads_are_rejected() {
    assert!(WorkflowRuntime::start(" ", Config::default()).is_err());
    assert!(
        WorkflowRuntime::start(
            "p",
            Config {
                capacity: 0,
                ..Default::default()
            }
        )
        .is_err()
    );
    let mut pool = WorkflowRuntime::start(
        "p",
        Config {
            max_signal_bytes: 4,
            ..Default::default()
        },
    )
    .unwrap();
    let (tx, _) = mpsc::unbounded_channel();
    pool.register("service", Probe::new(tx.clone())).unwrap();
    assert!(pool.register("service", Probe::new(tx)).is_err());
    let mut run = pool.submit(Waiting { timer: false }).await.unwrap();
    assert!(matches!(
        run.control.signal(Signal::new("", "")).await,
        Err(Failure::Rejected(_))
    ));
    assert!(matches!(
        run.control.signal(Signal::new("large", "message")).await,
        Err(Failure::Rejected(_))
    ));
    run.control.signal(Signal::new("ok", "!")).await.unwrap();
    finish(&mut run).await.unwrap();
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn workflow_admission_has_a_total_inflight_limit() {
    let mut pool = WorkflowRuntime::start(
        "p",
        Config {
            max_workflows: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let mut run = pool.submit(Waiting { timer: true }).await.unwrap();
    state(&run.control, RunState::WaitingTimer).await;
    assert!(matches!(
        pool.submit(Waiting { timer: true }).await,
        Err(Failure::Busy)
    ));
    run.control.cancel();
    finish(&mut run).await.unwrap_err();
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn forcefully_dropped_pool_reports_uncertainty_and_closes_old_clients() {
    let (pool, service, mut rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let client = pool.client();
    let mut run = pool
        .submit(Call::new(&service, Work::write("active", "r").gated(&gate)))
        .await
        .unwrap();
    next(&mut rx).await;
    drop(pool);
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    assert!(matches!(
        client.submit(Waiting { timer: true }).await,
        Err(Failure::Unavailable)
    ));
    assert!(matches!(service.query(()).await, Err(Failure::Unavailable)));
}

#[tokio::test]
async fn invalid_object_declarations_are_rejected_before_calling_a_service() {
    let (mut pool, service, mut rx) = setup();
    let mut invalid_resource = Work::write("never", "r");
    invalid_resource.access[0].object.namespace = String::new();
    let mut run = pool
        .submit(Call::new(&service, invalid_resource))
        .await
        .unwrap();
    assert!(matches!(finish(&mut run).await, Err(Failure::Rejected(_))));
    let mut invalid_owner = Call::new(&service, Work::write("never", "r"));
    invalid_owner.objects.push(ObjectKey::new("", "id"));
    let mut run = pool.submit(invalid_owner).await.unwrap();
    assert!(matches!(finish(&mut run).await, Err(Failure::Rejected(_))));
    assert!(rx.try_recv().is_err());
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn bounded_signal_buffer_applies_backpressure_and_cancel_releases_sender() {
    let mut pool = WorkflowRuntime::start(
        "p",
        Config {
            capacity: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let mut run = pool.submit(Waiting { timer: true }).await.unwrap();
    state(&run.control, RunState::WaitingTimer).await;
    run.control.signal(Signal::new("first", "")).await.unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(10),
            run.control.signal(Signal::new("second", ""))
        )
        .await
        .is_err()
    );
    let control = run.control.clone();
    let sender = tokio::spawn(async move { control.signal(Signal::new("pending", "")).await });
    run.control.cancel();
    assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
    assert_eq!(sender.await.unwrap(), Err(Failure::Unavailable));
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_the_last_run_handle_does_not_end_a_signal_wait() {
    let mut pool = WorkflowRuntime::start("p", Config::default()).unwrap();
    let run = pool.submit(Waiting { timer: false }).await.unwrap();
    state(&run.control, RunState::WaitingSignal).await;
    drop(run);
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(pool.client().snapshot().workflows, 1);
    tokio::time::timeout(LIMIT, pool.shutdown())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn duplicate_read_write_claims_are_normalized_to_exclusive_access() {
    let (mut pool, service, mut rx) = setup();
    let gate = Arc::new(Semaphore::new(0));
    let request = Work {
        name: "normalized",
        access: vec![read("r"), write("r"), read("r")],
        gate: Some(gate.clone()),
        behavior: Behavior::Success,
    };
    let mut a = pool.submit(Call::new(&service, request)).await.unwrap();
    next(&mut rx).await;
    let request = Work {
        name: "reader",
        access: vec![read("r")],
        gate: None,
        behavior: Behavior::Success,
    };
    let mut b = pool.submit(Call::new(&service, request)).await.unwrap();
    wait_counts(&pool.client(), 1).await;
    assert!(rx.try_recv().is_err());
    gate.add_permits(1);
    finish(&mut a).await.unwrap();
    finish(&mut b).await.unwrap();
    assert_eq!(next(&mut rx).await, "reader");
    pool.shutdown().await.unwrap();
}

struct SerialCheck {
    active: std::sync::atomic::AtomicUsize,
    commits: std::sync::atomic::AtomicUsize,
}
#[async_trait]
impl ServiceHandler for SerialCheck {
    type Request = ();
    type Reply = usize;
    type Query = ();
    type View = usize;
    fn resources(_: &()) -> Vec<ResourceAccess> {
        vec![write("shared-counter")]
    }
    async fn execute(&self, _: (), _: ActivityContext) -> Outcome<usize> {
        use std::sync::atomic::Ordering::SeqCst;
        assert_eq!(
            self.active.fetch_add(1, SeqCst),
            0,
            "central arbitration allowed overlap"
        );
        tokio::task::yield_now().await;
        let value = self.commits.fetch_add(1, SeqCst) + 1;
        self.active.fetch_sub(1, SeqCst);
        Ok(value)
    }
    fn query(&self, _: ()) -> Outcome<usize> {
        Ok(self.commits.load(std::sync::atomic::Ordering::SeqCst))
    }
}
struct Increment(ServiceClient<SerialCheck>);
#[async_trait]
impl Workflow for Increment {
    type Output = usize;
    const NAME: &'static str = "increment";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<usize> {
        ctx.activity(&self.0, ()).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_thread_contention_keeps_domain_mutations_serial_without_domain_locks() {
    let mut pool = WorkflowRuntime::start("p", Config::default()).unwrap();
    let service = pool
        .register(
            "counter",
            SerialCheck {
                active: 0.into(),
                commits: 0.into(),
            },
        )
        .unwrap();
    let mut tasks = vec![];
    for _ in 0..64 {
        let client = pool.client();
        let service = service.clone();
        tasks.push(tokio::spawn(async move {
            let mut run = client.submit(Increment(service)).await.unwrap();
            finish(&mut run).await.unwrap()
        }));
    }
    let mut values = vec![];
    for task in tasks {
        values.push(task.await.unwrap());
    }
    values.sort_unstable();
    assert_eq!(values, (1..=64).collect::<Vec<_>>());
    assert_eq!(service.query(()).await.unwrap(), 64);
    pool.shutdown().await.unwrap();
}
