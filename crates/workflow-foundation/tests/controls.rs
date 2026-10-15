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
    run.control.pause().unwrap();
    assert!(run.control.pause_requested());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), run.control.wait_paused())
            .await
            .is_err()
    );
    gate.add_permits(1);
    tokio::time::timeout(LIMIT, run.control.wait_paused())
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
    assert!(run.control.wait_paused().await.is_err());
    let id = run.id();
    run.control.pause().unwrap();
    tokio::time::timeout(LIMIT, run.control.wait_paused())
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

struct QueuePolicy;
impl AdmissionPolicy<u32> for QueuePolicy {
    fn decide(&self, _: &u32, _: &u32) -> AdmissionDecision {
        AdmissionDecision::Queue
    }
}
struct MixedPolicy;
impl AdmissionPolicy<u32> for MixedPolicy {
    fn decide(&self, _: &u32, existing: &u32) -> AdmissionDecision {
        if *existing == 1 {
            AdmissionDecision::CancelExisting
        } else {
            AdmissionDecision::Reject("second conflict cannot be replaced".into())
        }
    }
}
#[tokio::test]
async fn any_rejection_prevents_all_partial_cancellation_decisions() {
    let (mut pool, _, _) = setup();
    let client = pool.client();
    let mut a = client
        .submit_with_policy(Wait { timer: false }, 1u32, Arc::new(QueuePolicy))
        .await
        .unwrap();
    let mut b = client
        .submit_with_policy(Wait { timer: false }, 2u32, Arc::new(QueuePolicy))
        .await
        .unwrap();
    assert!(matches!(
        client
            .submit_with_policy(Wait { timer: false }, 3u32, Arc::new(MixedPolicy))
            .await,
        Err(Failure::Rejected(_))
    ));
    assert!(!a.control.cancellation_requested());
    assert!(!b.control.cancellation_requested());
    a.control.cancel();
    b.control.cancel();
    assert_eq!(finish(&mut a).await, Err(Failure::Cancelled));
    assert_eq!(finish(&mut b).await, Err(Failure::Cancelled));
    pool.shutdown().await.unwrap();
}
#[tokio::test]
async fn expired_timer_waits_for_resume_and_cancellation_wakes_paused_run() {
    let (mut pool, _, _) = setup();
    let mut timer = pool.submit(Wait { timer: true }).await.unwrap();
    state(&timer.control, RunState::WaitingTimer).await;
    timer.control.pause().unwrap();
    tokio::time::timeout(LIMIT, timer.control.wait_paused())
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
    paused.control.pause().unwrap();
    tokio::time::timeout(LIMIT, paused.control.wait_paused())
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
    waiter.control.pause().unwrap();
    tokio::time::timeout(LIMIT, waiter.control.wait_paused())
        .await
        .unwrap()
        .unwrap();
    gate.add_permits(1);
    finish(&mut holder).await.unwrap();
    assert!(events.try_recv().is_err());
    pool.shutdown().await.unwrap();
    assert_eq!(finish(&mut waiter).await, Err(Failure::Cancelled));
}

struct Policy;
impl AdmissionPolicy<u32> for Policy {
    fn decide(&self, incoming: &u32, existing: &u32) -> AdmissionDecision {
        if incoming == existing {
            AdmissionDecision::Reject("duplicate".into())
        } else {
            AdmissionDecision::CancelExisting
        }
    }
}
#[tokio::test]
async fn rejected_or_over_capacity_replacement_does_not_cancel_old_run() {
    let mut pool = Coordinator::start(
        "limited",
        Config {
            max_workflows: 1,
            ..Config::default()
        },
    )
    .unwrap();
    let client = pool.client();
    let mut first = client
        .submit_with_policy(Wait { timer: false }, 1u32, Arc::new(Policy))
        .await
        .unwrap();
    state(&first.control, RunState::WaitingSignal).await;
    assert!(matches!(
        client
            .submit_with_policy(Wait { timer: false }, 2u32, Arc::new(Policy))
            .await,
        Err(Failure::Busy)
    ));
    assert!(!first.control.cancellation_requested());
    first.control.cancel();
    assert_eq!(finish(&mut first).await, Err(Failure::Cancelled));
    pool.shutdown().await.unwrap();

    let (mut pool, _, _) = setup();
    let client = pool.client();
    let mut first = client
        .submit_with_policy(Wait { timer: false }, 1u32, Arc::new(Policy))
        .await
        .unwrap();
    assert!(matches!(
        client
            .submit_with_policy(Wait { timer: false }, 1u32, Arc::new(Policy))
            .await,
        Err(Failure::Rejected(_))
    ));
    assert!(!first.control.cancellation_requested());
    let mut replacement = client
        .submit_with_policy(Wait { timer: false }, 2u32, Arc::new(Policy))
        .await
        .unwrap();
    assert_eq!(finish(&mut first).await, Err(Failure::Cancelled));
    replacement
        .control
        .signal(Signal::new("finish", ""))
        .await
        .unwrap();
    finish(&mut replacement).await.unwrap();
    pool.shutdown().await.unwrap();
}

struct PanickingPolicy;
impl AdmissionPolicy<u32> for PanickingPolicy {
    fn decide(&self, _: &u32, _: &u32) -> AdmissionDecision {
        panic!("bad policy")
    }
}
#[tokio::test]
async fn policy_panic_rejects_submission_without_killing_existing_execution() {
    let (mut pool, _, _) = setup();
    let client = pool.client();
    let mut first = client
        .submit_with_policy(Wait { timer: false }, 1u32, Arc::new(Policy))
        .await
        .unwrap();
    assert!(matches!(
        client
            .submit_with_policy(Wait { timer: false }, 2u32, Arc::new(PanickingPolicy))
            .await,
        Err(Failure::Rejected(_))
    ));
    assert!(!first.control.cancellation_requested());
    first
        .control
        .signal(Signal::new("finish", ""))
        .await
        .unwrap();
    finish(&mut first).await.unwrap();
    pool.shutdown().await.unwrap();
}
