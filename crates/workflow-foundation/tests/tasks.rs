#[allow(dead_code)]
mod support;
use async_trait::async_trait;
use std::sync::Arc;
use support::*;
use tokio::sync::Semaphore;
use workflow_foundation::*;

struct Waiting;
#[async_trait]
impl Workflow for Waiting {
    type Output = ();
    const NAME: &'static str = "observable.wait";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![key("logical")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.report_progress("waiting-for-approval", 1, Some(3));
        ctx.next_signal().await?;
        Ok(())
    }
}
async fn terminal(observer: &mut TaskObserver) -> TaskSnapshot {
    tokio::time::timeout(LIMIT, async {
        loop {
            let snapshot = observer.snapshot();
            if matches!(snapshot.state, RunState::Completed | RunState::Failed(_)) {
                return snapshot;
            }
            observer.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn id_based_controls_and_observation_survive_dropped_run_handle() {
    let (mut pool, _, _) = setup();
    let client = pool.client();
    let run = pool.submit(Waiting).await.unwrap();
    let id = run.id();
    state(&run.control, RunState::WaitingSignal).await;
    let mut observer = client.observe_task(id).unwrap();
    drop(run);
    let snapshot = client.task(id).unwrap();
    assert_eq!(snapshot.pool, "test-pool");
    assert_eq!(snapshot.name, "observable.wait");
    assert_eq!(snapshot.objects, vec![key("logical")]);
    assert_eq!(
        snapshot.progress.stage.as_deref(),
        Some("waiting-for-approval")
    );
    client.suspend(id).unwrap();
    tokio::time::timeout(LIMIT, client.wait_suspended(id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(client.task(id).unwrap().state, RunState::Suspended);
    assert!(client.task(id).unwrap().suspend_requested);
    client.resume(id).unwrap();
    client.cancel(id).unwrap();
    let snapshot = terminal(&mut observer).await;
    assert_eq!(snapshot.state, RunState::Failed(Failure::Cancelled));
    assert!(snapshot.cancellation_requested);
    assert!(client.suspend(id).is_err());
    let mut unknown = id;
    unknown.sequence = u64::MAX;
    assert!(client.cancel(unknown).is_err());
    pool.shutdown().await.unwrap();
    assert_eq!(
        client.task(id).unwrap().state,
        RunState::Failed(Failure::Cancelled)
    );
}

struct UpdateProgress {
    start: Arc<Semaphore>,
    finish: Arc<Semaphore>,
}
#[async_trait]
impl Workflow for UpdateProgress {
    type Output = ();
    const NAME: &'static str = "progress-only";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.report_progress("before", 0, Some(1));
        self.start.acquire().await.unwrap().forget();
        ctx.report_progress("after", 1, Some(1));
        self.finish.acquire().await.unwrap().forget();
        Ok(())
    }
}
#[tokio::test]
async fn observer_wakes_on_progress_even_without_run_state_transition() {
    let (mut pool, _, _) = setup();
    let start = Arc::new(Semaphore::new(0));
    let finish_gate = Arc::new(Semaphore::new(0));
    let mut run = pool
        .submit(UpdateProgress {
            start: start.clone(),
            finish: finish_gate.clone(),
        })
        .await
        .unwrap();
    let mut observer = pool.client().observe_task(run.id()).unwrap();
    tokio::time::timeout(LIMIT, async {
        while observer.snapshot().progress.stage.as_deref() != Some("before") {
            observer.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(observer.snapshot().state, RunState::Running);
    start.add_permits(1);
    tokio::time::timeout(LIMIT, async {
        while observer.snapshot().progress.completed != 1 {
            observer.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert_eq!(observer.snapshot().state, RunState::Running);
    finish_gate.add_permits(1);
    finish(&mut run).await.unwrap();
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn waiting_task_is_observable_and_cancelable_by_id() {
    let (mut pool, _, _) = setup();
    let client = pool.client();
    let mut first = client.submit(Waiting).await.unwrap();
    state(&first.control, RunState::WaitingSignal).await;
    let mut second = client.submit(Waiting).await.unwrap();
    let snapshot = client.task(second.id()).unwrap();
    assert_eq!(snapshot.state, RunState::WaitingObjects);
    client.cancel(second.id()).unwrap();
    assert_eq!(finish(&mut second).await, Err(Failure::Cancelled));
    client.cancel(first.id()).unwrap();
    assert_eq!(finish(&mut first).await, Err(Failure::Cancelled));
    pool.shutdown().await.unwrap();
}

struct Immediate(bool);
#[async_trait]
impl Workflow for Immediate {
    type Output = ();
    const NAME: &'static str = "immediate";
    async fn run(self, _: &mut WorkflowContext) -> Outcome<()> {
        if self.0 {
            Err(Failure::Uncertain("unknown commit".into()))
        } else {
            Ok(())
        }
    }
}
#[tokio::test]
async fn history_is_bounded_but_uncertain_tasks_and_existing_observers_are_retained() {
    let mut pool = WorkflowRuntime::start(
        "p",
        Config {
            task_history: 1,
            ..Config::default()
        },
    )
    .unwrap();
    let client = pool.client();
    let mut uncertain = pool.submit(Immediate(true)).await.unwrap();
    assert!(finish(&mut uncertain).await.is_err());
    let mut first = pool.submit(Immediate(false)).await.unwrap();
    let observer = client.observe_task(first.id()).unwrap();
    finish(&mut first).await.unwrap();
    let mut last = pool.submit(Immediate(false)).await.unwrap();
    finish(&mut last).await.unwrap();
    tokio::time::timeout(LIMIT, async {
        while client.task(first.id()).is_ok() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(client.task(uncertain.id()).is_ok());
    assert!(client.task(last.id()).is_ok());
    assert_eq!(observer.snapshot().state, RunState::Completed);
    assert_eq!(client.tasks().len(), 2);
    pool.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejected_admission_is_not_registered_as_an_accepted_task() {
    let mut pool = WorkflowRuntime::start(
        "p",
        Config {
            max_workflows: 1,
            ..Config::default()
        },
    )
    .unwrap();
    let client = pool.client();
    let mut first = pool.submit(Waiting).await.unwrap();
    assert!(matches!(pool.submit(Waiting).await, Err(Failure::Busy)));
    assert_eq!(client.tasks().len(), 1);
    client.cancel(first.id()).unwrap();
    assert_eq!(finish(&mut first).await, Err(Failure::Cancelled));
    pool.shutdown().await.unwrap();
}

struct InvalidProgress;
#[async_trait]
impl Workflow for InvalidProgress {
    type Output = ();
    const NAME: &'static str = "invalid-observation";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.report_progress("valid", 1, Some(2));
        ctx.report_progress("", 0, None);
        ctx.report_progress("invalid-count", 3, Some(2));
        ctx.report_progress("x".repeat(257), 0, None);
        Ok(())
    }
}
#[tokio::test]
async fn invalid_observation_does_not_fail_business_or_replace_valid_progress() {
    let (mut pool, _, _) = setup();
    let mut run = pool.submit(InvalidProgress).await.unwrap();
    finish(&mut run).await.unwrap();
    let snapshot = pool.client().task(run.id()).unwrap();
    assert_eq!(snapshot.state, RunState::Completed);
    assert_eq!(snapshot.progress.stage.as_deref(), Some("valid"));
    pool.shutdown().await.unwrap();
}
