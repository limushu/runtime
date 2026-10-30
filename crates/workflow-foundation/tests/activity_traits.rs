#[allow(dead_code)]
mod support;
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use support::*;
use tokio::sync::{Semaphore, mpsc, oneshot};
use workflow_foundation::*;

struct StepFlow<A>(A);
#[async_trait]
impl<A: Activity> Workflow for StepFlow<A> {
    type Output = ActivityExit<A::Output, ()>;
    const NAME: &'static str = "trait-step";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Self::Output> {
        ctx.run_activity(self.0).await
    }
}

struct Check {
    client: ServiceClient<Probe>,
    attempts: usize,
    delay: Duration,
}
#[async_trait]
impl Activity for Check {
    type Output = (usize, usize);
    fn name(&self) -> &'static str {
        "check-disk"
    }
    fn description(&self) -> Option<&str> {
        Some("wait for the disk to become ready")
    }
    fn resources(&self) -> Vec<ResourceAccess> {
        vec![write("disk")]
    }
    fn retry(&self) -> Option<Retry> {
        Some(Retry::interval(self.delay).max_attempts(3))
    }
    async fn execute(
        &mut self,
        scope: &mut ActivityScope,
    ) -> Outcome<ActivityDecision<Self::Output>> {
        self.attempts += 1;
        let value = scope
            .call(&self.client, Work::write("check", "disk"))
            .await?;
        if value < 3 {
            Ok(ActivityDecision::Retry {
                reason: format!("value {value} is not ready"),
            })
        } else {
            Ok(ActivityDecision::Complete((self.attempts, value)))
        }
    }
}
#[tokio::test]
async fn named_step_owns_metadata_and_retains_its_state_across_attempts() {
    let (mut runtime, client, _) = setup();
    let mut run = runtime
        .submit(StepFlow(Check {
            client: client.clone(),
            attempts: 0,
            delay: Duration::ZERO,
        }))
        .await
        .unwrap();
    assert_eq!(
        finish(&mut run).await.unwrap(),
        ActivityExit::Completed((3, 3))
    );
    let history = run.control.activities();
    assert_eq!(history.total, 1);
    let step = &history.entries[0];
    assert_eq!(step.name, "check-disk");
    assert_eq!(
        step.description.as_deref(),
        Some("wait for the disk to become ready")
    );
    assert_eq!(step.resources, vec![write("disk")]);
    assert_eq!(step.attempts, 3);
    assert_eq!(step.retry_reason.as_deref(), Some("value 2 is not ready"));
    assert_eq!(step.retry_at, None);
    assert_eq!(client.query(()).await.unwrap().1, vec![step.id; 3]);
    runtime.shutdown().await.unwrap();
}

struct ResumeCheck(ServiceClient<Probe>);
#[async_trait]
impl Workflow for ResumeCheck {
    type Output = (usize, usize);
    const NAME: &'static str = "resume-check";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Self::Output> {
        loop {
            match ctx
                .run_activity(Check {
                    client: self.0.clone(),
                    attempts: 0,
                    delay: Duration::from_secs(60),
                })
                .await?
            {
                ActivityExit::Completed(value) => return Ok(value),
                ActivityExit::Suspended(()) => ctx.wait_until_runnable().await?,
            }
        }
    }
}
async fn wait_backoff(runtime: &WorkflowRuntime, id: TaskId) {
    let mut observer = runtime.client().observe_task(id).unwrap();
    tokio::time::timeout(LIMIT, async {
        while !observer
            .snapshot()
            .activities
            .entries
            .back()
            .is_some_and(|step| step.retry_at.is_some())
        {
            observer.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn named_step_uses_the_same_pause_cancel_and_resource_boundaries() {
    for cancel in [false, true] {
        let (mut runtime, client, _) = setup();
        let mut run = runtime.submit(ResumeCheck(client.clone())).await.unwrap();
        let id = run.id();
        wait_backoff(&runtime, id).await;
        let mut other = runtime
            .submit(Call::new(&client, Work::write("other", "disk")))
            .await
            .unwrap();
        state(&other.control, RunState::WaitingResources).await;
        if cancel {
            run.control.cancel();
            assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
            assert!(run.control.resume().is_err());
        } else {
            run.control.suspend().unwrap();
            tokio::time::timeout(LIMIT, run.control.wait_suspended())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                run.control.activities().entries[0].state,
                ActivityState::Completed
            );
        }
        finish(&mut other).await.unwrap();
        if !cancel {
            run.control.resume().unwrap();
            // A new invocation has a fresh step instance, but the same TaskId.
            assert_eq!(finish(&mut run).await.unwrap(), (1, 3));
            assert_eq!(run.id(), id);
            let history = run.control.activities();
            assert_eq!(history.total, 2);
            assert_ne!(history.entries[0].id, history.entries[1].id);
        }
        runtime.shutdown().await.unwrap();
    }
}

struct SwallowUncertain(ServiceClient<Probe>);
#[async_trait]
impl Activity for SwallowUncertain {
    type Output = ();
    fn name(&self) -> &'static str {
        "swallow-uncertain"
    }
    fn resources(&self) -> Vec<ResourceAccess> {
        vec![write("disk")]
    }
    fn retry(&self) -> Option<Retry> {
        Some(Retry::interval(Duration::ZERO))
    }
    async fn execute(&mut self, scope: &mut ActivityScope) -> Outcome<ActivityDecision<()>> {
        let _ = scope
            .call(
                &self.0,
                Work::write("uncertain", "disk").behavior(Behavior::Uncertain),
            )
            .await;
        Ok(ActivityDecision::Retry {
            reason: "must not retry unknown effects".into(),
        })
    }
}
#[tokio::test]
async fn named_step_cannot_swallow_an_uncertain_domain_result_and_retry() {
    let (mut runtime, client, _) = setup();
    let mut run = runtime
        .submit(StepFlow(SwallowUncertain(client.clone())))
        .await
        .unwrap();
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    assert_eq!(run.control.activities().entries[0].attempts, 1);
    assert_eq!(client.query(()).await.unwrap().1.len(), 1);
    let mut other = runtime
        .submit(Call::new(&client, Work::write("other", "disk")))
        .await
        .unwrap();
    assert!(matches!(
        finish(&mut other).await,
        Err(Failure::Rejected(_))
    ));
    runtime.shutdown().await.unwrap();
}

struct StopStep {
    events: mpsc::UnboundedSender<&'static str>,
    stopped: Arc<Semaphore>,
}
#[async_trait]
impl Activity for StopStep {
    type Output = ();
    fn name(&self) -> &'static str {
        "stop-step"
    }
    fn resources(&self) -> Vec<ResourceAccess> {
        vec![write("disk")]
    }
    async fn execute(&mut self, scope: &mut ActivityScope) -> Outcome<ActivityDecision<()>> {
        self.events.send("started").unwrap();
        scope.cancellation().cancelled().await;
        self.events.send("stop-requested").unwrap();
        self.stopped.acquire().await.unwrap().forget();
        Err(Failure::Cancelled)
    }
}
struct DropWaiter {
    step: StopStep,
    abandon: oneshot::Receiver<()>,
}
#[async_trait]
impl Workflow for DropWaiter {
    type Output = ();
    const NAME: &'static str = "drop-step-waiter";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        tokio::select! {
            result = ctx.run_activity(self.step) => { result?; },
            _ = self.abandon => {},
        }
        Ok(())
    }
}
#[tokio::test]
async fn dropping_the_waiter_keeps_the_step_alive_until_actual_settlement() {
    let (mut runtime, client, _) = setup();
    let (events, mut observed) = mpsc::unbounded_channel();
    let stopped = Arc::new(Semaphore::new(0));
    let (abandon, abandoned) = oneshot::channel();
    let mut run = runtime
        .submit(DropWaiter {
            step: StopStep {
                events,
                stopped: stopped.clone(),
            },
            abandon: abandoned,
        })
        .await
        .unwrap();
    assert_eq!(next(&mut observed).await, "started");
    abandon.send(()).unwrap();
    assert_eq!(next(&mut observed).await, "stop-requested");
    let mut other = runtime
        .submit(Call::new(&client, Work::write("other", "disk")))
        .await
        .unwrap();
    state(&other.control, RunState::WaitingResources).await;
    assert_eq!(
        run.control.activities().entries[0].state,
        ActivityState::Running
    );
    stopped.add_permits(1);
    finish(&mut run).await.unwrap();
    finish(&mut other).await.unwrap();
    assert_eq!(
        run.control.activities().entries[0].state,
        ActivityState::Failed(Failure::Cancelled)
    );
    runtime.shutdown().await.unwrap();
}
