#[allow(dead_code)]
mod support;
use async_trait::async_trait;
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;
use tokio::sync::{Semaphore, mpsc};
use workflow_foundation::*;

type Handler = fn(Outcome<usize>) -> Outcome<ActivityDecision<usize>>;
fn retry_any(_: Outcome<usize>) -> Outcome<ActivityDecision<usize>> {
    Ok(ActivityDecision::Retry {
        reason: "try again".into(),
    })
}
fn complete_any(_: Outcome<usize>) -> Outcome<ActivityDecision<usize>> {
    Ok(ActivityDecision::Complete(99))
}

struct Function<F> {
    function: F,
    retry: Option<Retry>,
    name: &'static str,
    handler: Option<Handler>,
}
#[async_trait]
impl<F, Fut> Workflow for Function<F>
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: Future<Output = Outcome<usize>> + Send + 'static,
{
    type Output = ActivityExit<usize, ()>;
    const NAME: &'static str = "function";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Self::Output> {
        let call = ctx
            .activity_fn(self.function)
            .name(self.name)
            .description("one attempt at a time");
        let call = match self.handler {
            Some(handler) => call.handle_result(handler),
            None => call,
        };
        match self.retry {
            Some(retry) => call.retry(retry).await,
            None => call.await,
        }
    }
}

#[tokio::test]
async fn result_handling_is_opt_in_and_terminal_errors_bypass_the_handler() {
    let cases = [
        (Failure::Busy, None, None::<Handler>, 1, None),
        // A budget alone does not reinterpret errors or cause retries.
        (
            Failure::Busy,
            Some(Retry::interval(Duration::ZERO)),
            None,
            1,
            None,
        ),
        (
            Failure::Busy,
            Some(Retry::interval(Duration::ZERO)),
            Some(retry_any),
            3,
            Some("retry limit exhausted"),
        ),
        (
            Failure::Failed("temporary".into()),
            Some(Retry::interval(Duration::ZERO).max_attempts(2)),
            Some(retry_any),
            2,
            Some("retry limit exhausted"),
        ),
        (
            Failure::Busy,
            None,
            Some(retry_any),
            1,
            Some("without a retry policy"),
        ),
        (
            Failure::Cancelled,
            Some(Retry::interval(Duration::ZERO)),
            Some(retry_any),
            1,
            None,
        ),
        (
            Failure::Cancelled,
            Some(Retry::interval(Duration::ZERO)),
            Some(complete_any),
            1,
            None,
        ),
        (
            Failure::Uncertain("unknown effects".into()),
            Some(Retry::interval(Duration::ZERO)),
            Some(retry_any),
            1,
            None,
        ),
        (
            Failure::Uncertain("unknown effects".into()),
            Some(Retry::interval(Duration::ZERO)),
            Some(complete_any),
            1,
            None,
        ),
    ];
    for (error, retry, handler, expected, message) in cases {
        let (mut runtime, _, _) = setup();
        let attempts = Arc::new(AtomicUsize::new(0));
        let count = attempts.clone();
        let failure = error.clone();
        let mut run = runtime
            .submit(Function {
                function: move || {
                    count.fetch_add(1, Ordering::SeqCst);
                    std::future::ready(Err(failure.clone()))
                },
                retry,
                name: "named-step",
                handler,
            })
            .await
            .unwrap();
        let result = finish(&mut run).await;
        if let Some(message) = message {
            assert!(
                matches!(result, Err(Failure::Failed(ref text)) if text.contains(message)),
                "{result:?}"
            );
        } else if matches!(error, Failure::Uncertain(_)) {
            assert!(matches!(result, Err(Failure::Uncertain(_))));
        } else {
            assert_eq!(result, Err(error));
        }
        assert_eq!(attempts.load(Ordering::SeqCst), expected);
        let history = run.control.activities();
        assert_eq!(history.total, 1);
        let record = &history.entries[0];
        assert_eq!(record.name, "named-step");
        assert_eq!(record.description.as_deref(), Some("one attempt at a time"));
        assert_eq!(record.attempts, expected as u32);
        assert!(record.retry_at.is_none());
        runtime.shutdown().await.unwrap();
    }
}

struct Simple;
async fn add(a: usize, b: usize) -> Outcome<usize> {
    Ok(a + b)
}
#[async_trait]
impl Workflow for Simple {
    type Output = ActivityExit<usize, ()>;
    const NAME: &'static str = "simple";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Self::Output> {
        // Building and discarding an Activity never invokes the function.
        drop(ctx.activity_fn(|| async {
            panic!("must remain lazy");
            #[allow(unreachable_code)]
            Ok(0usize)
        }));
        ctx.activity_fn(|| add(2, 3)).name("sum").await
    }
}
#[tokio::test]
async fn ordinary_async_function_needs_no_scope_or_async_move_wrapper() {
    let (mut runtime, _, _) = setup();
    let mut run = runtime.submit(Simple).await.unwrap();
    assert_eq!(finish(&mut run).await.unwrap(), ActivityExit::Completed(5));
    assert_eq!(run.control.activities().total, 1);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_options_are_rejected_before_function_execution() {
    for (name, attempts) in [("", 3), ("valid", 0)] {
        let (mut runtime, _, _) = setup();
        let mut run = runtime
            .submit(Function {
                function: || async {
                    panic!("invalid configuration must not execute");
                    #[allow(unreachable_code)]
                    Ok(0usize)
                },
                retry: Some(Retry::interval(Duration::ZERO).max_attempts(attempts)),
                name,
                handler: None,
            })
            .await
            .unwrap();
        assert!(matches!(finish(&mut run).await, Err(Failure::Rejected(_))));
        assert_eq!(run.control.activities().total, 0);
        runtime.shutdown().await.unwrap();
    }
}

async fn probe_ready(mut scope: ActivityScope, service: ServiceClient<Probe>) -> Outcome<usize> {
    scope.call(&service, Work::write("attempt", "disk")).await
}
struct WaitForReady {
    service: ServiceClient<Probe>,
    target: usize,
    delay: Duration,
}
#[async_trait]
impl Workflow for WaitForReady {
    type Output = usize;
    const NAME: &'static str = "wait-for-ready";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<usize> {
        loop {
            let service = self.service.clone();
            match ctx
                .activity_fn(move |scope| probe_ready(scope, service.clone()))
                .resources(vec![write("disk")])
                .name("wait-for-ready")
                .handle_result(move |result| {
                    let reply = result?;
                    if reply < self.target {
                        Ok(ActivityDecision::Retry {
                            reason: format!("value {reply} is below {}", self.target),
                        })
                    } else {
                        Ok(ActivityDecision::Complete(reply))
                    }
                })
                .retry(Retry::interval(self.delay).max_attempts(3))
                .await?
            {
                ActivityExit::Completed(value) => return Ok(value),
                ActivityExit::Suspended(()) => ctx.wait_until_runnable().await?,
            }
        }
    }
}
async fn backoff(runtime: &WorkflowRuntime, id: TaskId) -> ActivitySnapshot {
    let mut observer = runtime.client().observe_task(id).unwrap();
    tokio::time::timeout(LIMIT, async {
        loop {
            if let Some(step) = observer.snapshot().activities.entries.back()
                && step.retry_at.is_some()
            {
                return step.clone();
            }
            observer.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn function_interprets_service_replies_and_retries_under_one_activity_id() {
    let (mut runtime, service, _) = setup();
    let mut run = runtime
        .submit(WaitForReady {
            service: service.clone(),
            target: 3,
            delay: Duration::from_millis(1),
        })
        .await
        .unwrap();
    assert_eq!(finish(&mut run).await.unwrap(), 3);
    let history = run.control.activities();
    assert_eq!(history.total, 1);
    let record = &history.entries[0];
    assert_eq!(record.attempts, 3);
    assert_eq!(record.last_error, None);
    assert!(record.retry_reason.as_ref().unwrap().contains("below"));
    assert_eq!(service.query(()).await.unwrap().1, vec![record.id; 3]);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn pause_and_cancel_interrupt_backoff_but_only_resume_starts_another_invocation() {
    for cancel in [false, true] {
        let (mut runtime, service, _) = setup();
        let mut run = runtime
            .submit(WaitForReady {
                service: service.clone(),
                target: 2,
                delay: Duration::from_secs(60),
            })
            .await
            .unwrap();
        let id = run.id();
        let record = backoff(&runtime, id).await;
        assert_eq!(record.attempts, 1);
        assert_eq!(record.last_error, None);
        assert!(record.retry_reason.as_ref().unwrap().contains("below"));
        // The lease covers backoff too; only settled suspension releases it.
        let mut other = runtime
            .submit(Call::new(&service, Work::write("other", "disk")))
            .await
            .unwrap();
        state(&other.control, RunState::WaitingResources).await;
        if cancel {
            run.control.cancel();
            assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
            assert!(run.control.resume().is_err());
            assert_eq!(run.control.activities().total, 1);
        } else {
            run.control.suspend().unwrap();
            tokio::time::timeout(LIMIT, run.control.wait_suspended())
                .await
                .unwrap()
                .unwrap();
            let ended = run.control.activities().entries[0].clone();
            assert_eq!(ended.state, ActivityState::Completed);
            assert!(ended.retry_at.is_none());
        }
        finish(&mut other).await.unwrap();
        if !cancel {
            run.control.resume().unwrap();
            assert_eq!(finish(&mut run).await.unwrap(), 3);
            assert_eq!(run.id(), id);
            let history = run.control.activities();
            assert_eq!(history.total, 2);
            assert_ne!(history.entries[0].id, history.entries[1].id);
        }
        runtime.shutdown().await.unwrap();
    }
}

struct InFlight {
    events: mpsc::UnboundedSender<&'static str>,
    settled: Arc<Semaphore>,
}
async fn stop_then_return(
    control: ControlToken,
    events: mpsc::UnboundedSender<&'static str>,
    settled: Arc<Semaphore>,
) -> Outcome<usize> {
    events.send("started").unwrap();
    control.cancelled().await;
    events.send("stop-requested").unwrap();
    settled.acquire().await.unwrap().forget();
    Err(Failure::Cancelled)
}
#[async_trait]
impl Workflow for InFlight {
    type Output = ActivityExit<usize, ()>;
    const NAME: &'static str = "in-flight";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Self::Output> {
        let control = ctx.control();
        ctx.activity_fn(move || {
            stop_then_return(control.clone(), self.events.clone(), self.settled.clone())
        })
        .resources(vec![write("disk")])
        .handle_result(retry_any)
        .retry(Retry::interval(Duration::ZERO))
        .await
    }
}
#[tokio::test]
async fn cancelling_a_function_waits_for_actual_settlement_and_never_retries_it() {
    let (mut runtime, service, _) = setup();
    let (events, mut observed) = mpsc::unbounded_channel();
    let settled = Arc::new(Semaphore::new(0));
    let mut run = runtime
        .submit(InFlight {
            events,
            settled: settled.clone(),
        })
        .await
        .unwrap();
    assert_eq!(next(&mut observed).await, "started");
    run.control.cancel();
    assert_eq!(next(&mut observed).await, "stop-requested");
    let mut other = runtime
        .submit(Call::new(&service, Work::write("other", "disk")))
        .await
        .unwrap();
    state(&other.control, RunState::WaitingResources).await;
    assert_eq!(
        run.control.activities().entries[0].state,
        ActivityState::Running
    );
    settled.add_permits(1);
    assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
    assert_eq!(run.control.activities().entries[0].attempts, 1);
    finish(&mut other).await.unwrap();
    assert!(observed.try_recv().is_err());
    runtime.shutdown().await.unwrap();
}

struct Swallow(ServiceClient<Probe>);
async fn swallow_uncertain(
    mut scope: ActivityScope,
    service: ServiceClient<Probe>,
) -> Outcome<usize> {
    let _ = scope
        .call(
            &service,
            Work::write("uncertain", "disk").behavior(Behavior::Uncertain),
        )
        .await;
    Err(Failure::Busy)
}
#[async_trait]
impl Workflow for Swallow {
    type Output = ActivityExit<usize, ()>;
    const NAME: &'static str = "swallow";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Self::Output> {
        ctx.activity_fn(move |scope| swallow_uncertain(scope, self.0.clone()))
            .resources(vec![write("disk")])
            .handle_result(retry_any)
            .retry(Retry::interval(Duration::ZERO))
            .await
    }
}
#[tokio::test]
async fn uncertain_service_result_cannot_be_reclassified_as_retryable_by_the_function() {
    let (mut runtime, service, mut events) = setup();
    let mut run = runtime.submit(Swallow(service.clone())).await.unwrap();
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    assert_eq!(next(&mut events).await, "uncertain");
    assert!(events.try_recv().is_err());
    assert_eq!(service.query(()).await.unwrap().1.len(), 1);
    assert_eq!(run.control.activities().entries[0].attempts, 1);
    runtime.shutdown().await.unwrap();
}

struct NamedCall(ServiceClient<Probe>);
#[async_trait]
impl Workflow for NamedCall {
    type Output = usize;
    const NAME: &'static str = "named-call";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<usize> {
        ctx.activity(&self.0, Work::write("domain-operation", "disk"))
            .name("检查磁盘")
            .description("工作流中的步骤说明")
            .await
    }
}
#[tokio::test]
async fn direct_service_calls_share_the_metadata_builder_without_changing_dispatch() {
    let (mut runtime, service, _) = setup();
    let mut run = runtime.submit(NamedCall(service.clone())).await.unwrap();
    assert_eq!(finish(&mut run).await.unwrap(), 1);
    let history = run.control.activities();
    let record = &history.entries[0];
    assert_eq!(record.name, "检查磁盘");
    assert_eq!(record.description.as_deref(), Some("工作流中的步骤说明"));
    assert_eq!(record.service, "probe");
    assert_eq!(record.resources, vec![write("disk")]);
    assert_eq!(record.attempts, 1);
    assert_eq!(service.query(()).await.unwrap().1, vec![record.id]);
    runtime.shutdown().await.unwrap();
}

// The reply deliberately has a different type from the Activity's final output.
struct CheckReply {
    attempt: usize,
    remaining: usize,
    inflight: usize,
    disk: String,
}
struct InterpretReply {
    budget: Option<u32>,
    reject: bool,
}
#[async_trait]
impl Workflow for InterpretReply {
    type Output = ActivityExit<String, ()>;
    const NAME: &'static str = "interpret-reply";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Self::Output> {
        let mut attempt = 0;
        let call = ctx
            .activity_fn(move || {
                attempt += 1;
                std::future::ready(Ok(CheckReply {
                    attempt,
                    remaining: usize::from(attempt == 1),
                    inflight: usize::from(attempt < 3),
                    disk: "disk-7".to_owned(),
                }))
            })
            .name("interpret");
        let call = match self.budget {
            Some(limit) => call.retry(Retry::interval(Duration::ZERO).max_attempts(limit)),
            None => call,
        };
        let mut handled = 0;
        call.handle_result(move |result| {
            let reply = result?;
            handled += 1;
            assert_eq!(handled, reply.attempt);
            if self.reject {
                return Err(Failure::Failed("disk cannot be removed".into()));
            }
            if reply.remaining > 0 || reply.inflight > 0 {
                Ok(ActivityDecision::Retry {
                    reason: format!("remaining={}, inflight={}", reply.remaining, reply.inflight),
                })
            } else {
                // Move owned data out of the reply; no Clone bound is required.
                Ok(ActivityDecision::Complete(reply.disk))
            }
        })
        .await
    }
}
#[tokio::test]
async fn handler_interprets_complete_owned_replies_and_changes_the_output_type() {
    let (mut runtime, _, _) = setup();
    let mut run = runtime
        .submit(InterpretReply {
            budget: Some(3),
            reject: false,
        })
        .await
        .unwrap();
    assert_eq!(
        finish(&mut run).await.unwrap(),
        ActivityExit::Completed("disk-7".into())
    );
    let history = run.control.activities();
    assert_eq!(history.total, 1);
    let step = &history.entries[0];
    assert_eq!(step.attempts, 3);
    assert_eq!(step.last_error, None);
    assert_eq!(
        step.retry_reason.as_deref(),
        Some("remaining=0, inflight=1")
    );
    assert_eq!(step.retry_at, None);
    runtime.shutdown().await.unwrap();
}
#[tokio::test]
async fn pending_or_rejected_business_replies_never_become_false_success() {
    for (budget, reject, attempts, message) in [
        (Some(2), false, 2, "retry limit exhausted after 2 attempts"),
        (None, false, 1, "without a retry policy"),
        (Some(3), true, 1, "disk cannot be removed"),
    ] {
        let (mut runtime, _, _) = setup();
        let mut run = runtime
            .submit(InterpretReply { budget, reject })
            .await
            .unwrap();
        let result = finish(&mut run).await;
        assert!(
            matches!(result, Err(Failure::Failed(ref text)) if text.contains(message)),
            "{result:?}"
        );
        let history = run.control.activities();
        let step = &history.entries[0];
        assert_eq!(step.attempts, attempts);
        assert!(matches!(step.state, ActivityState::Failed(_)));
        assert_eq!(step.retry_at, None);
        if !reject {
            assert!(step.retry_reason.as_ref().unwrap().contains("inflight=1"));
        }
        runtime.shutdown().await.unwrap();
    }
}
#[tokio::test]
async fn handler_can_supply_a_fallback_for_an_ordinary_failure() {
    let (mut runtime, _, _) = setup();
    let mut run = runtime
        .submit(Function {
            function: || async { Err(Failure::Failed("optional lookup failed".into())) },
            retry: None,
            name: "fallback",
            handler: Some(complete_any),
        })
        .await
        .unwrap();
    assert_eq!(finish(&mut run).await.unwrap(), ActivityExit::Completed(99));
    assert_eq!(run.control.activities().entries[0].attempts, 1);
    runtime.shutdown().await.unwrap();
}

struct HandlerPanic;
#[async_trait]
impl Workflow for HandlerPanic {
    type Output = ActivityExit<(), ()>;
    const NAME: &'static str = "handler-panic";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<Self::Output> {
        ctx.activity_fn(|| async { Ok(()) })
            .resources(vec![write("disk")])
            .handle_result(|_| -> Outcome<ActivityDecision<()>> { panic!("handler panicked") })
            .retry(Retry::interval(Duration::ZERO))
            .await
    }
}
#[tokio::test]
async fn panicking_handler_is_not_retried_and_cannot_release_affected_resources() {
    let (mut runtime, service, _) = setup();
    let mut run = runtime.submit(HandlerPanic).await.unwrap();
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    assert_eq!(run.control.activities().entries[0].attempts, 1);
    let mut other = runtime
        .submit(Call::new(&service, Work::write("other", "disk")))
        .await
        .unwrap();
    assert!(matches!(
        finish(&mut other).await,
        Err(Failure::Rejected(_))
    ));
    runtime.shutdown().await.unwrap();
}
