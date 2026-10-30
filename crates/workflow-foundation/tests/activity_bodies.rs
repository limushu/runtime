#[allow(dead_code)]
mod support;
use async_trait::async_trait;
use std::{future::Future, sync::Arc, time::Duration};
use support::*;
use tokio::sync::{Semaphore, mpsc, oneshot};
use workflow_foundation::*;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, future)
        .await
        .expect("body stalled")
}
struct Body<F> {
    resources: Vec<ResourceAccess>,
    body: F,
    protected: bool,
}
impl<F> Body<F> {
    fn new(resources: Vec<ResourceAccess>, body: F) -> Self {
        Self {
            resources,
            body,
            protected: false,
        }
    }
}
#[async_trait]
impl<T, F, Fut> Workflow for Body<F>
where
    T: Send + 'static,
    F: FnOnce(ActivityScope) -> Fut + Send + 'static,
    Fut: Future<Output = Outcome<T>> + Send + 'static,
{
    type Output = T;
    const NAME: &'static str = "activity-body";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<T> {
        let call = ctx.activity_body("retry-domain", self.resources, self.body);
        if self.protected {
            call.uninterruptible().await
        } else {
            call.await
        }
    }
}

#[tokio::test]
async fn retries_inspect_replies_under_one_activity_id_and_keep_the_resource_lease() {
    let (mut runtime, service, mut events) = setup();
    let client = service.clone();
    let backoff = Arc::new(Semaphore::new(0));
    let release = backoff.clone();
    let (waiting, wait) = oneshot::channel();
    let mut run = runtime
        .submit(Body::new(
            vec![write("disk")],
            move |mut activity: ActivityScope| async move {
                assert_eq!(
                    activity
                        .call(&client, Work::write("attempt", "disk"))
                        .await?,
                    1
                );
                waiting.send(()).unwrap();
                release.acquire().await.unwrap().forget();
                loop {
                    let count = activity
                        .call(&client, Work::write("attempt", "disk"))
                        .await?;
                    activity.report_progress("attempts", count as u64, Some(3));
                    if count == 3 {
                        return Ok(count);
                    }
                }
            },
        ))
        .await
        .unwrap();
    bounded(wait).await.unwrap();
    next(&mut events).await;
    let mut other = runtime
        .submit(Call::new(&service, Work::write("other", "disk")))
        .await
        .unwrap();
    state(&other.control, RunState::WaitingResources).await;
    assert!(events.try_recv().is_err());
    backoff.add_permits(1);
    assert_eq!(finish(&mut run).await.unwrap(), 3);
    finish(&mut other).await.unwrap();
    let snapshot = runtime.client().task(run.id()).unwrap();
    assert_eq!(snapshot.activities.total, 1);
    let record = &snapshot.activities.entries[0];
    assert_eq!(record.name, "retry-domain");
    assert_eq!(record.state, ActivityState::Completed);
    assert_eq!(record.resources, vec![write("disk")]);
    assert_eq!(snapshot.progress.completed, 3);
    let (_, calls) = service.query(()).await.unwrap();
    assert_eq!(&calls[..3], &[record.id; 3]);
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn one_body_can_combine_services_and_write_access_covers_read_calls() {
    let (mut runtime, first, _) = setup();
    let (tx, _) = mpsc::unbounded_channel();
    let second = runtime.register("second", Probe::new(tx)).unwrap();
    let first_view = first.clone();
    let second_view = second.clone();
    let mut run = runtime
        .submit(Body::new(
            vec![write("a"), write("b")],
            move |mut activity: ActivityScope| async move {
                let one = activity.call(&first, Work::write("write", "a")).await?;
                let mut read = Work::write("read", "b");
                read.access = vec![support::read("b")];
                let two = activity.call(&second, read).await?;
                Ok(one + two)
            },
        ))
        .await
        .unwrap();
    assert_eq!(finish(&mut run).await.unwrap(), 2);
    assert_eq!(run.control.activities().total, 1);
    let one = first_view.query(()).await.unwrap().1;
    let two = second_view.query(()).await.unwrap().1;
    assert_eq!(one, two);
    runtime.shutdown().await.unwrap();
}

struct RetryFlow {
    service: ServiceClient<Probe>,
    waiting: mpsc::UnboundedSender<usize>,
    saved: mpsc::UnboundedSender<usize>,
    durable: bool,
}
#[async_trait]
impl Workflow for RetryFlow {
    type Output = usize;
    const NAME: &'static str = "retry-flow";
    fn persistence(&self) -> Outcome<Persistence> {
        if self.durable {
            Persistence::durable(1, &())
        } else {
            Ok(Persistence::Memory)
        }
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<usize> {
        let mut checkpoint = 0;
        loop {
            let client = self.service.clone();
            let waiting = self.waiting.clone();
            let exit = ctx
                .activity_body(
                    "retry-until-ready",
                    vec![write("disk")],
                    move |mut activity| async move {
                        loop {
                            if activity.check_control()? == ActivityExit::Suspended(()) {
                                return Ok(ActivityExit::Suspended(checkpoint));
                            }
                            let value = activity
                                .call(&client, Work::write("attempt", "disk"))
                                .await?;
                            checkpoint += 1;
                            if checkpoint == 2 {
                                return Ok(ActivityExit::Completed(value));
                            }
                            waiting.send(checkpoint).unwrap();
                            if activity.sleep(Duration::from_secs(60)).await?
                                == ActivityExit::Suspended(())
                            {
                                return Ok(ActivityExit::Suspended(checkpoint));
                            }
                        }
                    },
                )
                .await?;
            match exit {
                ActivityExit::Completed(value) => return Ok(value),
                ActivityExit::Suspended(saved) => {
                    checkpoint = saved;
                    if self.durable {
                        ctx.save_checkpoint(&checkpoint).await?;
                    }
                    self.saved.send(checkpoint).unwrap();
                    ctx.wait_until_runnable().await?;
                }
            }
        }
    }
}

#[tokio::test]
async fn pause_interrupts_backoff_and_saves_checkpoint_before_ack_then_resumes_same_task() {
    use workflow_foundation::persistence::{MemoryTaskStore, TaskStore};
    let store = Arc::new(MemoryTaskStore::default());
    let mut driver = WorkflowDriver::new("p", Config::default())
        .unwrap()
        .with_task_ids(42, TaskIds::for_leader(1).unwrap())
        .unwrap();
    driver.enable_persistence(store.clone()).await.unwrap();
    driver.restore().await.unwrap();
    let (events, _events) = mpsc::unbounded_channel();
    let service = driver.register("probe", Probe::new(events)).unwrap();
    let (waiting, mut waits) = mpsc::unbounded_channel();
    let (saved, mut saves) = mpsc::unbounded_channel();
    let mut run = driver
        .submit(RetryFlow {
            service,
            waiting,
            saved,
            durable: true,
        })
        .unwrap();
    let id = run.id();
    let execution = tokio::spawn(async move {
        while !driver.is_idle() {
            driver.tick().await;
        }
        driver.shutdown().await.unwrap();
    });
    assert_eq!(bounded(waits.recv()).await.unwrap(), 1);
    run.control.suspend().unwrap();
    bounded(run.control.wait_suspended()).await.unwrap();
    assert_eq!(saves.try_recv().unwrap(), 1);
    assert_eq!(store.list(42).unwrap()[0].checkpoint, serde_json::json!(1));
    assert_eq!(
        run.control.activities().entries[0].state,
        ActivityState::Completed
    );
    run.control.resume().unwrap();
    assert_eq!(finish(&mut run).await.unwrap(), 2);
    assert_eq!(run.id(), id);
    let history = run.control.activities();
    assert_eq!(history.total, 2);
    assert_ne!(history.entries[0].id, history.entries[1].id);
    bounded(execution).await.unwrap();
}

#[tokio::test]
async fn cancel_interrupts_backoff_without_retry_or_resumption() {
    let (mut runtime, service, _) = setup();
    let (waiting, mut waits) = mpsc::unbounded_channel();
    let (saved, mut saves) = mpsc::unbounded_channel();
    let mut run = runtime
        .submit(RetryFlow {
            service: service.clone(),
            waiting,
            saved,
            durable: false,
        })
        .await
        .unwrap();
    bounded(waits.recv()).await.unwrap();
    run.control.cancel();
    assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
    assert!(saves.try_recv().is_err());
    assert!(run.control.resume().is_err());
    assert_eq!(service.query(()).await.unwrap().1.len(), 1);
    runtime.shutdown().await.unwrap();
}

struct StopWorker(mpsc::UnboundedSender<(&'static str, oneshot::Sender<()>)>);
#[async_trait]
impl ServiceHandler for StopWorker {
    type Request = ();
    type Reply = ActivityExit<(), u64>;
    type Query = ();
    type View = ();
    fn resources(_: &()) -> Vec<ResourceAccess> {
        vec![write("disk")]
    }
    async fn execute(&self, _: (), ctx: ActivityContext) -> Outcome<Self::Reply> {
        let (started, wait) = oneshot::channel();
        self.0.send(("started", started)).unwrap();
        wait.await.unwrap();
        let mut control = ctx.control();
        loop {
            let cancelled = control.is_cancelled();
            if cancelled || control.request().paused() {
                let (stopped, stop) = oneshot::channel();
                self.0
                    .send((if cancelled { "cancel" } else { "suspend" }, stopped))
                    .unwrap();
                stop.await.unwrap();
                return if control.is_cancelled() {
                    Err(Failure::Cancelled)
                } else {
                    Ok(ActivityExit::Suspended(7))
                };
            }
            let _ = control.changed().await;
        }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
struct StopFlow(ServiceClient<StopWorker>);
#[async_trait]
impl Workflow for StopFlow {
    type Output = ();
    const NAME: &'static str = "stop-body";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let resources = self.0.resources(&());
        let reply = ctx
            .activity_body("stop", resources, move |mut activity| async move {
                activity.call(&self.0, ()).await
            })
            .await?;
        assert_eq!(reply, ActivityExit::Suspended(7));
        ctx.report_details(&7)?;
        ctx.wait_until_runnable().await
    }
}
#[tokio::test]
async fn control_reaches_domain_and_waits_for_real_stop_before_ack_or_resource_release() {
    for cancel in [false, true] {
        let (mut runtime, probe, _) = setup();
        let (tx, mut events) = mpsc::unbounded_channel();
        let service = runtime.register("stop", StopWorker(tx)).unwrap();
        let mut run = runtime.submit(StopFlow(service)).await.unwrap();
        let (name, start) = bounded(events.recv()).await.unwrap();
        assert_eq!(name, "started");
        if cancel {
            run.control.cancel();
        } else {
            run.control.suspend().unwrap();
        }
        start.send(()).unwrap();
        let (name, stop) = bounded(events.recv()).await.unwrap();
        assert_eq!(name, if cancel { "cancel" } else { "suspend" });
        assert!(!run.control.suspend_acknowledged());
        assert_eq!(
            run.control.activities().entries[0].state,
            ActivityState::Running
        );
        let mut other = runtime
            .submit(Call::new(&probe, Work::write("other", "disk")))
            .await
            .unwrap();
        state(&other.control, RunState::WaitingResources).await;
        stop.send(()).unwrap();
        if cancel {
            assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
        } else {
            bounded(run.control.wait_suspended()).await.unwrap();
            assert_eq!(
                runtime.client().task(run.id()).unwrap().details,
                serde_json::json!(7)
            );
            run.control.resume().unwrap();
            finish(&mut run).await.unwrap();
        }
        finish(&mut other).await.unwrap();
        runtime.shutdown().await.unwrap();
    }
}

struct Abandon {
    service: ServiceClient<Probe>,
    gate: Arc<Semaphore>,
    abandon: oneshot::Receiver<()>,
    nested: bool,
}
#[async_trait]
impl Workflow for Abandon {
    type Output = ();
    const NAME: &'static str = "abandon-body";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let Self {
            service,
            gate,
            abandon,
            nested,
        } = self;
        if nested {
            ctx.activity_body("abandon-call", vec![write("disk")], move |mut activity| async move {
                tokio::select! {
                    r = activity.call(&service, Work::write("started", "disk").gated(&gate).behavior(Behavior::CancelThenSettle)) => { r?; },
                    _ = abandon => {},
                }
                Ok(())
            }).await
        } else {
            tokio::select! {
                r = ctx.activity_body("abandon-body", vec![write("disk")], move |mut activity| async move {
                    activity.call(&service, Work::write("started", "disk").gated(&gate).behavior(Behavior::CancelThenSettle)).await
                }) => { r?; },
                _ = abandon => {},
            }
            Ok(())
        }
    }
}
#[tokio::test]
async fn dropped_waiter_keeps_body_tracked_and_dropped_nested_call_cannot_publish_false_success() {
    for nested in [false, true] {
        let (mut runtime, service, mut events) = setup();
        let gate = Arc::new(Semaphore::new(0));
        let (abandon, abandoned) = oneshot::channel();
        let mut run = runtime
            .submit(Abandon {
                service: service.clone(),
                gate: gate.clone(),
                abandon: abandoned,
                nested,
            })
            .await
            .unwrap();
        assert_eq!(next(&mut events).await, "started");
        abandon.send(()).unwrap();
        assert_eq!(next(&mut events).await, "cancel-observed");
        let mut other = runtime
            .submit(Call::new(&service, Work::write("other", "disk")))
            .await
            .unwrap();
        state(&other.control, RunState::WaitingResources).await;
        assert_eq!(
            run.control.activities().entries[0].state,
            ActivityState::Running
        );
        gate.add_permits(1);
        if nested {
            assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
            assert!(matches!(
                finish(&mut other).await,
                Err(Failure::Rejected(_))
            ));
        } else {
            // The Workflow intentionally stopped waiting; actual Activity cancellation
            // is still recorded and settled before the Workflow can finish.
            finish(&mut run).await.unwrap();
            assert_eq!(
                run.control.activities().entries[0].state,
                ActivityState::Failed(Failure::Cancelled)
            );
            finish(&mut other).await.unwrap();
        }
        runtime.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn uncertain_reply_cannot_be_swallowed_to_retry_or_release_resources() {
    let (mut runtime, service, mut events) = setup();
    let client = service.clone();
    let mut run = runtime
        .submit(Body::new(
            vec![write("disk")],
            move |mut activity: ActivityScope| async move {
                assert!(matches!(
                    activity
                        .call(
                            &client,
                            Work::write("uncertain", "disk").behavior(Behavior::Uncertain)
                        )
                        .await,
                    Err(Failure::Uncertain(_))
                ));
                assert!(matches!(
                    activity.call(&client, Work::write("retry", "disk")).await,
                    Err(Failure::Uncertain(_))
                ));
                Ok(())
            },
        ))
        .await
        .unwrap();
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    assert_eq!(next(&mut events).await, "uncertain");
    assert!(events.try_recv().is_err());
    let mut later = runtime
        .submit(Call::new(&service, Work::write("later", "disk")))
        .await
        .unwrap();
    assert!(matches!(
        finish(&mut later).await,
        Err(Failure::Rejected(_))
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn body_enforces_declared_access_and_pool_scope_before_dispatch() {
    let (mut runtime, service, mut events) = setup();
    let (mut other, foreign, mut foreign_events) = setup();
    for (client, resources) in [
        (service.clone(), vec![]),
        (service, vec![read("disk")]),
        (foreign, vec![write("disk")]),
    ] {
        let mut run = runtime
            .submit(Body::new(
                resources,
                move |mut activity: ActivityScope| async move {
                    activity.call(&client, Work::write("invalid", "disk")).await
                },
            ))
            .await
            .unwrap();
        assert!(matches!(finish(&mut run).await, Err(Failure::Rejected(_))));
    }
    assert!(events.try_recv().is_err());
    assert!(foreign_events.try_recv().is_err());
    runtime.shutdown().await.unwrap();
    other.shutdown().await.unwrap();
}

#[tokio::test]
async fn panic_in_body_fences_resources_and_is_observable() {
    let (mut runtime, service, _) = setup();
    let mut run = runtime
        .submit(Body::new(vec![write("disk")], |_: ActivityScope| async {
            panic!("body panic");
            #[allow(unreachable_code)]
            Ok(())
        }))
        .await
        .unwrap();
    assert!(matches!(finish(&mut run).await, Err(Failure::Uncertain(_))));
    assert!(matches!(
        run.control.activities().entries[0].state,
        ActivityState::Failed(Failure::Uncertain(_))
    ));
    let mut later = runtime
        .submit(Call::new(&service, Work::write("later", "disk")))
        .await
        .unwrap();
    assert!(matches!(
        finish(&mut later).await,
        Err(Failure::Rejected(_))
    ));
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn protected_body_defers_task_controls_but_observes_host_shutdown() {
    for shutdown in [false, true] {
        let (mut runtime, _, _) = setup();
        let (started, start) = oneshot::channel();
        let (checked, check) = oneshot::channel();
        let (acknowledge, acknowledged) = oneshot::channel();
        let (finish_body, finished) = oneshot::channel();
        let mut work = Body::new(vec![], move |mut activity: ActivityScope| async move {
            started.send(()).unwrap();
            check.await.unwrap();
            assert!(!activity.control().is_cancelled());
            assert!(!activity.control().request().paused());
            acknowledge.send(()).unwrap();
            if shutdown {
                activity.sleep(Duration::from_secs(60)).await?;
            } else {
                finished.await.unwrap();
            }
            Ok(())
        });
        work.protected = true;
        let mut run = runtime.submit(work).await.unwrap();
        bounded(start).await.unwrap();
        run.control.suspend().unwrap();
        run.control.cancel();
        checked.send(()).unwrap();
        bounded(acknowledged).await.unwrap();
        if shutdown {
            bounded(runtime.shutdown()).await.unwrap();
            assert_eq!(finish(&mut run).await, Err(Failure::Cancelled));
        } else {
            finish_body.send(()).unwrap();
            finish(&mut run).await.unwrap();
            runtime.shutdown().await.unwrap();
        }
    }
}

#[tokio::test]
async fn escaped_body_context_cannot_dispatch_after_its_resource_lease_ends() {
    let (mut runtime, service, mut events) = setup();
    let mut run = runtime
        .submit(Body::new(vec![write("disk")], |activity| async move {
            Ok(activity)
        }))
        .await
        .unwrap();
    let mut escaped: ActivityScope = finish(&mut run).await.unwrap();
    assert!(matches!(
        escaped.call(&service, Work::write("escaped", "disk")).await,
        Err(Failure::Rejected(_))
    ));
    assert!(events.try_recv().is_err());
    runtime.shutdown().await.unwrap();
}

struct DropProtected {
    started: oneshot::Sender<()>,
    abandon: oneshot::Receiver<()>,
    continue_body: oneshot::Receiver<()>,
}
#[async_trait]
impl Workflow for DropProtected {
    type Output = ();
    const NAME: &'static str = "drop-protected-body";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        tokio::select! {
            result = ctx.activity_body("protected", vec![], move |mut activity| async move {
                self.started.send(()).unwrap();
                self.continue_body.await.unwrap();
                assert_eq!(activity.sleep(Duration::from_millis(1)).await?, ActivityExit::Completed(()));
                Ok(())
            }).uninterruptible() => { result?; },
            _ = self.abandon => {},
        }
        Ok(())
    }
}
#[tokio::test]
async fn protected_body_keeps_its_control_channel_alive_after_waiter_is_dropped() {
    let (mut runtime, _, _) = setup();
    let (started, start) = oneshot::channel();
    let (abandon, abandoned) = oneshot::channel();
    let (resume, continue_body) = oneshot::channel();
    let mut run = runtime
        .submit(DropProtected {
            started,
            abandon: abandoned,
            continue_body,
        })
        .await
        .unwrap();
    bounded(start).await.unwrap();
    abandon.send(()).unwrap();
    state(&run.control, RunState::Settling).await;
    resume.send(()).unwrap();
    finish(&mut run).await.unwrap();
    assert_eq!(
        run.control.activities().entries[0].state,
        ActivityState::Completed
    );
    runtime.shutdown().await.unwrap();
}
