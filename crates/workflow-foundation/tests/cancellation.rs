//! Cancellation is cooperative even when the executor keeps polling an Activity.
use async_trait::async_trait;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, mpsc};
use workflow_foundation::*;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .unwrap()
}
enum Work {
    Ignore {
        advance: mpsc::Receiver<()>,
        progress: mpsc::UnboundedSender<&'static str>,
    },
    Observe {
        drain: Arc<Semaphore>,
        progress: mpsc::UnboundedSender<&'static str>,
    },
}
struct Domain;
#[async_trait]
impl ServiceHandler for Domain {
    type Request = Work;
    type Reply = ();
    type Query = ();
    type View = ();
    fn resources(_: &Work) -> Vec<ResourceAccess> {
        vec![ResourceAccess::write(ObjectKey::new("disk", "d1"))]
    }
    async fn execute(&self, request: Work, context: ActivityContext) -> Outcome<()> {
        match request {
            Work::Ignore {
                mut advance,
                progress,
            } => {
                progress.send("started").unwrap();
                advance.recv().await.unwrap();
                // This future was polled after cancellation, but never observes the token.
                progress.send("still-running").unwrap();
                advance.recv().await.unwrap();
                progress.send("committed").unwrap();
                Ok(())
            }
            Work::Observe { drain, progress } => {
                progress.send("started").unwrap();
                context.control().cancelled().await;
                progress.send("cancel-observed").unwrap();
                drain.acquire().await.unwrap().forget();
                progress.send("settled").unwrap();
                Err(Failure::Cancelled)
            }
        }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
struct Call {
    service: ServiceClient<Domain>,
    work: Work,
}
#[async_trait]
impl Workflow for Call {
    type Output = ();
    const NAME: &'static str = "test.cancellation-contract";
    async fn run(self, context: &mut WorkflowContext) -> Outcome<()> {
        context.activity(&self.service, self.work).await
    }
}

#[tokio::test]
async fn polling_after_cancel_does_not_preempt_a_domain_future_that_ignores_the_token() {
    let mut runtime = WorkflowRuntime::start("pool", Config::default()).unwrap();
    let service = runtime.register("domain", Domain).unwrap();
    let (tx, advance) = mpsc::channel(2);
    let (progress, mut rx) = mpsc::unbounded_channel();
    let mut run = runtime
        .submit(Call {
            service,
            work: Work::Ignore { advance, progress },
        })
        .await
        .unwrap();
    assert_eq!(bounded(rx.recv()).await, Some("started"));
    run.control.cancel();
    tx.send(()).await.unwrap();
    assert_eq!(bounded(rx.recv()).await, Some("still-running"));
    assert_eq!(
        run.control.activities().entries[0].state,
        ActivityState::Running
    );
    assert!(run.control.activities().entries[0].finished_at.is_none());
    assert!(futures_util::poll!(Box::pin(run.result())).is_pending());
    tx.send(()).await.unwrap();
    assert_eq!(bounded(rx.recv()).await, Some("committed"));
    // Real irreversible completion is retained, not relabelled as an undone cancellation.
    bounded(run.result()).await.unwrap();
    runtime.shutdown().await.unwrap();
}

#[tokio::test]
async fn observing_cancel_is_not_settlement_and_does_not_release_activity_resources() {
    let mut runtime = WorkflowRuntime::start("pool", Config::default()).unwrap();
    let service = runtime.register("domain", Domain).unwrap();
    let drain = Arc::new(Semaphore::new(0));
    let (progress, mut rx) = mpsc::unbounded_channel();
    let mut run = runtime
        .submit(Call {
            service: service.clone(),
            work: Work::Observe {
                drain: drain.clone(),
                progress,
            },
        })
        .await
        .unwrap();
    assert_eq!(bounded(rx.recv()).await, Some("started"));
    run.control.cancel();
    assert_eq!(bounded(rx.recv()).await, Some("cancel-observed"));
    assert!(futures_util::poll!(Box::pin(run.result())).is_pending());
    let (progress, mut follower) = mpsc::unbounded_channel();
    let (tx, advance) = mpsc::channel(2);
    let mut next = runtime
        .submit(Call {
            service,
            work: Work::Ignore { advance, progress },
        })
        .await
        .unwrap();
    let mut control = next.control.clone();
    bounded(async {
        while control.state() != RunState::WaitingResources {
            control.changed().await.unwrap();
        }
    })
    .await;
    assert!(follower.try_recv().is_err());
    drain.add_permits(1);
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert_eq!(bounded(follower.recv()).await, Some("started"));
    tx.send(()).await.unwrap();
    tx.send(()).await.unwrap();
    bounded(next.result()).await.unwrap();
    runtime.shutdown().await.unwrap();
}
