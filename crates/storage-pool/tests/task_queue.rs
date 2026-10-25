use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use storage_pool::{task_center::*, *};
use tokio::sync::{mpsc, oneshot};
use workflow_foundation::*;

async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), f)
        .await
        .expect("queue stalled")
}
type Event = (u64, bool, ControlToken, oneshot::Sender<Outcome<()>>);
struct Worker(mpsc::UnboundedSender<Event>);
#[async_trait]
impl ServiceHandler for Worker {
    type Request = (u64, bool);
    type Reply = ();
    type Query = ();
    type View = ();
    fn resources(_: &Self::Request) -> Vec<ResourceAccess> {
        vec![]
    }
    async fn execute(
        &self,
        (sequence, required): Self::Request,
        ctx: ActivityContext,
    ) -> Outcome<()> {
        let (done, reply) = oneshot::channel();
        self.0
            .send((sequence, required, ctx.control(), done))
            .unwrap();
        tokio::select! {
            _ = ctx.cancellation().cancelled() => Err(Failure::Cancelled),
            r = reply => r.map_err(|_| Failure::Unavailable)?,
        }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
struct Transition {
    sequence: u64,
    client: ServiceClient<Worker>,
}
impl PoolWorkflow for Transition {
    fn operation(&self) -> PoolOperation {
        let id = NodeId::new("node").unwrap();
        PoolOperation {
            kind: if self.sequence.is_multiple_of(2) {
                OperationKind::OfflineNode {
                    id,
                    sequence: self.sequence,
                }
            } else {
                OperationKind::OnlineNode {
                    id,
                    sequence: self.sequence,
                }
            },
            objects: vec![ObjectKey::new("node", "node")],
        }
    }
}
#[async_trait]
impl Workflow for Transition {
    type Output = ();
    const NAME: &'static str = "test.transition";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        // The critical step represents invalidating/closing the old connection.
        ctx.activity(&self.client, (self.sequence, true))
            .uninterruptible()
            .await?;
        ctx.activity(&self.client, (self.sequence, false)).await
    }
}
fn setup() -> (
    TaskCenter,
    ServiceClient<Worker>,
    mpsc::UnboundedReceiver<Event>,
) {
    let config = Config::default();
    let mut driver = WorkflowDriver::new("queue", config.clone()).unwrap();
    let (tx, rx) = mpsc::unbounded_channel();
    let client = driver.register("worker", Worker(tx)).unwrap();
    (
        TaskCenter::with_policy(driver, &config, Arc::new(PoolSchedulingPolicy)),
        client,
        rx,
    )
}
async fn cancelled(run: &Run<()>) {
    bounded(async {
        while !run.control.cancellation_requested() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}
#[tokio::test]
async fn up_down_up_never_skips_middle_transition_or_cancels_it_while_queued() {
    let (center, client, mut events) = setup();
    let mut a = center
        .submit(Transition {
            sequence: 1,
            client: client.clone(),
        })
        .await
        .unwrap();
    let (sequence, required, token_a, finish_a) = bounded(events.recv()).await.unwrap();
    assert_eq!((sequence, required), (1, true));
    let mut b = center
        .submit(Transition {
            sequence: 2,
            client: client.clone(),
        })
        .await
        .unwrap();
    let mut c = center
        .submit(Transition {
            sequence: 3,
            client,
        })
        .await
        .unwrap();
    cancelled(&a).await;
    assert!(!token_a.is_cancelled());
    assert!(!b.control.has_started());
    assert!(
        !b.control.cancellation_requested(),
        "C must not cancel queued B"
    );
    assert_eq!(center.task(c.id()).unwrap().blockers, vec![b.id()]);
    assert!(events.try_recv().is_err());
    finish_a.send(Ok(())).unwrap();
    assert_eq!(bounded(a.result()).await, Err(Failure::Cancelled));
    let (sequence, required, token_b, finish_b) = bounded(events.recv()).await.unwrap();
    assert_eq!((sequence, required), (2, true));
    cancelled(&b).await;
    assert!(b.control.has_started());
    assert!(
        !token_b.is_cancelled(),
        "middle disconnect must settle despite C"
    );
    assert!(!c.control.has_started());
    assert!(events.try_recv().is_err());
    finish_b.send(Ok(())).unwrap();
    assert_eq!(bounded(b.result()).await, Err(Failure::Cancelled));
    let (sequence, required, _, finish_c) = bounded(events.recv()).await.unwrap();
    assert_eq!((sequence, required), (3, true));
    finish_c.send(Ok(())).unwrap();
    let (sequence, required, _, finish_tail) = bounded(events.recv()).await.unwrap();
    assert_eq!((sequence, required), (3, false));
    finish_tail.send(Ok(())).unwrap();
    bounded(c.result()).await.unwrap();
    center.shutdown().await.unwrap();
}
#[tokio::test]
async fn successor_cancels_ordinary_rebuild_after_required_step_completed() {
    let (center, client, mut events) = setup();
    let mut b = center
        .submit(Transition {
            sequence: 2,
            client: client.clone(),
        })
        .await
        .unwrap();
    let (_, _, _, finish) = bounded(events.recv()).await.unwrap();
    finish.send(Ok(())).unwrap();
    let (_, required, tail_token, _tail) = bounded(events.recv()).await.unwrap();
    assert!(!required);
    let mut c = center
        .submit(Transition {
            sequence: 3,
            client,
        })
        .await
        .unwrap();
    assert_eq!(bounded(b.result()).await, Err(Failure::Cancelled));
    assert!(tail_token.is_cancelled());
    let (sequence, required, _, finish) = bounded(events.recv()).await.unwrap();
    assert_eq!((sequence, required), (3, true));
    finish.send(Ok(())).unwrap();
    let (_, _, _, finish) = bounded(events.recv()).await.unwrap();
    finish.send(Ok(())).unwrap();
    bounded(c.result()).await.unwrap();
    center.shutdown().await.unwrap();
}
#[tokio::test]
async fn uncertain_required_step_fences_successor_instead_of_reconnecting() {
    let (center, client, mut events) = setup();
    let mut b = center
        .submit(Transition {
            sequence: 2,
            client: client.clone(),
        })
        .await
        .unwrap();
    let (_, _, _, finish) = bounded(events.recv()).await.unwrap();
    let mut c = center
        .submit(Transition {
            sequence: 3,
            client,
        })
        .await
        .unwrap();
    finish
        .send(Err(Failure::Uncertain("disconnect not confirmed".into())))
        .unwrap();
    assert!(matches!(
        bounded(b.result()).await,
        Err(Failure::Uncertain(_))
    ));
    assert!(matches!(
        bounded(c.result()).await,
        Err(Failure::Rejected(_))
    ));
    assert!(!c.control.has_started());
    assert!(events.try_recv().is_err());
    center.shutdown().await.unwrap();
}
