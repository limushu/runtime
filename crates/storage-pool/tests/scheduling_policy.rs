use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use storage_pool::{task_center::*, *};
use tokio::sync::{mpsc, oneshot};
use workflow_foundation::{scheduling::*, *};

async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), f)
        .await
        .expect("stalled scheduler")
}
struct Pause(mpsc::UnboundedSender<oneshot::Sender<()>>);
#[async_trait]
impl ActivityControl for Pause {
    async fn suspend(&self) -> Outcome<()> {
        let (tx, rx) = oneshot::channel();
        self.0.send(tx).unwrap();
        rx.await.map_err(|_| Failure::Unavailable)
    }
    async fn resume(&self) -> Outcome<()> {
        Ok(())
    }
}
struct Worker {
    started: mpsc::UnboundedSender<(String, oneshot::Sender<()>)>,
    pause: Arc<Pause>,
}
#[async_trait]
impl ServiceHandler for Worker {
    type Request = String;
    type Reply = ();
    type Query = ();
    type View = ();
    fn resources(_: &String) -> Vec<ResourceAccess> {
        vec![]
    }
    async fn execute(&self, name: String, ctx: ActivityContext) -> Outcome<()> {
        if name == "scan" {
            ctx.register_control(self.pause.clone())?;
        }
        let (tx, rx) = oneshot::channel();
        self.started.send((name, tx)).unwrap();
        tokio::select! { _ = ctx.cancellation().cancelled() => Err(Failure::Cancelled), r = rx => r.map_err(|_| Failure::Unavailable) }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
struct Work {
    name: String,
    client: ServiceClient<Worker>,
}
#[async_trait]
impl Workflow for Work {
    type Output = ();
    const NAME: &'static str = "test.work";
    fn persistence(&self) -> Outcome<Persistence> {
        if self.name == "replacement" {
            Persistence::durable(1, &self.name)
        } else {
            Ok(Persistence::Memory)
        }
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.activity(&self.client, self.name).await
    }
}

struct RejectWrites;
impl workflow_foundation::persistence::TaskStore for RejectWrites {
    fn claim(&self, _: u128, _: u64) -> Outcome<()> {
        Ok(())
    }
    fn list(&self, _: u128) -> Outcome<Vec<workflow_foundation::persistence::SavedTask>> {
        Ok(vec![])
    }
    fn save(&self, _: workflow_foundation::persistence::SavedTask, _: Option<u64>) -> Outcome<()> {
        Err(Failure::Failed("store unavailable".into()))
    }
}
struct Replace;
impl SchedulingPolicy<PoolOperation> for Replace {
    fn admission(&self, _: &PoolOperation, _: &PoolOperation) -> Conflict {
        Conflict {
            decision: ConflictDecision::CancelThenWait,
            reason: "replacement",
        }
    }
    fn relation(&self, _: &PoolOperation, _: &PoolOperation) -> Relation {
        Relation::Exclusive
    }
}
#[tokio::test]
async fn failed_durable_replacement_does_not_cancel_or_pause_existing_work() {
    let (started, mut events) = mpsc::unbounded_channel();
    let (pauses, mut ack) = mpsc::unbounded_channel();
    let config = Config::default();
    let mut driver = WorkflowDriver::new("pool", config.clone())
        .unwrap()
        .with_task_ids(7, TaskIds::for_leader(1).unwrap())
        .unwrap();
    driver
        .enable_persistence(Arc::new(RejectWrites))
        .await
        .unwrap();
    driver.restore().await.unwrap();
    let client = driver
        .register(
            "worker",
            Worker {
                started,
                pause: Arc::new(Pause(pauses)),
            },
        )
        .unwrap();
    let center = TaskCenter::with_policy(driver, &config, Arc::new(Replace));
    let mut old = center
        .submit(Work {
            name: "scan".into(),
            client: client.clone(),
        })
        .await
        .unwrap();
    let (_, finish) = bounded(events.recv()).await.unwrap();
    assert!(
        bounded(center.submit(Work {
            name: "replacement".into(),
            client
        }))
        .await
        .is_err()
    );
    assert!(!old.control.cancellation_requested());
    assert!(!old.control.suspend_requested());
    assert!(ack.try_recv().is_err());
    assert!(events.try_recv().is_err());
    finish.send(()).unwrap();
    bounded(old.result()).await.unwrap();
    center.shutdown().await.unwrap();
}
impl PoolWorkflow for Work {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::Custom(self.name.clone()),
            objects: vec![],
        }
    }
}
struct Policy;
impl SchedulingPolicy<PoolOperation> for Policy {
    fn admission(&self, _: &PoolOperation, _: &PoolOperation) -> Conflict {
        Conflict {
            decision: ConflictDecision::Wait,
            reason: "custom",
        }
    }
    fn relation(&self, task: &PoolOperation, other: &PoolOperation) -> Relation {
        if task.kind == OperationKind::Custom("scan".into())
            && other.kind == OperationKind::Custom("maintenance".into())
        {
            Relation::YieldTo("maintenance")
        } else {
            Relation::Independent
        }
    }
}
#[tokio::test]
async fn policy_waits_for_real_ack_exposes_live_blockers_and_preserves_user_pause() {
    let (started, mut events) = mpsc::unbounded_channel();
    let (pauses, mut ack) = mpsc::unbounded_channel();
    let config = Config::default();
    let mut driver = WorkflowDriver::new("pool", config.clone()).unwrap();
    let client = driver
        .register(
            "worker",
            Worker {
                started,
                pause: Arc::new(Pause(pauses)),
            },
        )
        .unwrap();
    let center = TaskCenter::with_policy(driver, &config, Arc::new(Policy));
    let mut scan = center
        .submit(Work {
            name: "scan".into(),
            client: client.clone(),
        })
        .await
        .unwrap();
    let (name, finish_scan) = bounded(events.recv()).await.unwrap();
    assert_eq!(name, "scan");
    let mut maintenance = center
        .submit(Work {
            name: "maintenance".into(),
            client: client.clone(),
        })
        .await
        .unwrap();
    let pause = bounded(ack.recv()).await.unwrap();
    assert!(events.try_recv().is_err());
    let snapshot = center.task(scan.id()).unwrap();
    assert_eq!(snapshot.blockers, vec![maintenance.id()]);
    assert!(!snapshot.suspend_acknowledged);
    pause.send(()).unwrap();
    let (name, finish_maintenance) = bounded(events.recv()).await.unwrap();
    assert_eq!(name, "maintenance");
    scan.control.suspend().unwrap();
    finish_maintenance.send(()).unwrap();
    bounded(maintenance.result()).await.unwrap();
    bounded(scan.control.wait_suspended()).await.unwrap();
    assert!(scan.control.suspend_reasons().contains("user"));
    let mut observer = center.observe_task(scan.id()).unwrap();
    bounded(async {
        while !observer.snapshot().blockers.is_empty() {
            observer.changed().await.unwrap();
        }
    })
    .await;
    scan.control.resume().unwrap();
    finish_scan.send(()).unwrap();
    bounded(scan.result()).await.unwrap();
    center.shutdown().await.unwrap();
}
