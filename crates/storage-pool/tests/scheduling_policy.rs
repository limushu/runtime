use async_trait::async_trait;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use storage_pool::{task_center::*, *};
use tokio::sync::{mpsc, oneshot};
use workflow_foundation::{scheduling::*, *};

async fn bounded<T>(f: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), f)
        .await
        .expect("stalled scheduler")
}
struct Pause(mpsc::UnboundedSender<oneshot::Sender<()>>);
struct Worker {
    started: mpsc::UnboundedSender<(String, oneshot::Sender<()>)>,
    pause: Arc<Pause>,
}
#[async_trait]
impl ServiceHandler for Worker {
    type Request = String;
    type Reply = ActivityExit<(), ()>;
    type Query = ();
    type View = ();
    fn resources(_: &String) -> Vec<ResourceAccess> {
        vec![]
    }
    async fn execute(&self, name: String, ctx: ActivityContext) -> Outcome<Self::Reply> {
        let (tx, rx) = oneshot::channel();
        self.started.send((name.clone(), tx)).unwrap();
        let mut control = ctx.control();
        tokio::pin!(rx);
        loop {
            if control.is_cancelled() {
                return Err(Failure::Cancelled);
            }
            if name == "scan" && control.request().paused() {
                let (tx, ack) = oneshot::channel();
                self.pause.0.send(tx).unwrap();
                return tokio::select! {
                    _ = control.cancelled() => Err(Failure::Cancelled),
                    r = ack => { r.map_err(|_| Failure::Unavailable)?; Ok(ActivityExit::Suspended(())) },
                };
            }
            tokio::select! {
                _ = control.changed() => {},
                r = &mut rx => { r.map_err(|_| Failure::Unavailable)?; return Ok(ActivityExit::Completed(())); }
            }
        }
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
        loop {
            match ctx.activity(&self.client, self.name.clone()).await? {
                ActivityExit::Completed(()) => return Ok(()),
                ActivityExit::Suspended(()) => ctx.wait_until_runnable().await?,
            }
        }
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
    fn on_task(&self, _: &PoolOperation, _: &PoolOperation) -> Conflict {
        Conflict {
            decision: ConflictDecision::CancelThenWait,
            reason: "replacement",
        }
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
struct BalanceReady(AtomicBool);
#[async_trait]
impl RunCondition for BalanceReady {
    async fn check(&self) -> Outcome<Readiness> {
        Ok(if self.0.load(Ordering::SeqCst) {
            Readiness::Ready
        } else {
            Readiness::Blocked("balance not ready".into())
        })
    }
}
struct ScheduledStep {
    name: &'static str,
    client: ServiceClient<Worker>,
    ready: Arc<BalanceReady>,
}
#[async_trait]
impl Activity for ScheduledStep {
    type Reply = ActivityExit<(), ()>;
    type Output = ();
    fn name(&self) -> &'static str {
        self.name
    }
    fn run_condition(&self) -> Option<Arc<dyn RunCondition>> {
        Some(self.ready.clone())
    }
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<Self::Reply> {
        scope.call(&self.client, self.name.into()).await
    }
    fn handle(&mut self, result: Outcome<Self::Reply>) -> Outcome<ActivityAction<()>> {
        Ok(match result? {
            ActivityExit::Completed(()) => ActivityAction::Continue(()),
            ActivityExit::Suspended(()) => ActivityAction::Suspend(ActivityWait::Resume),
        })
    }
}
struct ScheduledWork {
    operation: OperationKind,
    step: ScheduledStep,
}
#[async_trait]
impl Workflow for ScheduledWork {
    type Output = ();
    const NAME: &'static str = "test.scheduled-activity";
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.run_activity(self.step).await
    }
}
impl PoolWorkflow for ScheduledWork {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: self.operation.clone(),
            objects: vec![],
        }
    }
}

#[tokio::test]
async fn taskcenter_owns_topology_exclusion_while_activity_owns_business_readiness() {
    let config = Config::default();
    let mut driver = WorkflowDriver::new("pool", config.clone()).unwrap();
    let (started, mut events) = mpsc::unbounded_channel();
    let (pauses, mut acknowledgements) = mpsc::unbounded_channel();
    let client = driver
        .register(
            "worker",
            Worker {
                started,
                pause: Arc::new(Pause(pauses)),
            },
        )
        .unwrap();
    let center = TaskCenter::with_policy(driver, &config, Arc::new(PoolSchedulingPolicy));
    let ready = Arc::new(BalanceReady(AtomicBool::new(true)));
    let disk = DiskId::new("disk-a").unwrap();
    let shrink_work = || ScheduledWork {
        operation: OperationKind::Shrink(vec![disk.clone()]),
        step: ScheduledStep {
            name: "scan",
            client: client.clone(),
            ready: ready.clone(),
        },
    };
    let topo_work = || ScheduledWork {
        operation: OperationKind::OnlineDisk(disk.clone()),
        step: ScheduledStep {
            name: "maintenance",
            client: client.clone(),
            ready: Arc::new(BalanceReady(AtomicBool::new(true))),
        },
    };
    let mut shrink = center.submit(shrink_work()).await.unwrap();
    let (name, _first) = bounded(events.recv()).await.unwrap();
    assert_eq!(name, "scan");
    let logical_id = shrink.control.activities().entries[0].id;

    let mut topo = center.submit(topo_work()).await.unwrap();
    let stopped = bounded(acknowledgements.recv()).await.unwrap();
    assert!(!shrink.control.suspend_acknowledged());
    assert!(
        events.try_recv().is_err(),
        "topology must wait for actual stop"
    );
    stopped.send(()).unwrap();
    let (name, finish_topo) = bounded(events.recv()).await.unwrap();
    assert_eq!(name, "maintenance");
    assert!(matches!(
        center.submit(shrink_work()).await,
        Err(Failure::Rejected(_))
    ));
    ready.0.store(false, Ordering::SeqCst);
    shrink.control.suspend().unwrap();
    finish_topo.send(()).unwrap();
    bounded(topo.result()).await.unwrap();
    let mut observer = center.observe_task(shrink.id()).unwrap();
    bounded(async {
        while !observer.snapshot().blockers.is_empty() {
            observer.changed().await.unwrap();
        }
    })
    .await;
    assert!(shrink.control.suspend_reasons().contains("user"));
    shrink.control.resume().unwrap();
    bounded(async {
        while shrink.control.state() != RunState::WaitingConditions("balance not ready".into()) {
            shrink.control.changed().await.unwrap();
        }
    })
    .await;
    assert!(
        events.try_recv().is_err(),
        "scheduler permission does not satisfy business readiness"
    );
    assert!(matches!(
        center.submit(shrink_work()).await,
        Err(Failure::Rejected(_))
    ));

    // A step blocked on its own condition must still acknowledge scheduler pause.
    let mut topo = center.submit(topo_work()).await.unwrap();
    let (name, finish_topo) = bounded(events.recv()).await.unwrap();
    assert_eq!(name, "maintenance");
    ready.0.store(true, Ordering::SeqCst);
    center.recheck(shrink.id()).unwrap();
    assert!(
        events.try_recv().is_err(),
        "business readiness cannot bypass topology exclusion"
    );
    assert!(shrink.control.suspend_acknowledged());
    finish_topo.send(()).unwrap();
    bounded(topo.result()).await.unwrap();
    let (name, finish_shrink) = bounded(events.recv()).await.unwrap();
    assert_eq!(name, "scan");
    finish_shrink.send(()).unwrap();
    bounded(shrink.result()).await.unwrap();
    let history = shrink.control.activities();
    let root = history
        .entries
        .iter()
        .find(|entry| entry.id == logical_id)
        .unwrap();
    assert_eq!(root.attempts, 2);
    assert_eq!(root.retries, 0);
    assert_eq!(root.state, ActivityState::Completed);
    center.shutdown().await.unwrap();
}

struct Policy;
impl SchedulingPolicy<PoolOperation> for Policy {
    fn on_task(&self, existing: &PoolOperation, incoming: &PoolOperation) -> Conflict {
        let decision = if existing.kind == OperationKind::Custom("scan".into())
            && incoming.kind == OperationKind::Custom("maintenance".into())
        {
            ConflictDecision::SuspendThenWait
        } else if existing.kind == OperationKind::Custom("maintenance".into())
            && incoming.kind == OperationKind::Custom("scan".into())
        {
            ConflictDecision::Wait
        } else {
            ConflictDecision::Independent
        };
        Conflict {
            decision,
            reason: "maintenance",
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
    let (name, _first_scan) = bounded(events.recv()).await.unwrap();
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
    let (name, finish_scan) = bounded(events.recv()).await.unwrap();
    assert_eq!(name, "scan");
    finish_scan.send(()).unwrap();
    bounded(scan.result()).await.unwrap();
    center.shutdown().await.unwrap();
}

struct Veto;
impl SchedulingPolicy<PoolOperation> for Veto {
    fn on_task(&self, existing: &PoolOperation, incoming: &PoolOperation) -> Conflict {
        if existing.kind == OperationKind::Custom("guard".into())
            && incoming.kind == OperationKind::Custom("maintenance".into())
        {
            Conflict {
                decision: ConflictDecision::Reject("guard refuses maintenance"),
                reason: "guard",
            }
        } else {
            Policy.on_task(existing, incoming)
        }
    }
}
#[tokio::test]
async fn rejection_by_one_task_does_not_suspend_another() {
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
    let center = TaskCenter::with_policy(driver, &config, Arc::new(Veto));
    let mut scan = center
        .submit(Work {
            name: "scan".into(),
            client: client.clone(),
        })
        .await
        .unwrap();
    let (_, finish_scan) = bounded(events.recv()).await.unwrap();
    let mut guard = center
        .submit(Work {
            name: "guard".into(),
            client: client.clone(),
        })
        .await
        .unwrap();
    let (_, finish_guard) = bounded(events.recv()).await.unwrap();
    assert!(
        center
            .submit(Work {
                name: "maintenance".into(),
                client
            })
            .await
            .is_err()
    );
    assert!(!scan.control.suspend_requested());
    assert!(ack.try_recv().is_err());
    finish_scan.send(()).unwrap();
    finish_guard.send(()).unwrap();
    bounded(scan.result()).await.unwrap();
    bounded(guard.result()).await.unwrap();
    center.shutdown().await.unwrap();
}

#[tokio::test]
async fn only_queue_head_owns_pause_and_next_head_takes_over_without_resuming_scan() {
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
    let (_, _old_call) = bounded(events.recv()).await.unwrap();
    let mut first = center
        .submit(Work {
            name: "maintenance".into(),
            client: client.clone(),
        })
        .await
        .unwrap();
    let stopping = bounded(ack.recv()).await.unwrap();
    let mut second = center
        .submit(Work {
            name: "maintenance".into(),
            client,
        })
        .await
        .unwrap();
    assert!(events.try_recv().is_err());
    stopping.send(()).unwrap();
    let (_, finish_one) = bounded(events.recv()).await.unwrap();
    assert!(
        events.try_recv().is_err(),
        "second maintenance stays behind the head"
    );
    assert_eq!(scan.control.suspend_reasons().len(), 1);
    assert_eq!(center.task(second.id()).unwrap().blockers, vec![first.id()]);
    finish_one.send(()).unwrap();
    let (name, finish_two) = bounded(events.recv()).await.unwrap();
    assert_eq!(
        name, "maintenance",
        "scan must not restart between queue heads"
    );
    assert_eq!(scan.control.suspend_reasons().len(), 1);
    assert!(
        !scan
            .control
            .suspend_reasons()
            .iter()
            .any(|r| r.contains(&first.id().to_string()))
    );
    finish_two.send(()).unwrap();
    bounded(first.result()).await.unwrap();
    bounded(second.result()).await.unwrap();
    let (name, finish_scan) = bounded(events.recv()).await.unwrap();
    assert_eq!(name, "scan");
    finish_scan.send(()).unwrap();
    bounded(scan.result()).await.unwrap();
    center.shutdown().await.unwrap();
}
