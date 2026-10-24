use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use storage_pool::{
    adapters::*,
    domains::{balance::*, shrink::*},
    task_center::*,
    *,
};
use tokio::sync::{mpsc, watch};
use workflow_foundation::{persistence::*, *};

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("workflow stalled")
}
async fn state(control: &RunControl, predicate: impl Fn(&RunState) -> bool) {
    let mut control = control.clone();
    bounded(async {
        while !predicate(&control.state()) {
            control.changed().await.unwrap();
        }
    })
    .await;
}
fn disks() -> Vec<DiskId> {
    vec![DiskId::new("a").unwrap(), DiskId::new("b").unwrap()]
}
fn request() -> ShrinkRequest {
    ShrinkRequest::new(disks()).unwrap()
}

/// Manual native callbacks: accepting suspend/cancel does NOT settle the operation.
struct Call {
    request: Migrate,
    commands: mpsc::UnboundedReceiver<&'static str>,
    result: watch::Sender<Option<Outcome<MigrationExit>>>,
    progress: watch::Sender<MigrationProgress>,
}
impl Call {
    async fn command(&mut self, expected: &str) {
        assert_eq!(bounded(self.commands.recv()).await.unwrap(), expected);
    }
    fn suspended(&self, completed: u64) {
        self.result
            .send_replace(Some(Ok(ActivityExit::Suspended(MigrationCheckpoint {
                resume: serde_json::json!({"cursor": completed}),
                progress: MigrationProgress {
                    completed,
                    total: 10,
                },
            }))));
    }
    fn completed(&self, storage: &SimulatedShrinkStorage) {
        storage.evacuated(&self.request.pool, &self.request.disks);
        self.result
            .send_replace(Some(Ok(ActivityExit::Completed(MigrationProgress {
                completed: 10,
                total: 10,
            }))));
    }
    fn cancelled(&self) {
        self.result.send_replace(Some(Err(Failure::Cancelled)));
    }
}
struct NativeOperation {
    commands: mpsc::UnboundedSender<&'static str>,
    result: watch::Receiver<Option<Outcome<MigrationExit>>>,
    progress: watch::Receiver<MigrationProgress>,
}
#[async_trait]
impl BalanceOperation for NativeOperation {
    async fn completion(&self) -> Outcome<MigrationExit> {
        let mut result = self.result.clone();
        loop {
            if let Some(done) = result.borrow_and_update().clone() {
                return done;
            }
            result
                .changed()
                .await
                .map_err(|_| Failure::Uncertain("C callback lost".into()))?;
        }
    }
    async fn suspend(&self) -> Outcome<()> {
        self.commands.send("suspend").unwrap();
        Ok(())
    }
    async fn cancel(&self) -> Outcome<()> {
        self.commands.send("cancel").unwrap();
        Ok(())
    }
    fn progress(&self) -> watch::Receiver<MigrationProgress> {
        self.progress.clone()
    }
}
struct NativeBackend(mpsc::UnboundedSender<Call>);
#[async_trait]
impl BalanceBackend for NativeBackend {
    async fn submit(&self, request: Migrate) -> Outcome<Arc<dyn BalanceOperation>> {
        let (commands, rx) = mpsc::unbounded_channel();
        let (result, result_rx) = watch::channel(None);
        let (progress, progress_rx) = watch::channel(MigrationProgress {
            completed: request.checkpoint.progress.completed,
            total: 10,
        });
        self.0
            .send(Call {
                request,
                commands: rx,
                result,
                progress,
            })
            .unwrap();
        Ok(Arc::new(NativeOperation {
            commands,
            result: result_rx,
            progress: progress_rx,
        }))
    }
}
struct Fixture {
    center: TaskCenter,
    services: ShrinkServices,
    storage: Arc<SimulatedShrinkStorage>,
    calls: mpsc::UnboundedReceiver<Call>,
}
impl Fixture {
    async fn new() -> Self {
        Self::host(1, None, Arc::new(SimulatedShrinkStorage::default())).await
    }
    async fn host(
        epoch: u64,
        store: Option<Arc<dyn TaskStore>>,
        storage: Arc<SimulatedShrinkStorage>,
    ) -> Self {
        let config = Config::default();
        let mut driver = WorkflowDriver::new("pool", config.clone())
            .unwrap()
            .with_task_ids(7, TaskIds::for_leader(epoch).unwrap())
            .unwrap();
        let (tx, calls) = mpsc::unbounded_channel();
        let services = ShrinkServices::register(
            &mut driver,
            PoolId::new("pool").unwrap(),
            storage.clone(),
            Arc::new(NativeBackend(tx)),
        )
        .unwrap();
        driver
            .define::<ShrinkWorkflow>(Dependencies::default().with(services.clone()))
            .unwrap();
        let restored = if let Some(store) = store {
            driver.enable_persistence(store).await.unwrap();
            driver.restore().await.unwrap()
        } else {
            vec![]
        };
        let center =
            TaskCenter::from_restored(driver, &config, Arc::new(PoolSchedulingPolicy), restored)
                .unwrap();
        Self {
            center,
            services,
            storage,
            calls,
        }
    }
    async fn submit(&self) -> Run<()> {
        bounded(
            self.center
                .submit(ShrinkWorkflow::new(request(), self.services.clone()).unwrap()),
        )
        .await
        .unwrap()
    }
    async fn call(&mut self) -> Call {
        bounded(self.calls.recv()).await.unwrap()
    }
}
struct Work {
    kind: OperationKind,
    started: mpsc::UnboundedSender<()>,
}
#[async_trait]
impl Workflow for Work {
    type Output = ();
    const NAME: &'static str = "test.topology-or-volume";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![ObjectKey::new("disk", "a")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        self.started.send(()).unwrap();
        ctx.next_signal().await?;
        Ok(())
    }
}
impl PoolWorkflow for Work {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: self.kind.clone(),
            objects: self.objects(),
        }
    }
}

#[tokio::test]
async fn topo_waits_for_native_stop_then_resumes_same_task_without_fifo_cycle() {
    let mut f = Fixture::new().await;
    let mut shrink = f.submit().await;
    let first = f.call().await;
    first.progress.send_replace(MigrationProgress {
        completed: 3,
        total: 10,
    });
    let mut observer = f.center.observe_task(shrink.id()).unwrap();
    bounded(async {
        while observer.snapshot().progress.completed != 3 {
            observer.changed().await.unwrap();
        }
    })
    .await;

    // Regression: shrink -> topo -> queued volume -> shrink previously deadlocked.
    let (volume_tx, mut volume_started) = mpsc::unbounded_channel();
    let mut volume = f
        .center
        .submit(Work {
            kind: OperationKind::CreateVolume(VolumeId::new("v").unwrap()),
            started: volume_tx,
        })
        .await
        .unwrap();
    let (topo_tx, mut topo_started) = mpsc::unbounded_channel();
    let mut topo = f
        .center
        .submit(Work {
            kind: OperationKind::OfflineDisk(DiskId::new("a").unwrap()),
            started: topo_tx,
        })
        .await
        .unwrap();
    let mut first = first;
    first.command("suspend").await;
    assert!(
        topo_started.try_recv().is_err(),
        "sending a C command is not a stop ACK"
    );
    assert!(!shrink.control.suspend_acknowledged());
    first.suspended(3);
    bounded(topo_started.recv()).await.unwrap();
    assert!(volume_started.try_recv().is_err());
    assert_eq!(
        f.center.task(shrink.id()).unwrap().details["migration"]["progress"]["completed"],
        3
    );
    assert!(
        f.center
            .submit(ShrinkWorkflow::new(request(), f.services.clone()).unwrap())
            .await
            .is_err()
    );

    shrink.control.suspend().unwrap();
    topo.control
        .signal(Signal::new("finish", ""))
        .await
        .unwrap();
    bounded(topo.result()).await.unwrap();
    bounded(shrink.control.wait_suspended()).await.unwrap();
    assert!(
        f.calls.try_recv().is_err(),
        "topo completion must preserve the user suspension"
    );
    f.storage.set_ready(false);
    shrink.control.resume().unwrap();
    state(&shrink.control, |s| {
        matches!(s, RunState::WaitingConditions(_))
    })
    .await;
    assert!(f.calls.try_recv().is_err());
    f.storage.set_ready(true);
    f.center.recheck(shrink.id()).unwrap();
    let second = f.call().await;
    assert_eq!(second.request.task_id, shrink.id());
    assert_eq!(second.request.disks, disks());
    assert_eq!(second.request.checkpoint.resume["cursor"], 3);
    second.completed(&f.storage);
    bounded(shrink.result()).await.unwrap();
    bounded(volume_started.recv()).await.unwrap();
    volume
        .control
        .signal(Signal::new("finish", ""))
        .await
        .unwrap();
    bounded(volume.result()).await.unwrap();
    assert_eq!(
        f.storage.steps(),
        vec![
            (ShrinkStage::RecordIntent, disks()),
            (ShrinkStage::DisableAllocation, disks()),
            (ShrinkStage::VerifyEvacuated, disks()),
            (ShrinkStage::DisableIo, disks()),
            (ShrinkStage::RemoveMembers, disks()),
        ]
    );
    assert_eq!(f.center.task(shrink.id()).unwrap().activities.total, 7);
    bounded(f.center.shutdown()).await.unwrap();
}

#[tokio::test]
async fn initial_conditions_wait_without_side_effects_and_can_be_cancelled() {
    let f = Fixture::new().await;
    f.storage.set_ready(false);
    let mut run = f.submit().await;
    state(&run.control, |s| {
        matches!(s, RunState::WaitingConditions(_))
    })
    .await;
    assert!(f.storage.steps().is_empty());
    run.control.cancel();
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert!(f.center.resume(run.id()).is_err());
    bounded(f.center.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cancel_running_waits_for_c_completion_and_cancel_suspended_never_resubmits() {
    let mut f = Fixture::new().await;
    let mut running = f.submit().await;
    let mut first = f.call().await;
    running.control.cancel();
    first.command("cancel").await;
    assert!(!matches!(
        running.control.state(),
        RunState::Failed(_) | RunState::Completed
    ));
    first.cancelled();
    assert_eq!(bounded(running.result()).await, Err(Failure::Cancelled));
    assert!(f.center.resume(running.id()).is_err());

    let mut paused = f.submit().await;
    let mut second = f.call().await;
    paused.control.suspend().unwrap();
    second.command("suspend").await;
    second.suspended(4);
    bounded(paused.control.wait_suspended()).await.unwrap();
    paused.control.cancel();
    assert_eq!(bounded(paused.result()).await, Err(Failure::Cancelled));
    assert!(f.center.resume(paused.id()).is_err());
    assert!(
        second.commands.try_recv().is_err(),
        "ended native invocation requires no cancellation cleanup"
    );
    assert!(f.calls.try_recv().is_err());
    assert!(
        !f.storage
            .steps()
            .iter()
            .any(|(s, _)| *s == ShrinkStage::RemoveMembers)
    );
    bounded(f.center.shutdown()).await.unwrap();
}

#[tokio::test]
async fn cancellation_while_suspend_is_in_flight_wins_over_a_suspended_reply() {
    let mut f = Fixture::new().await;
    let mut run = f.submit().await;
    let mut call = f.call().await;
    run.control.suspend().unwrap();
    call.command("suspend").await;
    run.control.cancel();
    call.command("cancel").await;
    call.suspended(5);
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert!(f.center.resume(run.id()).is_err());
    assert!(f.calls.try_recv().is_err());
    bounded(f.center.shutdown()).await.unwrap();
}

#[tokio::test]
async fn durable_shrink_restores_checkpoint_and_user_suspension_under_same_task_id() {
    let store = Arc::new(MemoryTaskStore::default());
    let storage = Arc::new(SimulatedShrinkStorage::default());
    let mut first = Fixture::host(1, Some(store.clone()), storage.clone()).await;
    let mut run = first
        .center
        .submit(
            ShrinkWorkflow::new(request(), first.services.clone())
                .unwrap()
                .durable(),
        )
        .await
        .unwrap();
    let id = run.id();
    let mut call = first.call().await;
    run.control.suspend().unwrap();
    call.command("suspend").await;
    call.suspended(7);
    bounded(run.control.wait_suspended()).await.unwrap();
    bounded(first.center.shutdown()).await.unwrap();
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    let saved = &store.list(7).unwrap()[0];
    assert_eq!(saved.outcome, StoredOutcome::Pending);
    assert!(saved.user_paused);

    let mut next = Fixture::host(2, Some(store.clone()), storage.clone()).await;
    bounded(next.center.wait_suspended(id)).await.unwrap();
    assert!(next.calls.try_recv().is_err());
    next.center.resume(id).unwrap();
    let restored = next.call().await;
    assert_eq!(restored.request.task_id, id);
    assert_eq!(restored.request.checkpoint.progress.completed, 7);
    restored.completed(&storage);
    let mut observer = next.center.observe_task(id).unwrap();
    bounded(async {
        while observer.snapshot().state != RunState::Completed {
            observer.changed().await.unwrap();
        }
    })
    .await;
    assert_eq!(store.list(7).unwrap()[0].outcome, StoredOutcome::Completed);
    assert_eq!(
        storage
            .steps()
            .iter()
            .filter(|(s, _)| *s == ShrinkStage::RecordIntent)
            .count(),
        1
    );
    bounded(next.center.shutdown()).await.unwrap();
}

#[tokio::test]
async fn public_pool_facade_exposes_one_shrink_without_retaining_task_id() {
    let storage = Arc::new(SimulatedShrinkStorage::default());
    storage.set_ready(false);
    let (tx, _calls) = mpsc::unbounded_channel();
    let manager = PoolManager::new(
        Config::default(),
        Arc::new(MemoryMetadata::default()),
        Arc::new(SimulatedDataPlane),
    )
    .with_shrink(storage, Arc::new(NativeBackend(tx)));
    let pool = manager.create_pool(PoolId::new("pool").unwrap()).unwrap();
    let mut run = pool.shrink(request()).await.unwrap();
    assert_eq!(pool.current_shrink().unwrap().id, run.id());
    assert!(pool.shrink(request()).await.is_err());
    pool.cancel(pool.current_shrink().unwrap().id).unwrap();
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert!(pool.current_shrink().is_none());
    bounded(manager.shutdown()).await.unwrap();
}

#[tokio::test]
async fn durable_cancelled_shrink_is_never_restored() {
    let store = Arc::new(MemoryTaskStore::default());
    let storage = Arc::new(SimulatedShrinkStorage::default());
    let mut f = Fixture::host(1, Some(store.clone()), storage.clone()).await;
    let mut run = f
        .center
        .submit(
            ShrinkWorkflow::new(request(), f.services.clone())
                .unwrap()
                .durable(),
        )
        .await
        .unwrap();
    let mut call = f.call().await;
    run.control.suspend().unwrap();
    call.command("suspend").await;
    call.suspended(6);
    bounded(run.control.wait_suspended()).await.unwrap();
    run.control.cancel();
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert_eq!(store.list(7).unwrap()[0].outcome, StoredOutcome::Cancelled);
    bounded(f.center.shutdown()).await.unwrap();
    let mut next = Fixture::host(2, Some(store), storage).await;
    assert!(next.center.tasks().is_empty());
    assert!(next.calls.try_recv().is_err());
    bounded(next.center.shutdown()).await.unwrap();
}

#[tokio::test]
async fn migration_success_without_authoritative_evacuation_never_disables_io_or_removes_disks() {
    let mut f = Fixture::new().await;
    let mut run = f.submit().await;
    let call = f.call().await;
    // Native callback alone is insufficient proof that committed BG references are clear.
    call.result
        .send_replace(Some(Ok(ActivityExit::Completed(MigrationProgress {
            completed: 10,
            total: 10,
        }))));
    assert!(matches!(
        bounded(run.result()).await,
        Err(Failure::Rejected(_))
    ));
    assert!(
        !f.storage
            .steps()
            .iter()
            .any(|(stage, _)| matches!(stage, ShrinkStage::DisableIo | ShrinkStage::RemoveMembers))
    );
    bounded(f.center.shutdown()).await.unwrap();
}

#[tokio::test]
async fn existing_topology_prevents_any_shrink_step_from_starting() {
    let mut f = Fixture::new().await;
    let (started, mut events) = mpsc::unbounded_channel();
    let mut topo = f
        .center
        .submit(Work {
            kind: OperationKind::OfflineDisk(DiskId::new("a").unwrap()),
            started,
        })
        .await
        .unwrap();
    bounded(events.recv()).await.unwrap();
    let mut shrink = f.submit().await;
    assert_eq!(
        f.center.task(shrink.id()).unwrap().blockers,
        vec![topo.id()]
    );
    assert!(f.storage.steps().is_empty());
    assert!(f.calls.try_recv().is_err());
    topo.control
        .signal(Signal::new("finish", ""))
        .await
        .unwrap();
    bounded(topo.result()).await.unwrap();
    f.call().await.completed(&f.storage);
    bounded(shrink.result()).await.unwrap();
    bounded(f.center.shutdown()).await.unwrap();
}
