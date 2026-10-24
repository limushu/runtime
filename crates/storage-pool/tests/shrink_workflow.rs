use async_trait::async_trait;
use std::{sync::Arc, time::Duration};
use storage_pool::{
    adapters::*,
    domains::balance::*,
    ports::{MetadataChange, MetadataStore},
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

async fn populate(manager: &PoolManager) -> PoolHandle {
    populate_with_capacity(manager, 16).await
}
async fn populate_with_capacity(manager: &PoolManager, capacity: u32) -> PoolHandle {
    let pool = manager.create_pool(PoolId::new("pool").unwrap()).unwrap();
    let tier = TierId::new("tier").unwrap();
    pool.create_tier(tier.clone())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    for name in ["a", "b", "c", "d"] {
        let node = NodeId::new(format!("node-{name}")).unwrap();
        pool.attach_node(node.clone(), 1, true)
            .await
            .unwrap()
            .result()
            .await
            .unwrap();
        pool.load_node(node.clone())
            .await
            .unwrap()
            .result()
            .await
            .unwrap();
        pool.add_disk(DiskSpec {
            id: DiskId::new(name).unwrap(),
            node,
            tier: tier.clone(),
            capacity_blks: capacity,
        })
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    }
    pool.create_volume(CreateVolume {
        id: VolumeId::new("data").unwrap(),
        disks: disks(),
    })
    .await
    .unwrap()
    .result()
    .await
    .unwrap();
    pool
}
async fn set_ready(storage: &MemoryMetadata, ready: bool) {
    let pool = PoolId::new("pool").unwrap();
    let a = storage
        .disks(&pool)
        .await
        .unwrap()
        .unwrap()
        .disks
        .into_iter()
        .find(|d| d.id.as_str() == "a")
        .unwrap();
    storage
        .commit(
            &pool,
            MetadataChange::DiskIo {
                disk: a.id,
                expected_revision: a.revision,
                io: if ready { IoState::Up } else { IoState::Down },
            },
        )
        .await
        .unwrap();
}
fn intent_count(storage: &MemoryMetadata) -> usize {
    storage
        .committed()
        .iter()
        .filter(|(_, c)| matches!(c, MetadataChange::RequestShrink { .. }))
        .count()
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
    fn completed(&self, storage: &MemoryMetadata) {
        storage
            .evacuate(&self.request.pool, &self.request.disks)
            .unwrap();
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
    services: Domains,
    storage: Arc<MemoryMetadata>,
    calls: mpsc::UnboundedReceiver<Call>,
}
impl Fixture {
    async fn new() -> Self {
        Self::host(1, None, Arc::new(MemoryMetadata::default())).await
    }
    async fn host(
        epoch: u64,
        store: Option<Arc<dyn TaskStore>>,
        storage: Arc<MemoryMetadata>,
    ) -> Self {
        if storage
            .disks(&PoolId::new("pool").unwrap())
            .await
            .unwrap()
            .unwrap()
            .disks
            .is_empty()
        {
            let manager = PoolManager::new(
                Config::default(),
                storage.clone(),
                Arc::new(SimulatedDataPlane),
            );
            populate(&manager).await;
            manager.shutdown().await.unwrap();
        }
        let config = Config::default();
        let mut driver = WorkflowDriver::new("pool", config.clone())
            .unwrap()
            .with_task_ids(7, TaskIds::for_leader(epoch).unwrap())
            .unwrap();
        let (tx, calls) = mpsc::unbounded_channel();
        let services = Domains::register(
            &mut driver,
            PoolId::new("pool").unwrap(),
            storage.clone(),
            Arc::new(SimulatedDataPlane),
            Some(Arc::new(NativeBackend(tx))),
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
    set_ready(&f.storage, false).await;
    shrink.control.resume().unwrap();
    state(&shrink.control, |s| {
        matches!(s, RunState::WaitingConditions(_))
    })
    .await;
    assert!(f.calls.try_recv().is_err());
    set_ready(&f.storage, true).await;
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
    assert_eq!(intent_count(&f.storage), 1);
    assert_eq!(
        f.storage
            .disks(&PoolId::new("pool").unwrap())
            .await
            .unwrap()
            .unwrap()
            .disks
            .len(),
        2
    );
    assert_eq!(f.center.task(shrink.id()).unwrap().activities.total, 8);
    bounded(f.center.shutdown()).await.unwrap();
}

#[tokio::test]
async fn initial_conditions_wait_without_side_effects_and_can_be_cancelled() {
    let f = Fixture::new().await;
    set_ready(&f.storage, false).await;
    let mut run = f.submit().await;
    state(&run.control, |s| {
        matches!(s, RunState::WaitingConditions(_))
    })
    .await;
    assert_eq!(intent_count(&f.storage), 0);
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
    f.storage
        .evacuate(&PoolId::new("pool").unwrap(), &[disks()[0].clone()])
        .unwrap();
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
            .committed()
            .iter()
            .any(|(_, c)| matches!(c, MetadataChange::RemoveDisks { .. }))
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
    let storage = Arc::new(MemoryMetadata::default());
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
    assert_eq!(intent_count(&storage), 1);
    bounded(next.center.shutdown()).await.unwrap();
}

#[tokio::test]
async fn public_pool_facade_exposes_one_shrink_without_retaining_task_id() {
    let storage = Arc::new(MemoryMetadata::default());
    let (tx, _calls) = mpsc::unbounded_channel();
    let manager = PoolManager::new(
        Config::default(),
        storage.clone(),
        Arc::new(SimulatedDataPlane),
    )
    .with_balance(Arc::new(NativeBackend(tx)));
    let pool = populate(&manager).await;
    set_ready(&storage, false).await;
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
    let storage = Arc::new(MemoryMetadata::default());
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
        f.storage
            .disks(&PoolId::new("pool").unwrap())
            .await
            .unwrap()
            .unwrap()
            .disks
            .iter()
            .all(|d| d.io == IoState::Up)
    );
    assert!(
        !f.storage
            .committed()
            .iter()
            .any(|(_, c)| matches!(c, MetadataChange::RemoveDisks { .. }))
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
    assert_eq!(intent_count(&f.storage), 0);
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

#[tokio::test]
async fn real_domains_observe_partial_migration_and_remove_all_member_indexes() {
    let storage = Arc::new(MemoryMetadata::default());
    let (tx, mut calls) = mpsc::unbounded_channel();
    let manager = PoolManager::new(
        Config::default(),
        storage.clone(),
        Arc::new(SimulatedDataPlane),
    )
    .with_balance(Arc::new(NativeBackend(tx)));
    let pool = populate(&manager).await;
    let mut shrink = pool.shrink(request()).await.unwrap();
    let mut first = bounded(calls.recv()).await.unwrap();
    assert_eq!(first.request.disks, disks());
    pool.suspend(shrink.id()).unwrap();
    first.command("suspend").await;
    // A real native job may commit part of its work before it acknowledges stop.
    storage.evacuate(pool.id(), &[disks()[0].clone()]).unwrap();
    first.suspended(3);
    bounded(pool.wait_suspended(shrink.id())).await.unwrap();
    let members = pool.disks().await.unwrap();
    let a = members.iter().find(|d| d.id.as_str() == "a").unwrap();
    assert!(a.allocated.is_empty());
    assert!(a.shrink_requested);
    assert_eq!(a.allocation, AllocationState::Inactive);
    assert!(
        pool.volumes().await.unwrap()[0].groups[0]
            .entries
            .iter()
            .all(|e| e.disk != a.id)
    );

    // Topology consumes the refreshed disk revision and preserves shrink intent.
    bounded(pool.online_disk(a.id.clone()).await.unwrap().result())
        .await
        .unwrap();
    assert_eq!(
        pool.disks()
            .await
            .unwrap()
            .iter()
            .find(|d| d.id == a.id)
            .unwrap()
            .allocation,
        AllocationState::Inactive
    );
    assert!(calls.try_recv().is_err());
    pool.resume(shrink.id()).unwrap();
    let second = bounded(calls.recv()).await.unwrap();
    assert_eq!(second.request.task_id, shrink.id());
    assert_eq!(second.request.disks, disks());
    second.completed(&storage);
    bounded(shrink.result()).await.unwrap();

    let remaining = pool.disks().await.unwrap();
    assert_eq!(remaining.len(), 2);
    assert!(
        remaining
            .iter()
            .all(|d| !disks().contains(&d.id) && d.allocated.len() == 1)
    );
    let volume = pool.volumes().await.unwrap().remove(0);
    assert_eq!(volume.replicas, 2);
    assert_eq!(volume.groups[0].entries.len(), 2);
    assert!(
        volume.groups[0]
            .entries
            .iter()
            .all(|e| !disks().contains(&e.disk) && e.data == DataState::Valid)
    );
    let receipt = pool.try_notify(Arc::new(TopologyEvent::new(
        1,
        1,
        TopologyChange::DiskAvailability {
            disk: disks()[0].clone(),
            online: true,
        },
    )));
    assert_eq!(receipt.snapshot().status, TargetStatus::NotRelevant);
    bounded(
        pool.detach_node(NodeId::new("node-a").unwrap())
            .await
            .unwrap()
            .result(),
    )
    .await
    .unwrap();

    // The manager's physical disk ownership was released too.
    let other = manager.create_pool(PoolId::new("other").unwrap()).unwrap();
    let tier = TierId::new("tier").unwrap();
    other
        .create_tier(tier.clone())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    let node = NodeId::new("node-a").unwrap();
    other
        .attach_node(node.clone(), 1, true)
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    other
        .load_node(node.clone())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    other
        .add_disk(DiskSpec {
            id: disks()[0].clone(),
            node,
            tier,
            capacity_blks: 16,
        })
        .await
        .unwrap()
        .result()
        .await
        .unwrap();

    // Ordinary VD deletion now releases the migrated allocations via DiskService.
    bounded(pool.delete_volume(volume.id).await.unwrap().result())
        .await
        .unwrap();
    assert!(
        pool.disks()
            .await
            .unwrap()
            .iter()
            .all(|d| d.allocated.is_empty())
    );
    bounded(manager.shutdown()).await.unwrap();
}

#[tokio::test]
async fn insufficient_failure_domains_block_before_shrink_effects() {
    let mut f = Fixture::new().await;
    let pool = PoolId::new("pool").unwrap();
    let d = f
        .storage
        .disks(&pool)
        .await
        .unwrap()
        .unwrap()
        .disks
        .into_iter()
        .find(|d| d.id.as_str() == "d")
        .unwrap();
    f.storage
        .commit(
            &pool,
            MetadataChange::DiskIo {
                disk: d.id.clone(),
                expected_revision: d.revision,
                io: IoState::Down,
            },
        )
        .await
        .unwrap();
    let mut run = f.submit().await;
    state(&run.control, |s| {
        matches!(s, RunState::WaitingConditions(_))
    })
    .await;
    assert_eq!(intent_count(&f.storage), 0);
    assert!(f.calls.try_recv().is_err());
    f.storage
        .commit(
            &pool,
            MetadataChange::DiskIo {
                disk: d.id,
                expected_revision: d.revision + 1,
                io: IoState::Up,
            },
        )
        .await
        .unwrap();
    f.center.recheck(run.id()).unwrap();
    f.call().await.completed(&f.storage);
    bounded(run.result()).await.unwrap();
    bounded(f.center.shutdown()).await.unwrap();
}

#[tokio::test]
async fn batch_metadata_rejects_stale_revision_without_marking_any_disk() {
    let f = Fixture::new().await;
    let pool = PoolId::new("pool").unwrap();
    let before = f.storage.disks(&pool).await.unwrap().unwrap().disks;
    let revisions = disks()
        .iter()
        .map(|id| {
            let d = before.iter().find(|d| &d.id == id).unwrap();
            (id.clone(), d.revision + u64::from(id.as_str() == "b"))
        })
        .collect();
    assert!(
        f.storage
            .commit(&pool, MetadataChange::RequestShrink { disks: revisions })
            .await
            .is_err()
    );
    assert_eq!(before, f.storage.disks(&pool).await.unwrap().unwrap().disks);
    bounded(f.center.shutdown()).await.unwrap();
}

#[tokio::test]
async fn capacity_wait_rechecks_after_an_ordinary_volume_deletion() {
    let storage = Arc::new(MemoryMetadata::default());
    let (tx, mut calls) = mpsc::unbounded_channel();
    let manager = PoolManager::new(
        Config::default(),
        storage.clone(),
        Arc::new(SimulatedDataPlane),
    )
    .with_balance(Arc::new(NativeBackend(tx)));
    let pool = populate_with_capacity(&manager, 1).await;
    let full = VolumeId::new("full").unwrap();
    pool.create_volume(CreateVolume {
        id: full.clone(),
        disks: vec![DiskId::new("c").unwrap(), DiskId::new("d").unwrap()],
    })
    .await
    .unwrap()
    .result()
    .await
    .unwrap();
    let mut run = pool.shrink(request()).await.unwrap();
    state(&run.control, |s| {
        matches!(s, RunState::WaitingConditions(_))
    })
    .await;
    assert_eq!(intent_count(&storage), 0);
    assert!(calls.try_recv().is_err());
    bounded(pool.delete_volume(full).await.unwrap().result())
        .await
        .unwrap();
    pool.recheck_task(run.id()).unwrap();
    bounded(calls.recv()).await.unwrap().completed(&storage);
    bounded(run.result()).await.unwrap();
    bounded(manager.shutdown()).await.unwrap();
}

#[tokio::test]
async fn no_bg_references_is_insufficient_when_allocations_remain() {
    let mut f = Fixture::new().await;
    let mut run = f.submit().await;
    let call = f.call().await;
    let pool = PoolId::new("pool").unwrap();
    let volume = f
        .storage
        .volumes(&pool)
        .await
        .unwrap()
        .unwrap()
        .volumes
        .remove(0);
    f.storage
        .commit(
            &pool,
            MetadataChange::DeleteVolume {
                volume: volume.id,
                expected_revision: volume.revision,
            },
        )
        .await
        .unwrap();
    call.result
        .send_replace(Some(Ok(ActivityExit::Completed(MigrationProgress {
            completed: 10,
            total: 10,
        }))));
    assert!(matches!(
        bounded(run.result()).await,
        Err(Failure::Rejected(_))
    ));
    let members = f.storage.disks(&pool).await.unwrap().unwrap().disks;
    assert!(members.iter().all(|d| d.io == IoState::Up));
    assert_eq!(members.len(), 4);
    bounded(f.center.shutdown()).await.unwrap();
}
