use async_trait::async_trait;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use storage_pool::TaskRun;
use storage_pool::{adapters::*, domains::balance::*, ports::*, task_center::persistence::*, *};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use workflow_foundation::{ActivityState, RunState};

const LIMIT: Duration = Duration::from_secs(5);
async fn bounded<T>(f: impl Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, f).await.expect("timed out")
}
async fn done<T>(mut run: TaskRun<T>) -> Outcome<T> {
    bounded(run.result()).await
}
fn disk(s: &str) -> DiskId {
    DiskId::new(s).unwrap()
}
fn node(s: &str) -> NodeId {
    NodeId::new(s).unwrap()
}
fn topology() -> Vec<TopologyEvent> {
    (1..=3)
        .map(|i| {
            TopologyEvent::new(
                2,
                10,
                TopologyChange::NodeConnectivity {
                    node: node(&format!("n{i}")),
                    connected: true,
                },
            )
        })
        .collect()
}

struct Fixture {
    manager: PoolManager,
    pool: PoolHandle,
    metadata: Arc<MemoryMetadata>,
    balancer: Arc<SimulatedBalancer>,
}
impl Fixture {
    async fn new(volumes: usize, targets: bool, store: Option<Arc<dyn TaskStore>>) -> Self {
        let metadata = Arc::new(MemoryMetadata::default());
        let balancer = Arc::new(SimulatedBalancer::new(
            metadata.clone(),
            Duration::from_millis(160),
        ));
        let mut manager = PoolManager::new(
            Config::default(),
            metadata.clone(),
            Arc::new(SimulatedDataPlane),
        )
        .with_balancer(balancer.clone())
        .unwrap();
        if let Some(store) = store {
            manager = manager
                .with_task_store(store, PersistenceConfig::default())
                .unwrap();
        }
        let pool = manager.create_pool(PoolId::new("pool").unwrap()).unwrap();
        if pool.task_persistence_health().is_ok() {
            pool.takeover(vec![]).await.unwrap();
        }
        done(pool.create_tier(TierId::new("ssd").unwrap()).await.unwrap())
            .await
            .unwrap();
        for index in 1..=if targets { 3 } else { 2 } {
            add(&pool, index).await;
        }
        for i in 0..volumes {
            done(
                pool.create_volume(CreateVolume {
                    id: VolumeId::new(format!("v{i}")).unwrap(),
                    disks: vec![disk("d1"), disk("d2")],
                })
                .await
                .unwrap(),
            )
            .await
            .unwrap();
        }
        Self {
            manager,
            pool,
            metadata,
            balancer,
        }
    }
    async fn close(self) {
        bounded(self.manager.shutdown()).await.unwrap();
    }
}
async fn add(pool: &PoolHandle, index: usize) {
    let n = node(&format!("n{index}"));
    done(pool.attach_node(n.clone(), 1, true).await.unwrap())
        .await
        .unwrap();
    done(pool.load_node(n.clone()).await.unwrap())
        .await
        .unwrap();
    done(
        pool.add_disk(DiskSpec {
            id: disk(&format!("d{index}")),
            node: n,
            tier: TierId::new("ssd").unwrap(),
            capacity_blks: 16,
        })
        .await
        .unwrap(),
    )
    .await
    .unwrap();
}
async fn migrating(pool: &PoolHandle, id: TaskId, minimum: u64) {
    let mut observer = pool.observe_task(id).unwrap();
    bounded(async {
        loop {
            let task = observer.snapshot();
            if task
                .execution
                .as_ref()
                .unwrap()
                .progress
                .stage
                .as_ref()
                .is_some_and(|s| s.starts_with("evacuating "))
                && task.execution.as_ref().unwrap().progress.completed >= minimum
            {
                break;
            }
            assert!(
                !matches!(
                    task.execution.as_ref().unwrap().state,
                    RunState::Completed | RunState::Failed(_)
                ),
                "ended before migration: {:?}",
                task.execution.as_ref().unwrap().state
            );
            observer.changed().await.unwrap();
        }
    })
    .await;
}
async fn wait_state(pool: &PoolHandle, id: TaskId, predicate: impl Fn(&RunState) -> bool) {
    let mut observer = pool.observe_task(id).unwrap();
    bounded(async {
        while !predicate(&observer.snapshot().execution.as_ref().unwrap().state) {
            observer.changed().await.unwrap();
        }
    })
    .await;
}

#[tokio::test]
async fn complete_workflow_commits_real_remaps_verifies_projections_and_retains_removed_identity() {
    let f = Fixture::new(2, true, None).await;
    let mut run = f.pool.shrink_disk(disk("d1")).await.unwrap();
    let id = run.id();
    let removed = bounded(run.result()).await.unwrap();
    assert_eq!(removed.membership, DiskMembership::Removed);
    assert_eq!(removed.io, IoState::Down);
    assert_eq!(removed.allocation, AllocationState::Inactive);
    assert!(removed.shrink.is_some());
    assert!(removed.allocated.is_empty());
    assert_eq!(f.pool.disk(disk("d1")).await.unwrap(), removed);
    assert!(
        f.pool
            .disks()
            .await
            .unwrap()
            .iter()
            .all(|d| d.id != disk("d1"))
    );
    for volume in f.pool.volumes().await.unwrap() {
        assert_eq!(volume.replicas, 2);
        assert!(
            volume
                .groups
                .iter()
                .flat_map(|g| &g.entries)
                .all(|e| e.disk != disk("d1") && e.data == DataState::Valid)
        );
    }
    assert_eq!(f.balancer.jobs_started(), 1);
    let task = f.pool.task(id).unwrap();
    let names: Vec<_> = task
        .execution
        .as_ref()
        .unwrap()
        .activities
        .entries
        .iter()
        .map(|a| a.name)
        .collect();
    assert_eq!(
        names,
        [
            "disk.inspect",
            "disk.request-shrink",
            "disk.disable-allocation",
            "balance.evacuate-whole-disk",
            "vd.require-disk-unreferenced",
            "disk.require-empty",
            "pool.finalize-shrink"
        ]
    );
    assert!(
        task.execution
            .as_ref()
            .unwrap()
            .activities
            .entries
            .iter()
            .all(|a| a.state == ActivityState::Completed)
    );
    let log = f.metadata.committed();
    let intent = log
        .iter()
        .position(|(_, c)| matches!(c, MetadataChange::ShrinkIntent { .. }))
        .unwrap();
    let disabled = log
        .iter()
        .position(|(_, c)| matches!(c, MetadataChange::DisableAllocation { .. }))
        .unwrap();
    let remaps: Vec<_> = log
        .iter()
        .enumerate()
        .filter(|(_, (_, c))| matches!(c, MetadataChange::Remap { .. }))
        .map(|(i, _)| i)
        .collect();
    let removed = log
        .iter()
        .position(|(_, c)| matches!(c, MetadataChange::MarkRemoved { .. }))
        .unwrap();
    assert!(intent < disabled && disabled < remaps[0] && remaps[1] < removed);
    assert!(f.pool.online_disk(disk("d1")).await.is_err());
    done(
        f.pool
            .delete_volume(VolumeId::new("v0").unwrap())
            .await
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(f.pool.volumes().await.unwrap().len(), 1);
    f.close().await;
}

#[tokio::test]
async fn missing_capacity_waits_then_topology_creation_wakes_the_guard_and_runs_the_same_task() {
    let f = Fixture::new(1, false, None).await;
    let mut run = f.pool.shrink_disk(disk("d1")).await.unwrap();
    wait_state(&f.pool, run.id(), |s| {
        matches!(s, RunState::WaitingConditions(_))
    })
    .await;
    assert_eq!(f.balancer.jobs_started(), 0);
    assert!(f.pool.disk(disk("d1")).await.unwrap().shrink.is_none());
    add(&f.pool, 3).await;
    assert_eq!(
        bounded(run.result()).await.unwrap().membership,
        DiskMembership::Removed
    );
    assert_eq!(f.balancer.jobs_started(), 1);
    f.close().await;
}

#[tokio::test]
async fn cancellation_stops_the_job_preserves_remaps_and_requires_explicit_intent_abandonment() {
    let f = Fixture::new(4, true, None).await;
    let mut run = f.pool.shrink_disk(disk("d1")).await.unwrap();
    migrating(&f.pool, run.id(), 1).await;
    assert_eq!(f.pool.current_shrink().unwrap().id, run.id());
    f.pool.suspend_shrink().await.unwrap();
    bounded(f.pool.wait_suspended(run.id())).await.unwrap();
    let job = f.pool.disk(disk("d1")).await.unwrap().shrink.unwrap();
    let before = f
        .metadata
        .snapshot(f.pool.id())
        .await
        .unwrap()
        .disks
        .into_iter()
        .find(|d| d.id == disk("d1"))
        .unwrap()
        .allocated
        .len();
    assert!(before > 0 && before < 4);
    f.pool.cancel_shrink().await.unwrap();
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert!(f.pool.current_shrink().is_none());
    let cancelled = f.balancer.find(f.pool.id(), job).await.unwrap().unwrap();
    assert_eq!(
        cancelled.progress().borrow().state,
        EvacuationState::Cancelled
    );
    let current = f
        .metadata
        .snapshot(f.pool.id())
        .await
        .unwrap()
        .disks
        .into_iter()
        .find(|d| d.id == disk("d1"))
        .unwrap();
    assert_eq!(current.allocated.len(), before);
    assert_eq!(current.allocation, AllocationState::Inactive);
    assert_eq!(current.shrink, Some(job));
    let aborted = done(f.pool.abort_shrink_disk(disk("d1")).await.unwrap())
        .await
        .unwrap();
    assert!(aborted.shrink.is_none());
    assert_eq!(aborted.allocation, AllocationState::Inactive);
    assert_eq!(aborted.allocated.len(), before);
    assert_eq!(
        done(f.pool.shrink_disk(disk("d1")).await.unwrap())
            .await
            .unwrap()
            .membership,
        DiskMembership::Removed
    );
    assert_eq!(f.balancer.jobs_started(), 2);
    f.close().await;
}

#[tokio::test]
async fn handoff_rehydrates_domains_and_continues_the_same_task_and_balancing_job() {
    handoff(false, true).await;
}
#[tokio::test]
async fn handoff_preserves_user_suspension_and_resume_does_not_submit_another_execution() {
    handoff(true, true).await;
}
#[tokio::test]
async fn handoff_with_offline_source_waits_then_current_topology_automatically_resumes_it() {
    handoff(false, false).await;
}
async fn handoff(user_paused: bool, source_connected: bool) {
    let tasks = Arc::new(MemoryTaskStore::default());
    let f = Fixture::new(4, true, Some(tasks.clone())).await;
    let mut run = f.pool.shrink_disk(disk("d1")).await.unwrap();
    migrating(&f.pool, run.id(), 1).await;
    let task_id = run.id();
    if user_paused {
        f.pool.suspend_task(task_id).await.unwrap();
        bounded(f.pool.wait_suspended(run.id())).await.unwrap();
    }
    let pool_id = f.pool.id().clone();
    let uuid = f.pool.uuid();
    let job = f.pool.disk(disk("d1")).await.unwrap().shrink.unwrap();
    bounded(f.manager.shutdown()).await.unwrap();
    assert_eq!(bounded(run.result()).await, Err(Failure::Cancelled));
    assert!(f.pool.current_shrink().is_none());
    let job_handle = f.balancer.find(&pool_id, job).await.unwrap().unwrap();
    assert_eq!(
        job_handle.progress().borrow().state,
        EvacuationState::Paused
    );
    let manager = PoolManager::new(
        Config::default(),
        f.metadata.clone(),
        Arc::new(SimulatedDataPlane),
    )
    .with_balancer(f.balancer.clone())
    .unwrap()
    .with_task_store(tasks, PersistenceConfig::default())
    .unwrap();
    let mut create = CreatePool::new(pool_id, PoolName::new("pool").unwrap());
    create.uuid = uuid;
    let topology = (1..=3)
        .map(|i| {
            TopologyEvent::new(
                2,
                10,
                TopologyChange::NodeConnectivity {
                    node: NodeId::new(format!("n{i}")).unwrap(),
                    connected: i != 1 || source_connected,
                },
            )
        })
        .collect();
    let pool = bounded(manager.open(create, topology)).await.unwrap();
    assert_eq!(pool.uuid(), uuid);
    assert_ne!(pool.instance(), f.pool.instance());
    let report = pool.recovery_report().unwrap();
    assert!(report.blocked.is_empty());
    let execution = report.executions[&task_id];
    // Logical references survive host replacement; local execution handles do not.
    assert_eq!(
        pool.task_by_ref(f.pool.task_ref(task_id)).unwrap().id,
        task_id
    );
    if user_paused {
        bounded(pool.wait_suspended(task_id)).await.unwrap();
        pool.flush_task_history().await.unwrap();
        assert_eq!(
            pool.task_detail(task_id).await.unwrap().status,
            TaskStatus::Paused
        );
        assert_eq!(
            job_handle.progress().borrow().state,
            EvacuationState::Paused
        );
        pool.resume_task(task_id).await.unwrap();
        assert_eq!(pool.task(task_id).unwrap().execution.unwrap().id, execution);
    }
    if !source_connected {
        wait_state(&pool, task_id, |s| {
            matches!(s, RunState::WaitingConditions(_))
        })
        .await;
        assert_eq!(
            job_handle.progress().borrow().state,
            EvacuationState::Paused
        );
        assert_eq!(pool.disk(disk("d1")).await.unwrap().io, IoState::Down);
        done(
            pool.online_node(NodeTransitionRequest {
                node: node("n1"),
                sequence: 100,
            })
            .await
            .unwrap(),
        )
        .await
        .unwrap();
    }
    assert!(pool.takeover(vec![]).await.is_err());
    wait_state(&pool, task_id, |s| {
        matches!(s, RunState::Completed | RunState::Failed(_))
    })
    .await;
    assert_eq!(
        pool.task(task_id)
            .unwrap()
            .execution
            .as_ref()
            .unwrap()
            .state,
        RunState::Completed
    );
    pool.flush_task_history().await.unwrap();
    let record = pool.task_detail(task_id).await.unwrap();
    assert_eq!(record.status, TaskStatus::Completed);
    assert_eq!(record.executions.len(), 2);
    assert!(pool.resume_task(task_id).await.is_err());
    assert_eq!(f.balancer.jobs_started(), 1);
    assert_eq!(
        pool.disk(disk("d1")).await.unwrap().membership,
        DiskMembership::Removed
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn takeover_requires_complete_topology_and_continues_after_the_waiter_is_dropped() {
    let tasks = Arc::new(MemoryTaskStore::default());
    let f = Fixture::new(1, false, Some(tasks.clone())).await;
    let mut shrink = f.pool.shrink_disk(disk("d1")).await.unwrap();
    wait_state(&f.pool, shrink.id(), |s| {
        matches!(s, RunState::WaitingConditions(_))
    })
    .await;
    let id = shrink.id();
    let descriptor = f.pool.descriptor();
    f.manager.shutdown().await.unwrap();
    assert_eq!(bounded(shrink.result()).await, Err(Failure::Cancelled));
    let plane = Arc::new(Plane::default());
    let manager = PoolManager::new(Config::default(), f.metadata.clone(), plane.clone())
        .with_balancer(f.balancer.clone())
        .unwrap()
        .with_task_store(tasks, PersistenceConfig::default())
        .unwrap();
    let pool = manager
        .create(CreatePool {
            id: descriptor.id,
            uuid: descriptor.uuid,
            name: descriptor.name,
        })
        .unwrap();
    assert!(pool.takeover(vec![]).await.is_err());
    assert!(pool.recovery_report().is_none());
    assert_eq!(pool.task_detail(id).await.unwrap().executions.len(), 1);
    assert!(pool.shrink_disk(disk("d1")).await.is_err());
    let gate = Gate::new();
    *plane.load.lock().unwrap() = Some(gate.clone());
    let caller = tokio::spawn({
        let pool = pool.clone();
        async move { pool.takeover(topology()).await }
    });
    gate.wait().await;
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    gate.open();
    bounded(async {
        while pool.recovery_report().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let report = pool.recovery_report().unwrap();
    assert!(report.blocked.is_empty());
    wait_state(&pool, id, |s| matches!(s, RunState::WaitingConditions(_))).await;
    pool.flush_task_history().await.unwrap();
    assert_eq!(pool.task_detail(id).await.unwrap().executions.len(), 2);
    assert_eq!(f.balancer.jobs_started(), 0);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn takeover_retains_the_single_shrink_slot_and_rechecks_conditions() {
    let tasks = Arc::new(MemoryTaskStore::default());
    let f = Fixture::new(1, false, Some(tasks.clone())).await;
    let first = f.pool.shrink_disk(disk("d1")).await.unwrap();
    wait_state(&f.pool, first.id(), |s| {
        matches!(s, RunState::WaitingConditions(_))
    })
    .await;
    assert!(f.pool.shrink_disk(disk("d2")).await.is_err());
    let first_id = first.id();
    let descriptor = f.pool.descriptor();
    f.manager.shutdown().await.unwrap();
    let manager = PoolManager::new(
        Config::default(),
        f.metadata.clone(),
        Arc::new(SimulatedDataPlane),
    )
    .with_balancer(f.balancer.clone())
    .unwrap()
    .with_task_store(tasks, PersistenceConfig::default())
    .unwrap();
    let pool = manager
        .open(
            CreatePool {
                id: descriptor.id,
                uuid: descriptor.uuid,
                name: descriptor.name,
            },
            topology().into_iter().take(2).collect(),
        )
        .await
        .unwrap();
    let report = pool.recovery_report().unwrap();
    assert!(report.blocked.is_empty());
    assert_eq!(report.executions.len(), 1);
    assert_eq!(pool.current_shrink().unwrap().id, first_id);
    assert!(pool.shrink_disk(disk("d2")).await.is_err());
    // A newly attached disk wakes the recovered shrink after topology settles.
    add(&pool, 3).await;
    wait_state(&pool, first_id, |s| matches!(s, RunState::Completed)).await;
    assert_eq!(f.balancer.jobs_started(), 1);
    manager.shutdown().await.unwrap();
}

struct Liar;
struct CompletedJob(DiskId);
#[async_trait]
impl EvacuationJob for CompletedJob {
    fn progress(&self) -> tokio::sync::watch::Receiver<EvacuationProgress> {
        tokio::sync::watch::channel(EvacuationProgress {
            disk: self.0.clone(),
            completed: 1,
            total: Some(1),
            state: EvacuationState::Completed,
        })
        .1
    }
    async fn wait(&self) -> Outcome<()> {
        Ok(())
    }
    async fn pause(&self) -> Outcome<()> {
        Ok(())
    }
    async fn resume(&self) -> Outcome<()> {
        Ok(())
    }
    async fn cancel(&self) -> Outcome<()> {
        Ok(())
    }
}
#[async_trait]
impl Balancer for Liar {
    async fn check_shrink(&self, _: &PoolId, _: &DiskId) -> Outcome<ShrinkReadiness> {
        Ok(ShrinkReadiness::Ready)
    }
    async fn evacuate(&self, _: &PoolId, disk: &DiskId, _: EvacuationId) -> Outcome<ArcJob> {
        Ok(Arc::new(CompletedJob(disk.clone())))
    }
    async fn find(&self, _: &PoolId, _: EvacuationId) -> Outcome<Option<ArcJob>> {
        Ok(None)
    }
}
#[tokio::test]
async fn a_successful_balance_reply_cannot_replace_committed_reference_and_allocation_checks() {
    let f = Fixture::new(1, true, None).await;
    f.manager.shutdown().await.unwrap();
    let manager = PoolManager::new(
        Config::default(),
        f.metadata.clone(),
        Arc::new(SimulatedDataPlane),
    )
    .with_balancer(Arc::new(Liar))
    .unwrap();
    let pool = manager
        .open(
            CreatePool::new(PoolId::new("pool").unwrap(), PoolName::new("pool").unwrap()),
            topology(),
        )
        .await
        .unwrap();
    assert!(matches!(
        done(pool.shrink_disk(disk("d1")).await.unwrap()).await,
        Err(Failure::Rejected(_))
    ));
    assert_eq!(
        pool.disk(disk("d1")).await.unwrap().membership,
        DiskMembership::Member
    );
    assert!(
        !f.metadata
            .committed()
            .iter()
            .any(|(_, c)| matches!(c, MetadataChange::MarkRemoved { .. }))
    );
    manager.shutdown().await.unwrap();
}

struct Gate {
    entered: Semaphore,
    release: Semaphore,
}
impl Gate {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
        })
    }
    async fn wait(&self) {
        bounded(self.entered.acquire()).await.unwrap().forget();
    }
    async fn block(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
    fn open(&self) {
        self.release.add_permits(1);
    }
}
#[derive(Default)]
struct Plane {
    load: Mutex<Option<Arc<Gate>>>,
    down: Mutex<Option<Arc<Gate>>>,
}
#[async_trait]
impl DataPlane for Plane {
    async fn load_pool(&self, _: &PoolId, _: &NodeId, _: &CancellationToken) -> Outcome<()> {
        let gate = self.load.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.block().await;
        }
        Ok(())
    }
    async fn open_disk(&self, _: &PoolId, _: &DiskId, _: &CancellationToken) -> Outcome<()> {
        Ok(())
    }
    async fn set_disk_io(
        &self,
        _: &PoolId,
        id: &DiskId,
        io: IoState,
        _: &CancellationToken,
    ) -> Outcome<()> {
        if id == &disk("d1") && io == IoState::Down {
            let gate = self.down.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.block().await;
            }
        }
        // Models an issued, non-interruptible successful IO. Its real result wins cancel.
        Ok(())
    }
    async fn close_disk(&self, _: &PoolId, _: &DiskId, _: &CancellationToken) -> Outcome<()> {
        Ok(())
    }
    async fn initialize_bg(
        &self,
        _: &PoolId,
        _: &VolumeId,
        _: &[BgEntry],
        _: &CancellationToken,
    ) -> Outcome<()> {
        Ok(())
    }
}
async fn reopened(
    f: &Fixture,
    metadata: Arc<dyn MetadataStore>,
    plane: Arc<dyn DataPlane>,
) -> (PoolManager, PoolHandle) {
    f.manager.shutdown().await.unwrap();
    let manager = PoolManager::new(Config::default(), metadata, plane)
        .with_balancer(f.balancer.clone())
        .unwrap();
    let pool = manager
        .open(
            CreatePool::new(PoolId::new("pool").unwrap(), PoolName::new("pool").unwrap()),
            topology(),
        )
        .await
        .unwrap();
    (manager, pool)
}

#[tokio::test]
async fn real_topology_work_pauses_the_same_evacuation_activity_and_job_until_completion() {
    let f = Fixture::new(4, true, None).await;
    let plane = Arc::new(Plane::default());
    let (manager, pool) = reopened(&f, f.metadata.clone(), plane.clone()).await;
    let mut shrink = pool.shrink_disk(disk("d1")).await.unwrap();
    migrating(&pool, shrink.id(), 1).await;
    let gate = Gate::new();
    *plane.load.lock().unwrap() = Some(gate.clone());
    let mut topo = pool.load_node(node("n2")).await.unwrap();
    gate.wait().await;
    assert_eq!(
        pool.task(shrink.id())
            .unwrap()
            .execution
            .as_ref()
            .unwrap()
            .state,
        RunState::Paused
    );
    let activity = pool
        .task(shrink.id())
        .unwrap()
        .execution
        .as_ref()
        .unwrap()
        .activities
        .entries
        .iter()
        .find(|a| a.name == "balance.evacuate-whole-disk")
        .unwrap()
        .id;
    let job_id = pool.disk(disk("d1")).await.unwrap().shrink.unwrap();
    let job = f.balancer.find(pool.id(), job_id).await.unwrap().unwrap();
    assert_eq!(job.progress().borrow().state, EvacuationState::Paused);
    gate.open();
    bounded(topo.result()).await.unwrap();
    assert_eq!(
        bounded(shrink.result()).await.unwrap().membership,
        DiskMembership::Removed
    );
    assert_eq!(f.balancer.jobs_started(), 1);
    assert_eq!(
        pool.task(shrink.id())
            .unwrap()
            .execution
            .as_ref()
            .unwrap()
            .activities
            .entries
            .iter()
            .find(|a| a.name == "balance.evacuate-whole-disk")
            .unwrap()
            .id,
        activity
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn irreversible_finalization_finishes_before_topology_and_committed_removal_wins_cancel() {
    let f = Fixture::new(0, true, None).await;
    let plane = Arc::new(Plane::default());
    let (manager, pool) = reopened(&f, f.metadata.clone(), plane.clone()).await;
    let gate = Gate::new();
    *plane.down.lock().unwrap() = Some(gate.clone());
    let mut shrink = pool.shrink_disk(disk("d1")).await.unwrap();
    gate.wait().await;
    let mut topo = pool.load_node(node("n2")).await.unwrap();
    assert_eq!(topo.control.state(), RunState::WaitingObjects);
    pool.cancel(shrink.id()).unwrap();
    assert!(futures_util::poll!(Box::pin(shrink.result())).is_pending());
    gate.open();
    assert_eq!(
        bounded(shrink.result()).await.unwrap().membership,
        DiskMembership::Removed
    );
    bounded(topo.result()).await.unwrap();
    manager.shutdown().await.unwrap();
}

#[derive(Clone, Copy)]
enum FaultPoint {
    Intent,
    Removed,
}
struct FaultStore {
    inner: Arc<MemoryMetadata>,
    fault: Mutex<Option<FaultPoint>>,
}
#[async_trait]
impl MetadataStore for FaultStore {
    async fn snapshot(&self, pool: &PoolId) -> Outcome<MetadataSnapshot> {
        self.inner.snapshot(pool).await
    }
    async fn commit(&self, pool: &PoolId, change: MetadataChange) -> Outcome<()> {
        let fail = {
            let mut next = self.fault.lock().unwrap();
            let matches = matches!(
                (&*next, &change),
                (
                    Some(FaultPoint::Intent),
                    MetadataChange::ShrinkIntent { .. }
                ) | (
                    Some(FaultPoint::Removed),
                    MetadataChange::MarkRemoved { .. }
                )
            );
            if matches {
                next.take();
            }
            matches
        };
        if fail {
            return Err(Failure::Failed("injected commit rejection".into()));
        }
        self.inner.commit(pool, change).await
    }
}
#[tokio::test]
async fn failed_intent_commit_never_disables_allocation_or_starts_a_job() {
    let f = Fixture::new(1, true, None).await;
    let store = Arc::new(FaultStore {
        inner: f.metadata.clone(),
        fault: Mutex::new(Some(FaultPoint::Intent)),
    });
    let (manager, pool) = reopened(&f, store, Arc::new(Plane::default())).await;
    assert!(matches!(
        done(pool.shrink_disk(disk("d1")).await.unwrap()).await,
        Err(Failure::Failed(_))
    ));
    let source = pool.disk(disk("d1")).await.unwrap();
    assert!(source.shrink.is_none() && source.allocatable());
    assert_eq!(f.balancer.jobs_started(), 0);
    manager.shutdown().await.unwrap();
}
#[tokio::test]
async fn unconfirmed_removed_commit_retains_the_member_and_fences_conflicting_topology() {
    let f = Fixture::new(0, true, None).await;
    let store = Arc::new(FaultStore {
        inner: f.metadata.clone(),
        fault: Mutex::new(Some(FaultPoint::Removed)),
    });
    let (manager, pool) = reopened(&f, store, Arc::new(Plane::default())).await;
    assert!(matches!(
        done(pool.shrink_disk(disk("d1")).await.unwrap()).await,
        Err(Failure::Uncertain(_))
    ));
    let source = pool.disk(disk("d1")).await.unwrap();
    assert_eq!(source.membership, DiskMembership::Member);
    assert_eq!(source.io, IoState::Down);
    assert!(matches!(
        pool.load_node(node("n2")).await,
        Err(Failure::Rejected(_))
    ));
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn logical_unload_stops_balancing_and_load_rechecks_before_resuming_it() {
    let f = Fixture::new(4, true, None).await;
    let mut shrink = f.pool.shrink_disk(disk("d1")).await.unwrap();
    migrating(&f.pool, shrink.id(), 1).await;
    let id = f.pool.disk(disk("d1")).await.unwrap().shrink.unwrap();
    bounded(f.pool.unload()).await.unwrap();
    assert_eq!(f.pool.descriptor().lifecycle, PoolLifecycle::Unloaded);
    let job = f.balancer.find(f.pool.id(), id).await.unwrap().unwrap();
    assert_eq!(job.progress().borrow().state, EvacuationState::Paused);
    let snapshot = (1..=3)
        .map(|i| {
            TopologyEvent::new(
                1,
                100,
                TopologyChange::NodeConnectivity {
                    node: node(&format!("n{i}")),
                    connected: true,
                },
            )
        })
        .collect();
    bounded(f.pool.load(snapshot)).await.unwrap();
    assert_eq!(
        bounded(shrink.result()).await.unwrap().membership,
        DiskMembership::Removed
    );
    assert_eq!(f.balancer.jobs_started(), 1);
    f.close().await;
}

#[tokio::test]
async fn source_down_keeps_shrink_waiting_until_source_online_revalidates_conditions() {
    let f = Fixture::new(4, true, None).await;
    let mut shrink = f.pool.shrink_disk(disk("d1")).await.unwrap();
    migrating(&f.pool, shrink.id(), 1).await;
    // The existing offline workflow has an explicit unimplemented rebuild boundary;
    // its confirmed Down still changes the facts that the shrink guard must inspect.
    assert!(matches!(
        done(f.pool.offline_disk(disk("d1")).await.unwrap()).await,
        Err(Failure::Failed(_))
    ));
    wait_state(&f.pool, shrink.id(), |s| {
        matches!(s, RunState::WaitingConditions(_))
    })
    .await;
    assert_eq!(f.pool.disk(disk("d1")).await.unwrap().io, IoState::Down);
    done(f.pool.online_disk(disk("d1")).await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        bounded(shrink.result()).await.unwrap().membership,
        DiskMembership::Removed
    );
    assert_eq!(f.balancer.jobs_started(), 1);
    f.close().await;
}
