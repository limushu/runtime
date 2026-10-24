use async_trait::async_trait;
use std::{
    future::Future,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use storage_pool::{adapters::*, ports::*, *};
use tokio::{sync::Semaphore, time::Instant};
use tokio_util::sync::CancellationToken;

const LIMIT: Duration = Duration::from_secs(3);
fn id(s: &str) -> PoolId {
    PoolId::new(s).unwrap()
}
fn name(s: &str) -> PoolName {
    PoolName::new(s).unwrap()
}
fn node(s: &str) -> NodeId {
    NodeId::new(s).unwrap()
}
fn disk(s: &str) -> DiskId {
    DiskId::new(s).unwrap()
}
fn tier() -> TierId {
    TierId::new("ssd").unwrap()
}
fn volume(s: &str) -> VolumeId {
    VolumeId::new(s).unwrap()
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(LIMIT, future)
        .await
        .expect("operation timed out")
}
async fn done<T>(mut run: Run<T>) -> Outcome<T> {
    bounded(run.result()).await
}
fn manager() -> PoolManager {
    PoolManager::new(
        Config::default(),
        Arc::new(MemoryMetadata::default()),
        Arc::new(SimulatedDataPlane),
    )
}
fn event(sequence: u64, connected: bool) -> TopologyEvent {
    TopologyEvent::new(
        1,
        sequence,
        TopologyChange::NodeConnectivity {
            node: node("shared"),
            connected,
        },
    )
}
async fn member(pool: &PoolHandle) {
    done(pool.attach_node(node("shared"), 0, true).await.unwrap())
        .await
        .unwrap();
}
async fn storage(pool: &PoolHandle) {
    done(pool.create_tier(tier()).await.unwrap()).await.unwrap();
    for index in 1..=2 {
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
                tier: tier(),
                capacity_blks: 2,
            })
            .await
            .unwrap(),
        )
        .await
        .unwrap();
    }
}
fn create_volume(s: &str) -> CreateVolume {
    CreateVolume {
        id: volume(s),
        disks: vec![disk("d1"), disk("d2")],
    }
}

#[tokio::test]
async fn three_indexes_rename_and_name_reuse_keep_handles_bound() {
    let manager = manager();
    let pool = manager
        .create(CreatePool::new(id("id-1"), name("alpha")))
        .unwrap();
    assert_eq!(manager.get(id("id-1")).unwrap().uuid(), pool.uuid());
    assert_eq!(manager.get(pool.uuid()).unwrap().id(), pool.id());
    assert_eq!(manager.get(name("alpha")).unwrap().uuid(), pool.uuid());
    let request = CreatePool {
        id: id("id-2"),
        uuid: pool.uuid(),
        name: name("beta"),
    };
    assert!(manager.create(request).is_err());
    assert!(
        manager
            .create(CreatePool::new(id("id-2"), name("alpha")))
            .is_err()
    );
    let renamed = done(pool.rename(name("beta"), 0).await.unwrap())
        .await
        .unwrap();
    assert_eq!(renamed.revision, 1);
    assert!(manager.get(name("alpha")).is_err());
    assert_eq!(manager.get(name("beta")).unwrap().uuid(), pool.uuid());
    assert!(
        done(pool.rename(name("gamma"), 0).await.unwrap())
            .await
            .is_err()
    );
    let old_task = pool.task_ref(pool.tasks()[0].id);
    bounded(pool.delete(1)).await.unwrap();
    assert_eq!(pool.descriptor().lifecycle, PoolLifecycle::Deleted);
    assert!(manager.get(name("beta")).is_err());
    assert!(manager.get(pool.uuid()).is_err());
    assert!(manager.task(old_task).is_ok());
    assert!(pool.create_tier(tier()).await.is_err());
    let replacement = manager
        .create(CreatePool::new(id("id-2"), name("beta")))
        .unwrap();
    assert_ne!(pool.uuid(), replacement.uuid());
    assert!(
        manager
            .create(CreatePool::new(id("id-1"), name("unused")))
            .is_err()
    );
    assert_eq!(pool.descriptor().lifecycle, PoolLifecycle::Deleted);
    manager.shutdown().await.unwrap();
}

#[test]
fn identity_validation_is_explicit() {
    assert!("not-a-uuid".parse::<PoolUuid>().is_err());
    assert!(
        "00000000-0000-0000-0000-000000000000"
            .parse::<PoolUuid>()
            .is_err()
    );
    let uuid = PoolUuid::new();
    assert_eq!(uuid.to_string().parse::<PoolUuid>().unwrap(), uuid);
    for invalid in ["", " a", "a ", "a\nb", "a\0b"] {
        assert!(PoolName::new(invalid).is_err());
    }
    assert_ne!(name("Pool"), name("pool"));
    assert!(PoolName::new("存储池").is_ok());
}

#[tokio::test]
async fn concurrent_rename_checks_revision_and_indexes_atomically() {
    let manager = manager();
    let pool = manager.create_pool(id("a")).unwrap();
    let (a, b) = tokio::join!(pool.rename(name("x"), 0), pool.rename(name("y"), 0));
    let mut successes = 0;
    for run in [a, b].into_iter().flatten() {
        if done(run).await.is_ok() {
            successes += 1;
        }
    }
    assert_eq!(successes, 1);
    assert_eq!(
        manager.get(pool.descriptor().name).unwrap().uuid(),
        pool.uuid()
    );
    assert!(manager.get(name("a")).is_err());
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn fanout_is_pool_local_deduplicated_and_order_rotates() {
    let manager = manager();
    let a = manager.create_pool(id("a")).unwrap();
    let b = manager.create_pool(id("b")).unwrap();
    let unrelated = manager.create_pool(id("c")).unwrap();
    member(&a).await;
    member(&b).await;
    let notification = event(2, false);
    let receipt = manager.dispatch(notification.clone()).await.unwrap();
    let report = manager
        .gather(&receipt, WaitFor::Completion, Instant::now() + LIMIT)
        .await;
    assert_eq!(report.outcome, FanoutOutcome::Succeeded);
    assert_eq!(
        report
            .targets
            .iter()
            .filter(|t| matches!(t.status, TargetStatus::Completed(_)))
            .count(),
        2
    );
    assert_eq!(unrelated.tasks().len(), 0);
    assert!(
        report
            .targets
            .iter()
            .any(|t| t.pool == unrelated.uuid() && t.status == TargetStatus::NotRelevant)
    );
    let count = manager.tasks().len();
    let duplicate = manager.dispatch(notification.clone()).await.unwrap();
    assert_eq!(
        duplicate
            .gather(WaitFor::Completion, Instant::now() + LIMIT)
            .await
            .outcome,
        FanoutOutcome::Succeeded
    );
    assert_eq!(manager.tasks().len(), count);
    assert_ne!(receipt.targets[0].pool, duplicate.targets[0].pool);
    let mut conflicting = notification;
    conflicting.change = TopologyChange::NodeConnectivity {
        node: node("shared"),
        connected: true,
    };
    let report = manager
        .dispatch(conflicting)
        .await
        .unwrap()
        .gather(WaitFor::Completion, Instant::now() + LIMIT)
        .await;
    assert_eq!(report.outcome, FanoutOutcome::Partial);
    let report = manager
        .dispatch(event(1, true))
        .await
        .unwrap()
        .gather(WaitFor::Completion, Instant::now() + LIMIT)
        .await;
    assert_eq!(report.outcome, FanoutOutcome::Partial);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn full_mailbox_does_not_block_other_pool_and_retry_skips_completed_target() {
    let manager = PoolManager::new(
        Config {
            capacity: 1,
            ..Config::default()
        },
        Arc::new(MemoryMetadata::default()),
        Arc::new(SimulatedDataPlane),
    );
    let a = manager.create_pool(id("a")).unwrap();
    let b = manager.create_pool(id("b")).unwrap();
    member(&a).await;
    member(&b).await;
    // No await/yield between these offers: only A's mailbox is deliberately filled.
    let first = a.try_notify(Arc::new(event(2, false)));
    let receipt = manager.dispatch(event(3, true)).await.unwrap();
    assert!(
        receipt
            .targets
            .iter()
            .any(|t| t.pool == a.uuid() && t.snapshot().status == TargetStatus::Backpressured)
    );
    let report = receipt
        .gather(WaitFor::Completion, Instant::now() + LIMIT)
        .await;
    assert_eq!(report.outcome, FanoutOutcome::Partial);
    assert!(
        report
            .targets
            .iter()
            .any(|t| t.pool == b.uuid() && matches!(t.status, TargetStatus::Completed(_)))
    );
    bounded(async {
        while matches!(
            first.snapshot().status,
            TargetStatus::Queued | TargetStatus::Accepted(_)
        ) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let b_count = b.tasks().len();
    let retry = manager.retry(&receipt).await;
    assert_eq!(
        retry
            .gather(WaitFor::Completion, Instant::now() + LIMIT)
            .await
            .outcome,
        FanoutOutcome::Succeeded
    );
    assert_eq!(b.tasks().len(), b_count);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn zero_relevant_targets_is_explicit() {
    let manager = manager();
    manager.create_pool(id("empty")).unwrap();
    let report = manager
        .dispatch(event(1, false))
        .await
        .unwrap()
        .gather(WaitFor::Completion, Instant::now() + LIMIT)
        .await;
    assert_eq!(report.outcome, FanoutOutcome::NoRelevantTargets);
    assert_eq!(manager.tasks().len(), 0);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn children_delete_in_dependency_order_and_old_release_cannot_free_new_volume() {
    let manager = manager();
    let pool = manager.create_pool(id("a")).unwrap();
    storage(&pool).await;
    done(pool.create_volume(create_volume("v1")).await.unwrap())
        .await
        .unwrap();
    assert!(pool.detach_node(node("n1")).await.is_err());
    assert!(pool.delete_tier(tier()).await.is_err());
    assert!(
        done(pool.remove_disk(disk("d1")).await.unwrap())
            .await
            .is_err()
    );
    assert!(bounded(pool.delete(0)).await.is_err());
    assert_eq!(pool.descriptor().lifecycle, PoolLifecycle::Active);
    done(pool.delete_volume(volume("v1")).await.unwrap())
        .await
        .unwrap();
    assert!(pool.volumes().await.unwrap().is_empty());
    assert!(
        pool.disks()
            .await
            .unwrap()
            .iter()
            .all(|disk| disk.allocated.is_empty())
    );
    done(pool.create_volume(create_volume("v2")).await.unwrap())
        .await
        .unwrap();
    done(pool.delete_volume(volume("v1")).await.unwrap())
        .await
        .unwrap();
    assert!(
        pool.disks()
            .await
            .unwrap()
            .iter()
            .all(|disk| disk.allocated.len() == 1)
    );
    assert!(
        done(pool.create_volume(create_volume("v1")).await.unwrap())
            .await
            .is_err()
    );
    done(pool.delete_volume(volume("v2")).await.unwrap())
        .await
        .unwrap();
    for index in 1..=2 {
        done(pool.remove_disk(disk(&format!("d{index}"))).await.unwrap())
            .await
            .unwrap();
    }
    done(pool.delete_tier(tier()).await.unwrap()).await.unwrap();
    for index in 1..=2 {
        done(pool.detach_node(node(&format!("n{index}"))).await.unwrap())
            .await
            .unwrap();
    }
    bounded(pool.delete(0)).await.unwrap();
    assert_eq!(pool.descriptor().lifecycle, PoolLifecycle::Deleted);
    assert!(pool.tasks().iter().any(|task| {
        task.name == "storage.delete-volume"
            && task
                .activities
                .entries
                .iter()
                .any(|activity| activity.name == "disk.release-allocation")
    }));
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn independent_pool_handles_cannot_claim_the_same_physical_disk() {
    let manager = manager();
    let a = manager.create_pool(id("a")).unwrap();
    let b = manager.clone().create_pool(id("b")).unwrap();
    for pool in [&a, &b] {
        member(pool).await;
        done(pool.load_node(node("shared")).await.unwrap())
            .await
            .unwrap();
        done(pool.create_tier(tier()).await.unwrap()).await.unwrap();
    }
    let spec = DiskSpec {
        id: disk("physical"),
        node: node("shared"),
        tier: tier(),
        capacity_blks: 2,
    };
    let (first, second) = tokio::join!(a.add_disk(spec.clone()), b.add_disk(spec));
    assert_ne!(first.is_ok(), second.is_ok());
    for run in [first, second].into_iter().flatten() {
        done(run).await.unwrap();
    }
    assert_eq!(
        a.disks().await.unwrap().len() + b.disks().await.unwrap().len(),
        1
    );
    manager.shutdown().await.unwrap();
}

struct GatePlane {
    entered: Semaphore,
    release: Semaphore,
    deletes: AtomicUsize,
    delete_failure: Mutex<Option<Failure>>,
}
impl Default for GatePlane {
    fn default() -> Self {
        Self {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            deletes: AtomicUsize::new(0),
            delete_failure: Mutex::default(),
        }
    }
}
#[async_trait]
impl DataPlane for GatePlane {
    async fn unload_pool(&self, _: &PoolId, _: &NodeId, _: &CancellationToken) -> Outcome<()> {
        Ok(())
    }
    async fn close_disk(&self, _: &PoolId, _: &DiskId, _: &CancellationToken) -> Outcome<()> {
        Ok(())
    }
    async fn load_pool(&self, pool: &PoolId, _: &NodeId, _: &CancellationToken) -> Outcome<()> {
        if pool.as_str() == "slow" {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        Ok(())
    }
    async fn open_disk(&self, _: &PoolId, _: &DiskId, _: &CancellationToken) -> Outcome<()> {
        Ok(())
    }
    async fn set_disk_io(
        &self,
        _: &PoolId,
        _: &DiskId,
        _: IoState,
        _: &CancellationToken,
    ) -> Outcome<()> {
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
    async fn delete_volume(&self, _: &PoolId, _: &VolumeId, _: &CancellationToken) -> Outcome<()> {
        self.deletes.fetch_add(1, Ordering::SeqCst);
        self.delete_failure
            .lock()
            .unwrap()
            .take()
            .map_or(Ok(()), Err)
    }
}

#[tokio::test]
async fn gather_timeout_keeps_accepted_work_alive_and_reports_each_target() {
    let plane = Arc::new(GatePlane::default());
    let manager = PoolManager::new(
        Config::default(),
        Arc::new(MemoryMetadata::default()),
        plane.clone(),
    );
    let slow = manager.create_pool(id("slow")).unwrap();
    let fast = manager.create_pool(id("fast")).unwrap();
    member(&slow).await;
    member(&fast).await;
    let loading = slow.load_node(node("shared")).await.unwrap();
    bounded(plane.entered.acquire()).await.unwrap().forget();
    let receipt = manager.dispatch(event(2, false)).await.unwrap();
    let decision = receipt
        .gather(WaitFor::Decision, Instant::now() + LIMIT)
        .await;
    assert_eq!(decision.outcome, FanoutOutcome::Succeeded);
    let completion = receipt
        .gather(
            WaitFor::Completion,
            Instant::now() + Duration::from_millis(10),
        )
        .await;
    assert_eq!(completion.outcome, FanoutOutcome::TimedOut);
    assert!(
        completion
            .targets
            .iter()
            .any(|t| t.pool == fast.uuid() && matches!(t.status, TargetStatus::Completed(_)))
    );
    assert!(
        completion
            .targets
            .iter()
            .any(|t| t.pool == slow.uuid() && matches!(t.status, TargetStatus::Accepted(_)))
    );
    plane.release.add_permits(1);
    done(loading).await.unwrap();
    assert_eq!(
        receipt
            .gather(WaitFor::Completion, Instant::now() + LIMIT)
            .await
            .outcome,
        FanoutOutcome::Succeeded
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn dropping_delete_wait_does_not_abandon_pool_owned_finalizer() {
    let manager = manager();
    let pool = manager.create_pool(id("a")).unwrap();
    {
        let deleting = pool.delete(0);
        tokio::pin!(deleting);
        assert!(futures_util::poll!(&mut deleting).is_pending());
    }
    bounded(async {
        while pool.descriptor().lifecycle != PoolLifecycle::Deleted {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(manager.get(id("a")).is_err());
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn task_references_are_scoped_by_uuid_and_instance() {
    let manager = manager();
    let a = manager.create_pool(id("a")).unwrap();
    let b = manager.create_pool(id("b")).unwrap();
    let first = a.create_tier(tier()).await.unwrap();
    let second = b.create_tier(tier()).await.unwrap();
    assert_ne!(first.id(), second.id());
    let task = a.task_ref(first.id());
    done(first).await.unwrap();
    done(second).await.unwrap();
    assert_eq!(manager.task(task).unwrap().pool, "a");
    assert!(b.task_by_ref(task).is_err());
    assert!(
        manager
            .task(TaskRef {
                instance: PoolUuid::new(),
                ..task
            })
            .is_err()
    );
    manager.shutdown().await.unwrap();
}

struct ReleaseFailure {
    inner: MemoryMetadata,
    fail: AtomicUsize,
}
#[async_trait]
impl MetadataStore for ReleaseFailure {
    async fn commit(&self, pool: &PoolId, change: MetadataChange) -> Outcome<()> {
        if matches!(change, MetadataChange::ReleaseAllocation { .. })
            && self.fail.fetch_add(1, Ordering::SeqCst) == 1
        {
            return Err(Failure::Failed("known release failure".into()));
        }
        self.inner.commit(pool, change).await
    }
}
#[tokio::test]
async fn retry_known_cleanup_failure_keeps_committed_deletion_and_releases_only_remaining_allocations()
 {
    let store = Arc::new(ReleaseFailure {
        inner: MemoryMetadata::default(),
        fail: AtomicUsize::new(0),
    });
    let manager = PoolManager::new(
        Config::default(),
        store.clone(),
        Arc::new(SimulatedDataPlane),
    );
    let pool = manager.create_pool(id("a")).unwrap();
    storage(&pool).await;
    done(pool.create_volume(create_volume("v")).await.unwrap())
        .await
        .unwrap();
    assert!(matches!(
        done(pool.delete_volume(volume("v")).await.unwrap()).await,
        Err(Failure::Failed(_))
    ));
    assert!(pool.volumes().await.unwrap().is_empty());
    assert_eq!(
        pool.disks()
            .await
            .unwrap()
            .iter()
            .map(|d| d.allocated.len())
            .sum::<usize>(),
        1
    );
    done(pool.delete_volume(volume("v")).await.unwrap())
        .await
        .unwrap();
    assert!(
        pool.disks()
            .await
            .unwrap()
            .iter()
            .all(|d| d.allocated.is_empty())
    );
    assert_eq!(
        store
            .inner
            .committed()
            .iter()
            .filter(|(_, change)| matches!(change, MetadataChange::DeleteVolume { .. }))
            .count(),
        1
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn unload_keeps_handles_and_load_requires_complete_fresh_snapshot() {
    let manager = manager();
    let pool = manager.create_pool(id("a")).unwrap();
    storage(&pool).await;
    let run = pool.create_volume(create_volume("v1")).await.unwrap();
    let reference = pool.task_ref(run.id());
    done(run).await.unwrap();
    bounded(pool.unload()).await.unwrap();
    assert_eq!(pool.descriptor().lifecycle, PoolLifecycle::Unloaded);
    assert!(pool.nodes().await.unwrap().iter().all(|n| !n.loaded));
    assert!(pool.create_volume(create_volume("v2")).await.is_err());
    let notification = TopologyEvent::new(
        2,
        1,
        TopologyChange::NodeConnectivity {
            node: node("n1"),
            connected: true,
        },
    );
    let receipt = manager.dispatch(notification).await.unwrap();
    assert_eq!(
        receipt
            .gather(WaitFor::Completion, Instant::now() + LIMIT)
            .await
            .outcome,
        FanoutOutcome::Partial
    );
    assert!(pool.load(vec![]).await.is_err());
    let snapshot: Vec<_> = (1..=2)
        .map(|i| {
            TopologyEvent::new(
                2,
                1,
                TopologyChange::NodeConnectivity {
                    node: node(&format!("n{i}")),
                    connected: true,
                },
            )
        })
        .collect();
    bounded(pool.load(snapshot.clone())).await.unwrap();
    assert_eq!(pool.descriptor().lifecycle, PoolLifecycle::Active);
    assert!(pool.nodes().await.unwrap().iter().all(PoolNode::serving));
    assert!(manager.task(reference).is_ok());
    assert_eq!(
        manager
            .dispatch(snapshot[0].clone())
            .await
            .unwrap()
            .gather(WaitFor::Completion, Instant::now() + LIMIT)
            .await
            .outcome,
        FanoutOutcome::Succeeded
    );
    assert_eq!(pool.volumes().await.unwrap().len(), 1);
    done(pool.create_volume(create_volume("v2")).await.unwrap())
        .await
        .unwrap();
    assert!(pool.tasks().iter().any(|task| {
        task.name == "storage.unload-pool"
            && task
                .activities
                .entries
                .iter()
                .any(|a| a.name == "node.unload")
    }));
    assert!(pool.tasks().iter().any(|task| {
        task.name == "storage.load-pool"
            && task
                .activities
                .entries
                .iter()
                .any(|a| a.name == "disk.open")
    }));
    manager.shutdown().await.unwrap();
    assert!(pool.load(snapshot).await.is_err());
}

#[tokio::test]
async fn empty_logically_unloaded_pool_can_be_deleted_without_loading() {
    let manager = manager();
    let pool = manager.create_pool(id("a")).unwrap();
    pool.unload().await.unwrap();
    pool.delete(0).await.unwrap();
    assert_eq!(pool.descriptor().lifecycle, PoolLifecycle::Deleted);
}

#[tokio::test]
async fn cancelling_queued_detach_releases_its_gate() {
    let plane = Arc::new(GatePlane::default());
    let manager = PoolManager::new(
        Config::default(),
        Arc::new(MemoryMetadata::default()),
        plane.clone(),
    );
    let pool = manager.create_pool(id("slow")).unwrap();
    member(&pool).await;
    let loading = pool.load_node(node("shared")).await.unwrap();
    bounded(plane.entered.acquire()).await.unwrap().forget();
    let removing = pool.detach_node(node("shared")).await.unwrap();
    assert_eq!(
        removing.control.state(),
        workflow_foundation::RunState::WaitingObjects
    );
    pool.cancel(removing.id()).unwrap();
    assert_eq!(done(removing).await, Err(Failure::Cancelled));
    plane.release.add_permits(1);
    done(loading).await.unwrap();
    done(pool.detach_node(node("shared")).await.unwrap())
        .await
        .unwrap();
    assert!(pool.nodes().await.unwrap().is_empty());
    pool.delete(0).await.unwrap();
}

#[tokio::test]
async fn rejected_node_creation_does_not_leave_a_phantom_member() {
    let plane = Arc::new(GatePlane::default());
    let manager = PoolManager::new(
        Config {
            max_workflows: 1,
            ..Config::default()
        },
        Arc::new(MemoryMetadata::default()),
        plane.clone(),
    );
    let pool = manager.create_pool(id("slow")).unwrap();
    member(&pool).await;
    let loading = pool.load_node(node("shared")).await.unwrap();
    bounded(plane.entered.acquire()).await.unwrap().forget();
    assert!(matches!(
        pool.attach_node(node("rejected"), 1, true).await,
        Err(Failure::Busy)
    ));
    let receipt = manager
        .dispatch(TopologyEvent::new(
            1,
            2,
            TopologyChange::NodeConnectivity {
                node: node("rejected"),
                connected: false,
            },
        ))
        .await
        .unwrap();
    assert_eq!(
        receipt
            .gather(WaitFor::Completion, Instant::now() + LIMIT)
            .await
            .outcome,
        FanoutOutcome::NoRelevantTargets
    );
    plane.release.add_permits(1);
    done(loading).await.unwrap();
    done(pool.detach_node(node("shared")).await.unwrap())
        .await
        .unwrap();
    pool.delete(0).await.unwrap();
}

#[tokio::test]
async fn known_pre_add_failure_releases_only_uncommitted_ownership() {
    let manager = manager();
    let a = manager.create_pool(id("a")).unwrap();
    let b = manager.create_pool(id("b")).unwrap();
    member(&a).await;
    done(a.load_node(node("shared")).await.unwrap())
        .await
        .unwrap();
    let spec = DiskSpec {
        id: disk("physical"),
        node: node("shared"),
        tier: tier(),
        capacity_blks: 2,
    };
    // A has no Tier, so the first activity fails before Add could have any effect.
    assert!(done(a.add_disk(spec.clone()).await.unwrap()).await.is_err());
    done(a.detach_node(node("shared")).await.unwrap())
        .await
        .unwrap();
    a.delete(0).await.unwrap();
    member(&b).await;
    done(b.load_node(node("shared")).await.unwrap())
        .await
        .unwrap();
    done(b.create_tier(tier()).await.unwrap()).await.unwrap();
    done(b.add_disk(spec).await.unwrap()).await.unwrap();
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn known_and_ambiguous_volume_deletion_failures_do_not_free_allocations() {
    for failure in [
        Failure::Failed("not stopped".into()),
        Failure::Uncertain("stop result unknown".into()),
    ] {
        let plane = Arc::new(GatePlane::default());
        let manager = PoolManager::new(
            Config::default(),
            Arc::new(MemoryMetadata::default()),
            plane.clone(),
        );
        let pool = manager.create_pool(id("a")).unwrap();
        storage(&pool).await;
        done(pool.create_volume(create_volume("v")).await.unwrap())
            .await
            .unwrap();
        *plane.delete_failure.lock().unwrap() = Some(failure.clone());
        let result = done(pool.delete_volume(volume("v")).await.unwrap()).await;
        if matches!(failure, Failure::Uncertain(_)) {
            assert!(matches!(result, Err(Failure::Uncertain(_))));
        } else {
            assert_eq!(result, Err(failure.clone()));
        }
        assert_eq!(pool.volumes().await.unwrap().len(), 1);
        assert!(
            pool.disks()
                .await
                .unwrap()
                .iter()
                .all(|d| d.allocated.len() == 1)
        );
        if matches!(failure, Failure::Uncertain(_)) {
            assert!(
                done(pool.create_volume(create_volume("later")).await.unwrap())
                    .await
                    .is_err()
            );
            assert!(pool.delete(0).await.is_err());
        } else {
            done(pool.delete_volume(volume("v")).await.unwrap())
                .await
                .unwrap();
        }
        manager.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn deleted_pool_histories_are_bounded_but_existing_observers_survive() {
    let manager = PoolManager::new(
        Config {
            task_history: 1,
            ..Config::default()
        },
        Arc::new(MemoryMetadata::default()),
        Arc::new(SimulatedDataPlane),
    );
    let a = manager.create_pool(id("a")).unwrap();
    a.delete(0).await.unwrap();
    let task = a
        .tasks()
        .into_iter()
        .find(|task| task.name == "storage.delete-pool")
        .unwrap();
    let reference = a.task_ref(task.id);
    let observer = manager.observe_task(reference).unwrap();
    let b = manager.create_pool(id("b")).unwrap();
    b.delete(0).await.unwrap();
    assert!(manager.task(reference).is_err());
    assert_eq!(
        observer.snapshot().state,
        workflow_foundation::RunState::Completed
    );
    assert!(manager.create_pool(id("a")).is_err());
    assert_eq!(
        manager
            .list()
            .iter()
            .filter(|d| d.lifecycle == PoolLifecycle::Deleted)
            .count(),
        2
    );
}
