use async_trait::async_trait;
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use storage_pool::{adapters::*, ports::*, *};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use workflow_foundation::{Lifecycle, RunState};

fn pool_id(s: &str) -> PoolId {
    PoolId::new(s).unwrap()
}
fn disk_id(s: &str) -> DiskId {
    DiskId::new(s).unwrap()
}
fn node_id(s: &str) -> NodeId {
    NodeId::new(s).unwrap()
}
fn volume_id(s: &str) -> VolumeId {
    VolumeId::new(s).unwrap()
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("operation must make progress")
}
async fn result<T>(mut run: Run<T>) -> Outcome<T> {
    bounded(run.result()).await
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
    fn open(&self) {
        self.release.add_permits(1);
    }
    async fn pause(&self) {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
    }
}
#[derive(Clone)]
enum Point {
    Add,
    Allocate(DiskId),
    Create,
    Io(DiskId),
    Invalidate,
}
impl Point {
    fn matches(&self, change: &MetadataChange) -> bool {
        match (self, change) {
            (Self::Allocate(id), MetadataChange::Allocate { disk, .. })
            | (Self::Io(id), MetadataChange::DiskIo { disk, .. }) => id == disk,
            (Self::Add, MetadataChange::AddDisk(_))
            | (Self::Create, MetadataChange::CreateVolume(_))
            | (Self::Invalidate, MetadataChange::InvalidateDisk { .. }) => true,
            _ => false,
        }
    }
}
enum Behavior {
    Fail(Failure, bool),
    Pause(Arc<Gate>),
}
#[derive(Default)]
struct ControlledStore {
    inner: MemoryMetadata,
    next: Mutex<Option<(Point, Behavior)>>,
}
impl ControlledStore {
    fn fail(&self, point: Point, failure: Failure, applied: bool) {
        *self.next.lock().unwrap() = Some((point, Behavior::Fail(failure, applied)));
    }
    fn pause(&self, point: Point) -> Arc<Gate> {
        let gate = Gate::new();
        *self.next.lock().unwrap() = Some((point, Behavior::Pause(gate.clone())));
        gate
    }
}
#[async_trait]
impl MetadataStore for ControlledStore {
    async fn commit(&self, pool: &PoolId, change: MetadataChange) -> Outcome<()> {
        let behavior = {
            let mut next = self.next.lock().unwrap();
            if next.as_ref().is_some_and(|(p, _)| p.matches(&change)) {
                next.take().map(|(_, b)| b)
            } else {
                None
            }
        };
        match behavior {
            Some(Behavior::Fail(failure, applied)) => {
                if applied {
                    self.inner.commit(pool, change).await?;
                }
                return Err(failure);
            }
            Some(Behavior::Pause(gate)) => gate.pause().await,
            None => {}
        }
        self.inner.commit(pool, change).await
    }
}

#[derive(Default)]
struct ControlledPlane {
    down: Mutex<Option<(DiskId, Arc<Gate>)>>,
    init_failure: Mutex<Option<Failure>>,
    open_gate: Mutex<Option<Arc<Gate>>>,
    open_failure: Mutex<Option<Failure>>,
    up_failure: Mutex<Option<Failure>>,
    disk_calls: Mutex<Vec<&'static str>>,
}
#[async_trait]
impl DataPlane for ControlledPlane {
    async fn open_disk(&self, _: &PoolId, _: &DiskId, cancel: &CancellationToken) -> Outcome<()> {
        if cancel.is_cancelled() {
            return Err(Failure::Cancelled);
        }
        self.disk_calls.lock().unwrap().push("open");
        let gate = self.open_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.pause().await;
        }
        self.open_failure.lock().unwrap().take().map_or(Ok(()), Err)
    }
    async fn load_pool(&self, _: &PoolId, _: &NodeId, cancel: &CancellationToken) -> Outcome<()> {
        if cancel.is_cancelled() {
            Err(Failure::Cancelled)
        } else {
            Ok(())
        }
    }
    async fn set_disk_io(
        &self,
        _: &PoolId,
        id: &DiskId,
        io: IoState,
        cancel: &CancellationToken,
    ) -> Outcome<()> {
        if cancel.is_cancelled() {
            return Err(Failure::Cancelled);
        }
        self.disk_calls.lock().unwrap().push(if io == IoState::Up {
            "enable"
        } else {
            "disable"
        });
        if io == IoState::Up
            && let Some(failure) = self.up_failure.lock().unwrap().take()
        {
            return Err(failure);
        }
        let gate = {
            let mut next = self.down.lock().unwrap();
            if io == IoState::Down && next.as_ref().is_some_and(|(disk, _)| disk == id) {
                next.take().map(|(_, gate)| gate)
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.pause().await;
        }
        // Once started, this simulated external effect completes successfully
        // despite cancellation. The framework must wait for this confirmation.
        Ok(())
    }
    async fn initialize_bg(
        &self,
        _: &PoolId,
        _: &VolumeId,
        _: &[BgEntry],
        cancel: &CancellationToken,
    ) -> Outcome<()> {
        if cancel.is_cancelled() {
            return Err(Failure::Cancelled);
        }
        self.init_failure.lock().unwrap().take().map_or(Ok(()), Err)
    }
}

fn manager(store: Arc<dyn MetadataStore>, plane: Arc<dyn DataPlane>) -> PoolManager {
    PoolManager::new(Config::default(), store, plane)
}
async fn add(manager: &mut PoolManager, pool: &PoolHandle, disk: &str, node: &str, capacity: u32) {
    let node = node_id(node);
    result(pool.observe_node(node.clone(), 1, true).await.unwrap())
        .await
        .unwrap();
    result(pool.load_node(node.clone()).await.unwrap())
        .await
        .unwrap();
    result(
        manager
            .online_disk(
                pool.id(),
                DiskSpec {
                    id: disk_id(disk),
                    node,
                    tier: TierId::new("ssd").unwrap(),
                    capacity_blks: capacity,
                },
            )
            .await
            .unwrap(),
    )
    .await
    .unwrap();
}
async fn fixture(
    store: Arc<dyn MetadataStore>,
    plane: Arc<dyn DataPlane>,
    capacity: u32,
) -> (PoolManager, PoolHandle) {
    let mut manager = manager(store, plane);
    let pool = manager.create_pool(pool_id("a")).unwrap();
    add(&mut manager, &pool, "d1", "n1", capacity).await;
    add(&mut manager, &pool, "d2", "n2", capacity).await;
    (manager, pool)
}
fn request(name: &str) -> CreateVolume {
    CreateVolume {
        id: volume_id(name),
        disks: vec![disk_id("d1"), disk_id("d2")],
    }
}
async fn invariant(pool: &PoolHandle) {
    let disks = pool.disks().await.unwrap();
    let mut seen = std::collections::BTreeSet::new();
    for volume in pool.volumes().await.unwrap() {
        for entry in volume.groups.iter().flat_map(|g| &g.entries) {
            let disk = disks.iter().find(|d| d.id == entry.disk).unwrap();
            assert_eq!(disk.pool, volume.pool);
            assert!(disk.allocated.contains(&entry.blk));
            assert!(
                seen.insert((entry.disk.clone(), entry.blk)),
                "BLK double referenced"
            );
        }
    }
}

#[tokio::test]
async fn lifecycle_and_commit_order() {
    let store = Arc::new(MemoryMetadata::default());
    let (mut manager, pool) = fixture(store.clone(), Arc::new(SimulatedDataPlane), 8).await;
    let volume = result(pool.create_volume(request("v")).await.unwrap())
        .await
        .unwrap();
    assert_eq!(volume.replicas, 2);
    assert!(
        volume.groups[0]
            .entries
            .iter()
            .all(|e| e.data == DataState::Valid)
    );
    invariant(&pool).await;
    let log = store.committed();
    let published = log
        .iter()
        .position(|(_, c)| matches!(c, MetadataChange::CreateVolume(_)))
        .unwrap();
    assert_eq!(
        log[..published]
            .iter()
            .filter(|(_, c)| matches!(c, MetadataChange::Allocate { .. }))
            .count(),
        2
    );
    manager.shutdown().await.unwrap();
    assert_eq!(pool.execution().lifecycle, Lifecycle::Stopped);
}

#[tokio::test]
async fn connectivity_is_not_loading_and_observations_are_ordered() {
    let mut manager = manager(
        Arc::new(MemoryMetadata::default()),
        Arc::new(SimulatedDataPlane),
    );
    let pool = manager.create_pool(pool_id("a")).unwrap();
    let node = node_id("n");
    assert!(
        !result(pool.observe_node(node.clone(), 1, true).await.unwrap())
            .await
            .unwrap()
            .serving()
    );
    assert!(
        result(pool.load_node(node.clone()).await.unwrap())
            .await
            .unwrap()
            .serving()
    );
    let down = result(pool.observe_node(node.clone(), 3, false).await.unwrap())
        .await
        .unwrap();
    assert!(!down.loaded);
    assert_eq!(
        result(pool.observe_node(node.clone(), 2, true).await.unwrap())
            .await
            .unwrap(),
        down
    );
    assert!(matches!(
        result(pool.observe_node(node.clone(), 3, true).await.unwrap()).await,
        Err(Failure::Rejected(_))
    ));
    assert!(
        result(pool.load_node(node.clone()).await.unwrap())
            .await
            .is_err()
    );
    assert!(
        !result(pool.observe_node(node.clone(), 4, true).await.unwrap())
            .await
            .unwrap()
            .serving()
    );
    assert!(
        result(pool.load_node(node).await.unwrap())
            .await
            .unwrap()
            .serving()
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn online_does_not_restore_invalid_data_or_free_blocks() {
    let (mut manager, pool) = fixture(
        Arc::new(MemoryMetadata::default()),
        Arc::new(SimulatedDataPlane),
        2,
    )
    .await;
    result(pool.create_volume(request("v")).await.unwrap())
        .await
        .unwrap();
    result(pool.offline_disk(disk_id("d1")).await.unwrap())
        .await
        .unwrap();
    assert_eq!(pool.disks().await.unwrap()[0].io, IoState::Down);
    result(pool.online_disk(disk_id("d1")).await.unwrap())
        .await
        .unwrap();
    let views = pool.volumes().await.unwrap();
    assert_eq!(views[0].groups[0].entries[0].data, DataState::Invalid);
    assert_eq!(views[0].groups[0].entries[1].data, DataState::Valid);
    invariant(&pool).await;
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejects_bad_placement_capacity_duplicates_and_unknown_identity() {
    assert!(PoolId::new(" ").is_err());
    assert!(DiskId::new("x".repeat(129)).is_err());
    let (mut manager, pool) = fixture(
        Arc::new(MemoryMetadata::default()),
        Arc::new(SimulatedDataPlane),
        1,
    )
    .await;
    assert!(
        pool.create_volume(CreateVolume {
            id: volume_id("bad"),
            disks: vec![disk_id("d1"), disk_id("d1")]
        })
        .await
        .is_err()
    );
    assert!(
        pool.create_volume(CreateVolume {
            id: volume_id("bad"),
            disks: vec![disk_id("d1"), disk_id("missing")]
        })
        .await
        .is_err()
    );
    add(&mut manager, &pool, "d3", "n1", 1).await;
    assert!(
        pool.create_volume(CreateVolume {
            id: volume_id("bad"),
            disks: vec![disk_id("d1"), disk_id("d3")]
        })
        .await
        .is_err()
    );
    result(pool.create_volume(request("v")).await.unwrap())
        .await
        .unwrap();
    assert!(
        result(pool.create_volume(request("v")).await.unwrap())
            .await
            .is_err()
    );
    assert!(
        result(pool.create_volume(request("full")).await.unwrap())
            .await
            .is_err()
    );
    assert_eq!(pool.disks().await.unwrap()[0].allocated.len(), 1);
    invariant(&pool).await;
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn down_or_disconnected_replica_rejected_before_allocation() {
    let (mut manager, pool) = fixture(
        Arc::new(MemoryMetadata::default()),
        Arc::new(SimulatedDataPlane),
        2,
    )
    .await;
    result(pool.offline_disk(disk_id("d2")).await.unwrap())
        .await
        .unwrap();
    assert!(
        result(pool.create_volume(request("v")).await.unwrap())
            .await
            .is_err()
    );
    result(pool.online_disk(disk_id("d2")).await.unwrap())
        .await
        .unwrap();
    result(pool.observe_node(node_id("n2"), 2, false).await.unwrap())
        .await
        .unwrap();
    assert!(
        result(pool.create_volume(request("v")).await.unwrap())
            .await
            .is_err()
    );
    assert!(
        result(pool.online_disk(disk_id("d2")).await.unwrap())
            .await
            .is_err()
    );
    assert!(
        pool.disks()
            .await
            .unwrap()
            .iter()
            .all(|d| d.allocated.is_empty())
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_creates_do_not_double_allocate() {
    let (mut manager, pool) = fixture(
        Arc::new(MemoryMetadata::default()),
        Arc::new(SimulatedDataPlane),
        32,
    )
    .await;
    let mut tasks = Vec::new();
    for i in 0..24 {
        let pool = pool.clone();
        tasks.push(tokio::spawn(async move {
            result(pool.create_volume(request(&format!("v{i}"))).await.unwrap())
                .await
                .unwrap()
        }));
    }
    for task in tasks {
        bounded(task).await.unwrap();
    }
    assert_eq!(pool.volumes().await.unwrap().len(), 24);
    assert!(
        pool.disks()
            .await
            .unwrap()
            .iter()
            .all(|d| d.allocated.len() == 24)
    );
    invariant(&pool).await;
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn partial_allocation_failure_leaks_but_never_publishes_reference() {
    let store = Arc::new(ControlledStore::default());
    let (mut manager, pool) = fixture(store.clone(), Arc::new(SimulatedDataPlane), 3).await;
    store.fail(
        Point::Allocate(disk_id("d2")),
        Failure::Failed("known rejection".into()),
        false,
    );
    assert!(
        result(pool.create_volume(request("v")).await.unwrap())
            .await
            .is_err()
    );
    assert_eq!(pool.disks().await.unwrap()[0].allocated.len(), 1);
    assert!(pool.disks().await.unwrap()[1].allocated.is_empty());
    assert!(pool.volumes().await.unwrap().is_empty());
    result(pool.create_volume(request("v2")).await.unwrap())
        .await
        .unwrap();
    assert_eq!(pool.volumes().await.unwrap()[0].groups[0].entries[0].blk, 1);
    invariant(&pool).await;
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn publication_failure_keeps_allocations_and_allows_safe_new_work() {
    let store = Arc::new(ControlledStore::default());
    let (mut manager, pool) = fixture(store.clone(), Arc::new(SimulatedDataPlane), 3).await;
    store.fail(
        Point::Create,
        Failure::Failed("not committed".into()),
        false,
    );
    assert!(
        result(pool.create_volume(request("v")).await.unwrap())
            .await
            .is_err()
    );
    assert!(
        pool.disks()
            .await
            .unwrap()
            .iter()
            .all(|d| d.allocated.len() == 1)
    );
    assert!(pool.volumes().await.unwrap().is_empty());
    result(pool.create_volume(request("v2")).await.unwrap())
        .await
        .unwrap();
    assert_eq!(pool.execution().fenced, 0);
    invariant(&pool).await;
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_initialization_never_marks_data_valid() {
    let plane = Arc::new(ControlledPlane::default());
    let (mut manager, pool) = fixture(Arc::new(MemoryMetadata::default()), plane.clone(), 2).await;
    *plane.init_failure.lock().unwrap() = Some(Failure::Failed("not initialized".into()));
    assert!(
        result(pool.create_volume(request("v")).await.unwrap())
            .await
            .is_err()
    );
    assert!(pool.volumes().await.unwrap().is_empty());
    assert!(
        pool.disks()
            .await
            .unwrap()
            .iter()
            .all(|d| d.allocated.len() == 1)
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn ambiguous_commit_fences_followers_and_does_not_publish_unconfirmed_view() {
    let store = Arc::new(ControlledStore::default());
    let (mut manager, pool) = fixture(store.clone(), Arc::new(SimulatedDataPlane), 3).await;
    store.fail(
        Point::Create,
        Failure::Uncertain("reply lost after commit".into()),
        true,
    );
    assert!(matches!(
        result(pool.create_volume(request("v")).await.unwrap()).await,
        Err(Failure::Uncertain(_))
    ));
    assert!(pool.volumes().await.unwrap().is_empty());
    assert!(
        store
            .inner
            .committed()
            .iter()
            .any(|(_, c)| matches!(c, MetadataChange::CreateVolume(_)))
    );
    assert!(matches!(
        result(pool.create_volume(request("v2")).await.unwrap()).await,
        Err(Failure::Rejected(_))
    ));
    assert!(pool.execution().fenced > 0);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn io_success_then_metadata_failure_is_uncertain() {
    let store = Arc::new(ControlledStore::default());
    let (mut manager, pool) = fixture(store.clone(), Arc::new(SimulatedDataPlane), 2).await;
    store.fail(
        Point::Io(disk_id("d1")),
        Failure::Failed("write refused".into()),
        false,
    );
    assert!(matches!(
        result(pool.offline_disk(disk_id("d1")).await.unwrap()).await,
        Err(Failure::Uncertain(_))
    ));
    assert_eq!(
        pool.disks().await.unwrap()[0].io,
        IoState::Up,
        "only confirmed metadata is published"
    );
    assert!(matches!(
        result(pool.online_disk(disk_id("d1")).await.unwrap()).await,
        Err(Failure::Rejected(_))
    ));
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn metadata_in_flight_keeps_old_view_and_object_ownership() {
    let store = Arc::new(ControlledStore::default());
    let (mut manager, pool) = fixture(store.clone(), Arc::new(SimulatedDataPlane), 2).await;
    let gate = store.pause(Point::Allocate(disk_id("d1")));
    let create = pool.create_volume(request("v")).await.unwrap();
    gate.wait().await;
    assert!(pool.disks().await.unwrap()[0].allocated.is_empty());
    let mut offline = pool.offline_disk(disk_id("d1")).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), offline.result())
            .await
            .is_err()
    );
    gate.open();
    result(create).await.unwrap();
    result(offline).await.unwrap();
    assert_eq!(
        pool.volumes().await.unwrap()[0].groups[0].entries[0].data,
        DataState::Invalid
    );
    invariant(&pool).await;
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_after_allocation_retains_safe_orphan_and_unblocks_next_run() {
    let store = Arc::new(ControlledStore::default());
    let (mut manager, pool) = fixture(store.clone(), Arc::new(SimulatedDataPlane), 3).await;
    let gate = store.pause(Point::Allocate(disk_id("d1")));
    let mut run = pool.create_volume(request("v")).await.unwrap();
    gate.wait().await;
    run.control.cancel();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), run.result())
            .await
            .is_err()
    );
    gate.open();
    assert_eq!(result(run).await, Err(Failure::Cancelled));
    assert_eq!(pool.disks().await.unwrap()[0].allocated.len(), 1);
    assert!(pool.volumes().await.unwrap().is_empty());
    result(pool.create_volume(request("v2")).await.unwrap())
        .await
        .unwrap();
    invariant(&pool).await;
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_offline_waits_for_stable_io_before_online_and_other_disk_progresses() {
    let plane = Arc::new(ControlledPlane::default());
    let (mut manager, pool) = fixture(Arc::new(MemoryMetadata::default()), plane.clone(), 2).await;
    let gate = Gate::new();
    *plane.down.lock().unwrap() = Some((disk_id("d1"), gate.clone()));
    let mut offline = pool.offline_disk(disk_id("d1")).await.unwrap();
    gate.wait().await;
    offline.control.cancel();
    let mut online = pool.online_disk(disk_id("d1")).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), offline.result())
            .await
            .is_err()
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), online.result())
            .await
            .is_err()
    );
    result(pool.offline_disk(disk_id("d2")).await.unwrap())
        .await
        .unwrap();
    gate.open();
    assert_eq!(result(offline).await.unwrap().io, IoState::Down);
    assert_eq!(result(online).await.unwrap().io, IoState::Up);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn pools_isolate_execution_but_cannot_share_physical_disk() {
    let store = Arc::new(ControlledStore::default());
    let (mut manager, a) = fixture(store.clone(), Arc::new(SimulatedDataPlane), 2).await;
    let b = manager.create_pool(pool_id("b")).unwrap();
    add(&mut manager, &b, "b1", "n1", 2).await;
    add(&mut manager, &b, "b2", "n2", 2).await;
    assert!(
        manager
            .online_disk(
                b.id(),
                DiskSpec {
                    id: disk_id("d1"),
                    node: node_id("n1"),
                    tier: TierId::new("ssd").unwrap(),
                    capacity_blks: 2
                }
            )
            .await
            .is_err()
    );
    let gate = store.pause(Point::Allocate(disk_id("d1")));
    let first = a.create_volume(request("same-vd-id")).await.unwrap();
    gate.wait().await;
    result(
        b.create_volume(CreateVolume {
            id: volume_id("same-vd-id"),
            disks: vec![disk_id("b1"), disk_id("b2")],
        })
        .await
        .unwrap(),
    )
    .await
    .unwrap();
    assert!(a.volumes().await.unwrap().is_empty());
    gate.open();
    result(first).await.unwrap();
    manager.shutdown_pool(a.id()).await.unwrap();
    assert_eq!(b.execution().lifecycle, Lifecycle::Running);
    assert!(manager.create_pool(a.id().clone()).is_err());
    manager.shutdown().await.unwrap();
    assert!(manager.create_pool(pool_id("c")).is_err());
}

#[tokio::test]
async fn invalidation_failure_does_not_claim_offline_success() {
    let store = Arc::new(ControlledStore::default());
    let (mut manager, pool) = fixture(store.clone(), Arc::new(SimulatedDataPlane), 2).await;
    result(pool.create_volume(request("v")).await.unwrap())
        .await
        .unwrap();
    store.fail(
        Point::Invalidate,
        Failure::Failed("not committed".into()),
        false,
    );
    let run = pool.offline_disk(disk_id("d1")).await.unwrap();
    let control = run.control.clone();
    assert!(result(run).await.is_err());
    assert!(matches!(control.state(), RunState::Failed(_)));
    assert_eq!(pool.disks().await.unwrap()[0].io, IoState::Up);
    assert_eq!(
        pool.volumes().await.unwrap()[0].groups[0].entries[0].data,
        DataState::Valid
    );
    manager.shutdown().await.unwrap();
}

async fn new_disk_fixture(
    store: Arc<dyn MetadataStore>,
    plane: Arc<dyn DataPlane>,
) -> (PoolManager, PoolHandle, DiskSpec) {
    let mut manager = manager(store, plane);
    let pool = manager.create_pool(pool_id("a")).unwrap();
    let node = node_id("n");
    result(pool.observe_node(node.clone(), 1, true).await.unwrap())
        .await
        .unwrap();
    result(pool.load_node(node.clone()).await.unwrap())
        .await
        .unwrap();
    let spec = DiskSpec {
        id: disk_id("new"),
        node,
        tier: TierId::new("ssd").unwrap(),
        capacity_blks: 3,
    };
    (manager, pool, spec)
}

#[tokio::test]
async fn online_is_one_run_with_read_add_open_enable_steps_and_no_early_success() {
    let store = Arc::new(ControlledStore::default());
    let plane = Arc::new(ControlledPlane::default());
    let (mut manager, pool, spec) = new_disk_fixture(store.clone(), plane.clone()).await;
    let gate = Gate::new();
    *plane.open_gate.lock().unwrap() = Some(gate.clone());
    let mut run = manager.online_disk(pool.id(), spec.clone()).await.unwrap();
    gate.wait().await;
    assert_eq!(run.control.name, "storage.online-disk");
    // InspectNode is Activity #1 too, not an unmanaged direct query.
    assert_eq!(
        run.control.state(),
        RunState::Activity(workflow_foundation::ActivityId {
            workflow: run.id(),
            sequence: 3
        })
    );
    assert_eq!(pool.disks().await.unwrap()[0].io, IoState::Down);
    assert_eq!(plane.disk_calls.lock().unwrap().as_slice(), ["open"]);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), run.result())
            .await
            .is_err()
    );
    gate.open();
    assert_eq!(result(run).await.unwrap().io, IoState::Up);
    assert_eq!(
        plane.disk_calls.lock().unwrap().as_slice(),
        ["open", "enable"]
    );
    result(manager.online_disk(pool.id(), spec).await.unwrap())
        .await
        .unwrap();
    assert_eq!(
        store
            .inner
            .committed()
            .iter()
            .filter(|(_, c)| matches!(c, MetadataChange::AddDisk(_)))
            .count(),
        1,
        "membership must not be added twice"
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn online_failure_matrix_never_enables_before_add_and_open_succeed() {
    for stage in 0..3 {
        let store = Arc::new(ControlledStore::default());
        let plane = Arc::new(ControlledPlane::default());
        let (mut manager, pool, spec) = new_disk_fixture(store.clone(), plane.clone()).await;
        let failure = Failure::Failed("known step failure".into());
        match stage {
            0 => store.fail(Point::Add, failure, false),
            1 => *plane.open_failure.lock().unwrap() = Some(failure),
            _ => *plane.up_failure.lock().unwrap() = Some(failure),
        }
        assert!(matches!(
            result(manager.online_disk(pool.id(), spec.clone()).await.unwrap()).await,
            Err(Failure::Failed(_))
        ));
        let disks = pool.disks().await.unwrap();
        if stage == 0 {
            assert!(disks.is_empty());
        } else {
            assert_eq!(disks[0].io, IoState::Down);
        }
        assert!(!store.inner.committed().iter().any(|(_, c)| matches!(
            c,
            MetadataChange::DiskIo {
                io: IoState::Up,
                ..
            }
        )));
        assert_eq!(plane.disk_calls.lock().unwrap().len(), stage);
        result(manager.online_disk(pool.id(), spec).await.unwrap())
            .await
            .unwrap();
        assert_eq!(pool.disks().await.unwrap()[0].io, IoState::Up);
        manager.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn pause_after_open_blocks_enable_and_resume_continues_same_online_run() {
    let plane = Arc::new(ControlledPlane::default());
    let (mut manager, pool, spec) =
        new_disk_fixture(Arc::new(MemoryMetadata::default()), plane.clone()).await;
    let gate = Gate::new();
    *plane.open_gate.lock().unwrap() = Some(gate.clone());
    let mut run = manager.online_disk(pool.id(), spec).await.unwrap();
    gate.wait().await;
    run.control.pause().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), run.control.wait_paused())
            .await
            .is_err()
    );
    gate.open();
    bounded(run.control.wait_paused()).await.unwrap();
    assert_eq!(plane.disk_calls.lock().unwrap().as_slice(), ["open"]);
    assert_eq!(pool.disks().await.unwrap()[0].io, IoState::Down);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), run.result())
            .await
            .is_err()
    );
    run.control.resume().unwrap();
    assert_eq!(result(run).await.unwrap().io, IoState::Up);
    assert_eq!(
        plane.disk_calls.lock().unwrap().as_slice(),
        ["open", "enable"]
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn offline_policy_cancels_online_but_waits_for_open_to_settle() {
    let plane = Arc::new(ControlledPlane::default());
    let (mut manager, pool, spec) =
        new_disk_fixture(Arc::new(MemoryMetadata::default()), plane.clone()).await;
    let gate = Gate::new();
    *plane.open_gate.lock().unwrap() = Some(gate.clone());
    let online = manager.online_disk(pool.id(), spec.clone()).await.unwrap();
    gate.wait().await;
    assert!(matches!(
        manager.online_disk(pool.id(), spec.clone()).await,
        Err(Failure::Rejected(_))
    ));
    assert!(
        !online.control.cancellation_requested(),
        "rejected duplicate must not cancel accepted run"
    );
    let mut offline = pool.offline_disk(spec.id).await.unwrap();
    assert!(online.control.cancellation_requested());
    assert!(
        tokio::time::timeout(Duration::from_millis(20), offline.result())
            .await
            .is_err()
    );
    gate.open();
    assert_eq!(result(online).await, Err(Failure::Cancelled));
    assert_eq!(result(offline).await.unwrap().io, IoState::Down);
    assert_eq!(
        plane.disk_calls.lock().unwrap().as_slice(),
        ["open", "disable"]
    );
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn paused_online_can_be_cancelled_by_offline_without_resume() {
    let plane = Arc::new(ControlledPlane::default());
    let (mut manager, pool, spec) =
        new_disk_fixture(Arc::new(MemoryMetadata::default()), plane.clone()).await;
    let gate = Gate::new();
    *plane.open_gate.lock().unwrap() = Some(gate.clone());
    let online = manager.online_disk(pool.id(), spec.clone()).await.unwrap();
    gate.wait().await;
    online.control.pause().unwrap();
    gate.open();
    bounded(online.control.wait_paused()).await.unwrap();
    let offline = pool.offline_disk(spec.id).await.unwrap();
    assert_eq!(result(online).await, Err(Failure::Cancelled));
    result(offline).await.unwrap();
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn pool_shutdown_waits_for_inflight_operation_and_is_retryable() {
    let plane = Arc::new(ControlledPlane::default());
    let (mut manager, pool, spec) =
        new_disk_fixture(Arc::new(MemoryMetadata::default()), plane.clone()).await;
    let gate = Gate::new();
    *plane.open_gate.lock().unwrap() = Some(gate.clone());
    let run = manager.online_disk(pool.id(), spec).await.unwrap();
    gate.wait().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), manager.shutdown())
            .await
            .is_err()
    );
    assert!(run.control.cancellation_requested());
    gate.open();
    bounded(manager.shutdown()).await.unwrap();
    assert_eq!(result(run).await, Err(Failure::Cancelled));
    assert_eq!(plane.disk_calls.lock().unwrap().as_slice(), ["open"]);
    assert_eq!(pool.execution().lifecycle, Lifecycle::Stopped);
}

#[test]
fn conflict_policy_is_explicit_and_testable_without_running_services() {
    use storage_pool::coordination::*;
    use workflow_foundation::{AdmissionDecision::*, AdmissionPolicy, ObjectKey};
    let node = node_id("n");
    let operation = |kind| PoolOperation {
        kind,
        objects: vec![ObjectKey::new("node", "n")],
    };
    let old = operation(OperationKind::ObserveNode {
        id: node.clone(),
        sequence: 3,
        connected: true,
    });
    let new = operation(OperationKind::ObserveNode {
        id: node.clone(),
        sequence: 4,
        connected: false,
    });
    assert_eq!(PoolConflictPolicy.decide(&new, &old), CancelExisting);
    assert!(matches!(PoolConflictPolicy.decide(&old, &new), Reject(_)));
    assert_eq!(
        PoolConflictPolicy.decide(&new, &operation(OperationKind::LoadNode(node))),
        CancelExisting
    );
    let create = operation(OperationKind::CreateVolume(volume_id("v")));
    let offline = operation(OperationKind::OfflineDisk(disk_id("d")));
    assert_eq!(PoolConflictPolicy.decide(&offline, &create), Queue);
}

#[test]
fn workflows_do_not_bypass_activity_with_direct_domain_queries() {
    assert!(!include_str!("../src/app/workflows/mod.rs").contains(".query("));
}

#[tokio::test]
async fn ambiguous_open_fences_replacement_without_publishing_up() {
    let plane = Arc::new(ControlledPlane::default());
    let (mut manager, pool, spec) =
        new_disk_fixture(Arc::new(MemoryMetadata::default()), plane.clone()).await;
    *plane.open_failure.lock().unwrap() =
        Some(Failure::Uncertain("open acknowledgement lost".into()));
    assert!(matches!(
        result(manager.online_disk(pool.id(), spec.clone()).await.unwrap()).await,
        Err(Failure::Uncertain(_))
    ));
    assert_eq!(pool.disks().await.unwrap()[0].io, IoState::Down);
    assert!(matches!(
        result(pool.offline_disk(spec.id).await.unwrap()).await,
        Err(Failure::Rejected(_))
    ));
    assert_eq!(plane.disk_calls.lock().unwrap().as_slice(), ["open"]);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn three_replica_layout_and_topology_and_revision_validation() {
    let store = Arc::new(MemoryMetadata::default());
    let (mut manager, pool) = fixture(store.clone(), Arc::new(SimulatedDataPlane), 3).await;
    add(&mut manager, &pool, "d3", "n3", 3).await;
    let mut req = request("three");
    req.disks.push(disk_id("d3"));
    assert_eq!(
        result(pool.create_volume(req).await.unwrap())
            .await
            .unwrap()
            .replicas,
        3
    );
    let bad = DiskSpec {
        id: disk_id("d1"),
        node: node_id("n1"),
        tier: TierId::new("other-tier").unwrap(),
        capacity_blks: 3,
    };
    assert!(matches!(
        result(manager.online_disk(pool.id(), bad).await.unwrap()).await,
        Err(Failure::Rejected(_))
    ));
    let before = store.committed().len();
    assert!(
        store
            .commit(
                pool.id(),
                MetadataChange::DiskIo {
                    disk: disk_id("d1"),
                    expected_revision: 0,
                    io: IoState::Down
                }
            )
            .await
            .is_err()
    );
    assert_eq!(store.committed().len(), before);
    let mut detached = pool.disks().await.unwrap();
    detached[0].allocated.clear();
    assert_eq!(
        pool.disks().await.unwrap()[0].allocated.len(),
        1,
        "query must not expose mutable state"
    );
    invariant(&pool).await;
    manager.shutdown().await.unwrap();
}
