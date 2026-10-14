use super::*;
use crate::process_manager::*;
use async_trait::async_trait;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Ports {
    commits: Mutex<Vec<MemberDiskCommit>>,
    calls: AtomicUsize,
    gate_first: bool,
    started: Notify,
    finish: Notify,
}

#[async_trait]
impl MemberDiskMetadata for Ports {
    async fn commit(&self, changes: Vec<MemberDiskCommit>) -> Vec<Result<(), PortError>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.gate_first && call == 0 {
            self.started.notify_one();
            self.finish.notified().await;
        }
        self.commits.lock().unwrap().extend(changes.clone());
        changes.into_iter().map(|_| Ok(())).collect()
    }
}
#[async_trait]
impl PoolNodes for Ports {
    async fn open_disks(&self, disks: Vec<DiskUuid>) -> Result<Vec<DiskOpenResult>, PortError> {
        Ok(disks
            .into_iter()
            .map(|disk| DiskOpenResult {
                disk,
                result: Ok(()),
            })
            .collect())
    }
    async fn push_disk_states(&self, _: Vec<DiskStateChange>) -> Result<(), PortError> {
        Ok(())
    }
}
#[async_trait]
impl VirtualDisks for Ports {
    async fn evacuate(&self, _: &DiskUuid, _: &CancellationToken) -> Result<(), PortError> {
        Ok(())
    }
}

fn pool(id: &str, ports: Arc<Ports>) -> Pool {
    Pool::new(
        id,
        vec![MemberDisk::new("disk".into(), id, 8)],
        ports.clone(),
        ports.clone(),
        ports,
        PoolConfig {
            recovery_window: Duration::ZERO,
            ..Default::default()
        },
    )
    .unwrap()
}
fn down(id: &str) -> ProcessCommand {
    ProcessCommand::ApplyEvents(vec![DiskEvent {
        disk: id.into(),
        state: PhysicalState::Down,
        observed_at: now_millis(),
    }])
}
fn allocated(reply: ProcessReply) -> Vec<AllocatedBlock> {
    match reply {
        ProcessReply::Allocated(b) => b,
        _ => panic!("wrong reply"),
    }
}

#[tokio::test]
async fn pool_manager_routes_and_stops_without_cross_pool_state() {
    let mut manager = PoolManager::default();
    let a_ports = Arc::new(Ports::default());
    let b_ports = Arc::new(Ports::default());
    let a = manager
        .insert(pool("a", a_ports.clone()), ServiceConfig::default())
        .unwrap();
    let b = manager
        .insert(pool("b", b_ports.clone()), ServiceConfig::default())
        .unwrap();
    assert!(matches!(
        manager.insert(
            pool("a", Arc::new(Ports::default())),
            ServiceConfig::default()
        ),
        Err(PoolManagerError::DuplicatePool(_))
    ));
    assert!(matches!(
        manager.get("absent"),
        Err(PoolManagerError::UnknownPool(_))
    ));
    let result = manager
        .get("a")
        .unwrap()
        .client
        .call(down("disk"))
        .await
        .unwrap();
    assert!(
        matches!(result, ProcessReply::Disks(ref outcomes) if outcomes[0].result.as_ref().unwrap().state() == MemberDiskState::Removed)
    );
    assert!(b_ports.commits.lock().unwrap().is_empty());
    let ProcessQueryReply::Disk(Some(member)) = b
        .client
        .query(ProcessQuery::Get("disk".into()))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(member.state(), MemberDiskState::UpActive);
    assert_eq!(a.observer.snapshot().service.pool, "a");
    assert_eq!(b.observer.snapshot().service.pool, "b");
    manager.remove("a").await.unwrap();
    assert!(a.client.call(down("disk")).await.is_err());
    assert_eq!(
        allocated(
            b.client
                .call(ProcessCommand::Allocate(AllocateBlocks { count: 2 }))
                .await
                .unwrap()
        )
        .len(),
        2
    );
    let new_a = manager
        .insert(
            pool("a", Arc::new(Ports::default())),
            ServiceConfig::default(),
        )
        .unwrap();
    assert!(
        a.client.call(down("disk")).await.is_err(),
        "stale route must not reach replacement Pool"
    );
    assert!(
        matches!(new_a.client.query(ProcessQuery::Get("disk".into())).await.unwrap(), ProcessQueryReply::Disk(Some(d)) if d.state() == MemberDiskState::UpActive)
    );
    manager.remove("a").await.unwrap();
    manager.remove("b").await.unwrap();
    assert!(manager.ids().is_empty());
}

#[test]
fn pool_validates_member_ownership_and_identity() {
    let ports = Arc::new(Ports::default());
    let make = |id: &str, disks| {
        Pool::new(
            id,
            disks,
            ports.clone(),
            ports.clone(),
            ports.clone(),
            PoolConfig::default(),
        )
    };
    assert!(make("a", vec![MemberDisk::new("disk".into(), "b", 8)]).is_err());
    assert!(make(" ", Vec::new()).is_err());
    assert!(
        make(
            "a",
            vec![
                MemberDisk::new("disk".into(), "a", 8),
                MemberDisk::new("disk".into(), "a", 8)
            ]
        )
        .is_err()
    );
}

#[tokio::test]
async fn dropped_batch_leader_does_not_orphan_other_domain_callers() {
    let ports = Arc::new(Ports::default());
    let pool = Pool::new(
        "a",
        vec![
            MemberDisk::new("first".into(), "a", 8),
            MemberDisk::new("second".into(), "a", 8),
        ],
        ports.clone(),
        ports.clone(),
        ports,
        PoolConfig::default(),
    )
    .unwrap();
    let disk = pool.disks();
    let cancel = CancellationToken::new();
    let first_id = DiskUuid::from("first");
    let second_id = DiskUuid::from("second");
    let mut first = Box::pin(disk.close_io(&first_id, &cancel));
    let mut second = Box::pin(disk.close_io(&second_id, &cancel));
    assert!(futures_util::poll!(&mut first).is_pending());
    assert!(futures_util::poll!(&mut second).is_pending());
    drop(first);
    let result = tokio::time::timeout(Duration::from_secs(2), second)
        .await
        .unwrap();
    assert!(matches!(result, Err(MemberDiskError::NeedsRecovery(_))));
    assert_eq!(disk.blocked_resources().len(), 2);
}

#[tokio::test]
async fn direct_domain_allocations_are_serialized_without_a_process_queue() {
    let pool = pool("a", Arc::new(Ports::default()));
    let disk = pool.disks();
    let instance = pool.start(ServiceConfig::default());
    let cancel = CancellationToken::new();
    let (left, right) = tokio::join!(disk.allocate(3, &cancel), disk.allocate(3, &cancel));
    let left = left.unwrap();
    let right = right.unwrap();
    assert!(left.iter().all(|block| !right.contains(block)));
    assert_eq!(
        disk.get(&"disk".into()).unwrap().allocated_blocks().count(),
        6
    );
    assert!(instance.observer.snapshot().service.workflows.is_empty());
    instance.control.stop().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn direct_disk_mutation_waits_for_the_space_commit() {
    let ports = Arc::new(Ports {
        gate_first: true,
        ..Default::default()
    });
    let pool = pool("a", ports.clone());
    let disk = pool.disks();
    let instance = pool.start(ServiceConfig::default());
    let allocator = disk.clone();
    let allocation =
        tokio::spawn(async move { allocator.allocate(1, &CancellationToken::new()).await });
    ports.started.notified().await;
    let cancel = CancellationToken::new();
    let id = DiskUuid::from("disk");
    let close = disk.close_io(&id, &cancel);
    tokio::pin!(close);
    assert!(futures_util::poll!(&mut close).is_pending());
    assert_eq!(ports.calls.load(Ordering::SeqCst), 1);
    assert_eq!(disk.get(&id).unwrap().state(), MemberDiskState::UpActive);
    ports.finish.notify_one();
    allocation.await.unwrap().unwrap();
    close.await.unwrap();
    assert_eq!(disk.get(&id).unwrap().state(), MemberDiskState::DownActive);
    instance.control.stop().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn dropped_domain_effect_keeps_a_fence_outside_workflow_tracking() {
    let ports = Arc::new(Ports {
        gate_first: true,
        ..Default::default()
    });
    let pool = pool("a", ports.clone());
    let disk = pool.disks();
    let instance = pool.start(ServiceConfig::default());
    let writer = disk.clone();
    let action = tokio::spawn(async move {
        writer
            .close_io(&"disk".into(), &CancellationToken::new())
            .await
    });
    ports.started.notified().await;
    action.abort();
    assert!(action.await.unwrap_err().is_cancelled());
    assert_eq!(disk.blocked_resources(), vec![Some("disk".into())]);
    assert!(matches!(
        disk.bring_online(&"disk".into(), &CancellationToken::new())
            .await,
        Err(MemberDiskError::NeedsRecovery(_))
    ));
    assert!(matches!(
        disk.allocate(1, &CancellationToken::new()).await,
        Err(MemberDiskError::NeedsRecovery(_))
    ));
    assert_eq!(ports.calls.load(Ordering::SeqCst), 1);
    instance.control.stop().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn drain_proofs_check_pool_incarnation_disk_and_committed_version() {
    let original = pool("a", Arc::new(Ports::default()));
    let other = pool("a", Arc::new(Ports::default())); // Same name, different instance.
    let disk = original.disks();
    let vd = original.virtual_disks();
    let foreign = other.disks();
    let cancel = CancellationToken::new();
    let id = DiskUuid::from("disk");
    disk.disable_allocation(&id, &cancel).await.unwrap();
    let ticket = disk.prepare_drain(&id, &cancel).await.unwrap();
    assert!(matches!(
        other.virtual_disks().evacuate(ticket, &cancel).await,
        Err(MemberDiskError::InvalidRequest(_))
    ));
    let proof = vd
        .evacuate(disk.prepare_drain(&id, &cancel).await.unwrap(), &cancel)
        .await
        .unwrap();
    assert!(matches!(
        foreign.remove(proof, &cancel).await,
        Err(MemberDiskError::InvalidRequest(_))
    ));
    let old = vd
        .evacuate(disk.prepare_drain(&id, &cancel).await.unwrap(), &cancel)
        .await
        .unwrap();
    disk.bring_online(&id, &cancel).await.unwrap();
    disk.disable_allocation(&id, &cancel).await.unwrap();
    assert!(matches!(
        disk.remove(old, &cancel).await,
        Err(MemberDiskError::InvalidState {
            reason: "stale drain proof",
            ..
        })
    ));
    let proof = vd
        .evacuate(disk.prepare_drain(&id, &cancel).await.unwrap(), &cancel)
        .await
        .unwrap();
    disk.remove(proof, &cancel).await.unwrap();
    assert_eq!(disk.get(&id).unwrap().state(), MemberDiskState::Removed);
    assert_eq!(foreign.get(&id).unwrap().state(), MemberDiskState::UpActive);
}

#[tokio::test]
async fn pool_stop_waits_for_direct_domain_actions_and_closes_retained_facades() {
    let ports = Arc::new(Ports {
        gate_first: true,
        ..Default::default()
    });
    let pool = pool("a", ports.clone());
    let disk = pool.disks();
    let instance = pool.start(ServiceConfig::default());
    let writer = disk.clone();
    let action = tokio::spawn(async move { writer.allocate(1, &CancellationToken::new()).await });
    ports.started.notified().await;
    let control = instance.control.clone();
    let stop = tokio::spawn(async move { control.stop().await });
    let mut observer = instance.observer.clone();
    while observer.snapshot().lifecycle != Lifecycle::Stopping {
        observer.changed().await.unwrap();
    }
    assert!(!stop.is_finished());
    ports.finish.notify_one();
    assert_eq!(
        action.await.unwrap().unwrap().len(),
        1,
        "confirmed final commit wins over cancellation"
    );
    stop.await.unwrap().unwrap();
    assert!(matches!(
        disk.allocate(1, &CancellationToken::new()).await,
        Err(MemberDiskError::Cancelled)
    ));
    instance.join().await.unwrap();
}

struct HeldEvacuation {
    started: Semaphore,
    settled: Semaphore,
}
#[async_trait]
impl VirtualDisks for HeldEvacuation {
    async fn evacuate(&self, _: &DiskUuid, cancel: &CancellationToken) -> Result<(), PortError> {
        self.started.add_permits(1);
        cancel.cancelled().await;
        self.settled.acquire().await.unwrap().forget();
        Err(PortError("stably cancelled".into()))
    }
}

#[tokio::test]
async fn waiting_lifecycle_does_not_hold_domain_space_lock_or_block_another_pool() {
    let ports = Arc::new(Ports::default());
    let vd = Arc::new(HeldEvacuation {
        started: Semaphore::new(0),
        settled: Semaphore::new(0),
    });
    let a = Pool::new(
        "a",
        vec![
            MemberDisk::new("disk".into(), "a", 8),
            MemberDisk::new("other".into(), "a", 8),
        ],
        ports.clone(),
        ports.clone(),
        vd.clone(),
        PoolConfig {
            recovery_window: Duration::ZERO,
            ..Default::default()
        },
    )
    .unwrap();
    let mut manager = PoolManager::default();
    let a = manager.insert(a, ServiceConfig::default()).unwrap();
    let b = manager
        .insert(
            pool("b", Arc::new(Ports::default())),
            ServiceConfig::default(),
        )
        .unwrap();
    a.client.submit(down("disk")).await.unwrap();
    vd.started.acquire().await.unwrap().forget();
    let reply = tokio::time::timeout(
        Duration::from_secs(2),
        a.client
            .call(ProcessCommand::Allocate(AllocateBlocks { count: 2 })),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        allocated(reply)
            .iter()
            .all(|block| block.disk == DiskUuid::from("other"))
    );
    b.client.call(down("disk")).await.unwrap();
    let control = a.control.clone();
    let stop = tokio::spawn(async move { control.stop().await });
    let mut observer = a.observer.clone();
    while observer.snapshot().lifecycle != Lifecycle::Stopping {
        observer.changed().await.unwrap();
    }
    assert!(
        !stop.is_finished(),
        "cannot release Pool before VD acknowledges cancellation"
    );
    assert!(b.client.query(ProcessQuery::List).await.is_ok());
    vd.settled.add_permits(1);
    stop.await.unwrap().unwrap();
    manager.remove("a").await.unwrap();
    manager.remove("b").await.unwrap();
}

#[tokio::test]
async fn pause_also_gates_direct_domain_admission() {
    let ports = Arc::new(Ports::default());
    let pool = pool("a", ports.clone());
    let disk = pool.disks();
    let instance = pool.start(ServiceConfig::default());
    instance.control.pause().await.unwrap();
    let cancel = CancellationToken::new();
    let mut allocation = Box::pin(disk.allocate(1, &cancel));
    assert!(futures_util::poll!(&mut allocation).is_pending());
    assert_eq!(ports.calls.load(Ordering::SeqCst), 0);
    instance.control.resume().await.unwrap();
    assert_eq!(allocation.await.unwrap().len(), 1);
    instance.control.stop().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn cancelling_pool_removal_keeps_ownership_until_stable_stop() {
    let ports = Arc::new(Ports::default());
    let vd = Arc::new(HeldEvacuation {
        started: Semaphore::new(0),
        settled: Semaphore::new(0),
    });
    let pool = Pool::new(
        "a",
        vec![MemberDisk::new("disk".into(), "a", 8)],
        ports.clone(),
        ports,
        vd.clone(),
        PoolConfig {
            recovery_window: Duration::ZERO,
            ..Default::default()
        },
    )
    .unwrap();
    let mut manager = PoolManager::default();
    let handle = manager.insert(pool, ServiceConfig::default()).unwrap();
    handle.client.submit(down("disk")).await.unwrap();
    vd.started.acquire().await.unwrap().forget();
    let mut remove = Box::pin(manager.remove("a"));
    assert!(futures_util::poll!(&mut remove).is_pending());
    let mut observer = handle.observer.clone();
    while observer.snapshot().lifecycle != Lifecycle::Stopping {
        observer.changed().await.unwrap();
    }
    drop(remove);
    assert!(
        manager.get("a").is_ok(),
        "cancelled removal must not drop the owned root"
    );
    assert_eq!(observer.snapshot().lifecycle, Lifecycle::Stopping);
    vd.settled.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), manager.remove("a"))
        .await
        .unwrap()
        .unwrap();
    assert!(manager.ids().is_empty());
}
