use super::*;
use async_trait::async_trait;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;

const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Call {
    Metadata(MemberDiskUpdate),
    PoolNode(UserDpRequest),
    Evacuate(DiskUuid),
}

struct Metadata {
    calls: Arc<Mutex<Vec<Call>>>,
    fail: bool,
}

#[async_trait]
impl MetadataService for Metadata {
    async fn update_member_disk(
        &self,
        _disk: &DiskUuid,
        update: &MemberDiskUpdate,
    ) -> Result<(), MetadataError> {
        if self.fail {
            return Err(MetadataError::new("SDB unavailable"));
        }
        self.calls.lock().await.push(Call::Metadata(update.clone()));
        Ok(())
    }
}

struct Nodes {
    calls: Arc<Mutex<Vec<Call>>>,
    block_down: AtomicBool,
    down_started: Semaphore,
    down_release: Semaphore,
    down_applied: Semaphore,
    down_failures: AtomicUsize,
    down_attempts: AtomicUsize,
}

impl Nodes {
    async fn wait_down(&self) {
        self.down_applied.acquire().await.unwrap().forget();
    }

    fn block_down(&self) {
        self.block_down.store(true, Ordering::SeqCst);
    }

    async fn wait_down_started(&self) {
        self.down_started.acquire().await.unwrap().forget();
    }

    fn release_down(&self) {
        self.down_release.add_permits(1);
    }

    fn fail_next_down(&self, attempts: usize) {
        self.down_failures.store(attempts, Ordering::SeqCst);
    }

    fn down_attempts(&self) -> usize {
        self.down_attempts.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl PoolNodeService for Nodes {
    async fn broadcast(
        &self,
        cancel: &CancellationToken,
        request: UserDpRequest,
    ) -> Result<(), PoolNodeError> {
        if cancel.is_cancelled() {
            return Err(PoolNodeError::Cancelled);
        }
        let is_down = matches!(
            request,
            UserDpRequest::SetDiskState {
                state: DiskIoState::Down,
                ..
            }
        );
        if is_down {
            self.down_attempts.fetch_add(1, Ordering::SeqCst);
            self.down_started.add_permits(1);
            if self.block_down.load(Ordering::SeqCst) {
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err(PoolNodeError::Cancelled),
                    permit = self.down_release.acquire() => permit.unwrap().forget(),
                }
            }
            if self
                .down_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(PoolNodeError::failed("temporary user_dp failure"));
            }
            self.down_applied.add_permits(1);
        }
        self.calls.lock().await.push(Call::PoolNode(request));
        Ok(())
    }
}

struct VirtualDisks {
    calls: Arc<Mutex<Vec<Call>>>,
    has_references: AtomicBool,
    clear_references_on_success: AtomicBool,
    reference_queries: AtomicUsize,
    controlled: bool,
    started: Semaphore,
    release: Semaphore,
    call_count: AtomicUsize,
    active: AtomicUsize,
    max_active: AtomicUsize,
}

impl VirtualDisks {
    async fn wait_started(&self) {
        self.started.acquire().await.unwrap().forget();
    }

    fn release_one(&self) {
        self.release.add_permits(1);
    }

    fn call_count(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }

    fn reference_queries(&self) -> usize {
        self.reference_queries.load(Ordering::SeqCst)
    }

    fn keep_references_after_success(&self) {
        self.clear_references_on_success
            .store(false, Ordering::SeqCst);
    }
}

#[async_trait]
impl VirtualDiskService for VirtualDisks {
    async fn has_references(&self, _disk: &DiskUuid) -> Result<bool, VirtualDiskError> {
        self.reference_queries.fetch_add(1, Ordering::SeqCst);
        Ok(self.has_references.load(Ordering::SeqCst))
    }

    async fn evacuate(
        &self,
        cancel: &CancellationToken,
        disk: &DiskUuid,
    ) -> Result<(), VirtualDiskError> {
        self.calls.lock().await.push(Call::Evacuate(disk.clone()));
        self.call_count.fetch_add(1, Ordering::SeqCst);
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        self.started.add_permits(1);

        let result = if self.controlled {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Err(VirtualDiskError::Cancelled),
                permit = self.release.acquire() => {
                    permit.unwrap().forget();
                    Ok(())
                }
            }
        } else if cancel.is_cancelled() {
            Err(VirtualDiskError::Cancelled)
        } else {
            Ok(())
        };

        self.active.fetch_sub(1, Ordering::SeqCst);
        if result.is_ok() && self.clear_references_on_success.load(Ordering::SeqCst) {
            self.has_references.store(false, Ordering::SeqCst);
        }
        result
    }
}

fn member() -> MemberDisk {
    let mut disk = MemberDisk::new(
        DiskUuid::new("disk-1"),
        "pool-1",
        "tier-ssd",
        "ssd",
        GIB,
        vec![FailureDomain::new("node", "node-1")],
    )
    .unwrap();
    disk.mark_io_up().unwrap();
    disk
}

type RuntimeParts = (
    MemberDiskClient,
    tokio::task::JoinHandle<()>,
    Arc<Mutex<Vec<Call>>>,
    Arc<Nodes>,
    Arc<VirtualDisks>,
);

fn runtime(
    recovery_window: std::time::Duration,
    controlled_evacuation: bool,
    fail_metadata: bool,
) -> RuntimeParts {
    let calls = Arc::new(Mutex::new(Vec::new()));
    let nodes = Arc::new(Nodes {
        calls: calls.clone(),
        block_down: AtomicBool::new(false),
        down_started: Semaphore::new(0),
        down_release: Semaphore::new(0),
        down_applied: Semaphore::new(0),
        down_failures: AtomicUsize::new(0),
        down_attempts: AtomicUsize::new(0),
    });
    let virtual_disks = Arc::new(VirtualDisks {
        calls: calls.clone(),
        has_references: AtomicBool::new(true),
        clear_references_on_success: AtomicBool::new(true),
        reference_queries: AtomicUsize::new(0),
        controlled: controlled_evacuation,
        started: Semaphore::new(0),
        release: Semaphore::new(0),
        call_count: AtomicUsize::new(0),
        active: AtomicUsize::new(0),
        max_active: AtomicUsize::new(0),
    });
    let service = MemberDiskService::new(
        vec![member()],
        Arc::new(Metadata {
            calls: calls.clone(),
            fail: fail_metadata,
        }),
        nodes.clone(),
        virtual_disks.clone(),
        recovery_window,
    );
    let (client, task) = service.spawn(16);
    (client, task, calls, nodes, virtual_disks)
}

fn physical(state: PhysicalState, observed_at: u64) -> MemberDiskEvent {
    MemberDiskEvent::PhysicalChanged {
        disk: DiskUuid::new("disk-1"),
        state,
        observed_at: EpochMillis::new(observed_at),
    }
}

async fn stop(client: MemberDiskClient, task: tokio::task::JoinHandle<()>) {
    drop(client);
    task.await.unwrap();
}

#[tokio::test]
async fn offline_is_driven_to_removed_by_the_state_table() {
    let (client, task, calls, _, _) = runtime(std::time::Duration::ZERO, false, false);
    let disk = DiskUuid::new("disk-1");

    client
        .submit(physical(PhysicalState::Down, 1_000))
        .await
        .unwrap();
    client.wait_idle(disk.clone()).await.unwrap();

    assert_eq!(
        client.get(disk.clone()).await.unwrap().state(),
        MemberDiskState::Removed
    );
    assert_eq!(
        calls.lock().await.as_slice(),
        [
            Call::PoolNode(UserDpRequest::SetDiskState {
                disk: disk.clone(),
                state: DiskIoState::Down,
            }),
            Call::Metadata(MemberDiskUpdate::ApplyDown),
            Call::Metadata(MemberDiskUpdate::DisableAllocation),
            Call::Evacuate(disk.clone()),
            Call::Metadata(MemberDiskUpdate::Remove),
        ]
    );
    stop(client, task).await;
}

#[tokio::test]
async fn online_cancels_the_recovery_wait_and_returns_to_up_active() {
    let (client, task, _, nodes, virtual_disks) =
        runtime(std::time::Duration::from_secs(3_600), false, false);
    let disk = DiskUuid::new("disk-1");

    client
        .submit(physical(PhysicalState::Down, current_time()))
        .await
        .unwrap();
    nodes.wait_down().await;
    client
        .submit(physical(PhysicalState::Up, current_time()))
        .await
        .unwrap();
    client.wait_idle(disk.clone()).await.unwrap();

    assert_eq!(
        client.get(disk).await.unwrap().state(),
        MemberDiskState::UpActive
    );
    assert_eq!(virtual_disks.call_count(), 0);
    assert_eq!(
        virtual_disks.reference_queries(),
        0,
        "mandatory DOWN and replacement UP must not depend on VDM"
    );
    stop(client, task).await;
}

#[tokio::test]
async fn up_cannot_interrupt_the_required_down_broadcast() {
    let (client, task, calls, nodes, _) =
        runtime(std::time::Duration::from_secs(3_600), false, false);
    let disk = DiskUuid::new("disk-1");
    nodes.block_down();

    client
        .submit(physical(PhysicalState::Down, current_time()))
        .await
        .unwrap();
    nodes.wait_down_started().await;

    client
        .submit(physical(PhysicalState::Up, current_time()))
        .await
        .unwrap();
    nodes.release_down();
    client.wait_idle(disk.clone()).await.unwrap();

    let node_calls: Vec<_> = calls
        .lock()
        .await
        .iter()
        .filter_map(|call| match call {
            Call::PoolNode(request) => Some(request.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        node_calls,
        [
            UserDpRequest::SetDiskState {
                disk: disk.clone(),
                state: DiskIoState::Down,
            },
            UserDpRequest::OpenDisk(disk.clone()),
            UserDpRequest::SetDiskState {
                disk: disk.clone(),
                state: DiskIoState::Up,
            },
        ]
    );
    assert_eq!(
        client.get(disk).await.unwrap().state(),
        MemberDiskState::UpActive
    );
    stop(client, task).await;
}

#[tokio::test]
async fn repeated_down_is_merged_in_the_disk_slot() {
    let (client, task, _, nodes, _) = runtime(std::time::Duration::from_secs(3_600), false, false);
    let disk = DiskUuid::new("disk-1");
    nodes.block_down();

    client
        .submit(physical(PhysicalState::Down, current_time()))
        .await
        .unwrap();
    nodes.wait_down_started().await;
    client
        .submit(physical(PhysicalState::Down, current_time()))
        .await
        .unwrap();

    assert_eq!(nodes.down_attempts(), 1);
    nodes.release_down();

    client
        .submit(physical(PhysicalState::Up, current_time()))
        .await
        .unwrap();
    client.wait_idle(disk).await.unwrap();
    assert_eq!(nodes.down_attempts(), 1);
    stop(client, task).await;
}

#[tokio::test]
async fn shrink_replaces_the_recovery_wait_without_losing_its_intent() {
    let (client, task, _, nodes, virtual_disks) =
        runtime(std::time::Duration::from_secs(3_600), false, false);
    let disk = DiskUuid::new("disk-1");

    client
        .submit(physical(PhysicalState::Down, current_time()))
        .await
        .unwrap();
    nodes.wait_down().await;
    client
        .submit(MemberDiskEvent::Shrink { disk: disk.clone() })
        .await
        .unwrap();
    client.wait_idle(disk.clone()).await.unwrap();

    let removed = client.get(disk).await.unwrap();
    assert_eq!(removed.state(), MemberDiskState::Removed);
    assert!(removed.shrink_requested());
    assert_eq!(virtual_disks.call_count(), 1);
    stop(client, task).await;
}

#[tokio::test]
async fn disk_service_retries_a_failed_down_broadcast() {
    let (client, task, _, nodes, _) = runtime(std::time::Duration::ZERO, false, false);
    let disk = DiskUuid::new("disk-1");
    nodes.fail_next_down(1);

    client
        .submit(physical(PhysicalState::Down, current_time()))
        .await
        .unwrap();
    client.wait_idle(disk.clone()).await.unwrap();

    assert_eq!(nodes.down_attempts(), 2);
    assert_eq!(
        client.get(disk).await.unwrap().state(),
        MemberDiskState::Removed
    );
    stop(client, task).await;
}

#[tokio::test]
async fn dropping_the_client_drains_a_required_down_broadcast() {
    let (client, task, calls, nodes, _) = runtime(std::time::Duration::ZERO, false, false);
    nodes.block_down();

    client
        .submit(physical(PhysicalState::Down, current_time()))
        .await
        .unwrap();
    nodes.wait_down_started().await;
    drop(client);

    let mut task = task;
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), &mut task)
            .await
            .is_err(),
        "drain must wait for the required DOWN boundary"
    );

    nodes.release_down();
    tokio::time::timeout(std::time::Duration::from_secs(1), task)
        .await
        .expect("MemberDisk service must finish draining")
        .unwrap();
    assert!(
        calls
            .lock()
            .await
            .contains(&Call::Metadata(MemberDiskUpdate::ApplyDown))
    );
}

#[tokio::test]
async fn aborting_the_root_task_force_drops_a_blocked_down_broadcast() {
    let (client, task, calls, nodes, _) = runtime(std::time::Duration::ZERO, false, false);
    nodes.block_down();

    client
        .submit(physical(PhysicalState::Down, current_time()))
        .await
        .unwrap();
    nodes.wait_down_started().await;
    drop(client);

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(
        !calls
            .lock()
            .await
            .contains(&Call::Metadata(MemberDiskUpdate::ApplyDown))
    );
}

#[tokio::test]
async fn shrink_drains_before_stopping_io_and_removing_the_disk() {
    let (client, task, calls, _, _) = runtime(std::time::Duration::ZERO, false, false);
    let disk = DiskUuid::new("disk-1");

    client
        .submit(MemberDiskEvent::Shrink { disk: disk.clone() })
        .await
        .unwrap();
    client.wait_idle(disk.clone()).await.unwrap();

    assert_eq!(
        calls.lock().await.as_slice(),
        [
            Call::Metadata(MemberDiskUpdate::RequestShrink),
            Call::Metadata(MemberDiskUpdate::DisableAllocation),
            Call::Evacuate(disk.clone()),
            Call::PoolNode(UserDpRequest::SetDiskState {
                disk: disk.clone(),
                state: DiskIoState::Down,
            }),
            Call::Metadata(MemberDiskUpdate::ApplyDown),
            Call::Metadata(MemberDiskUpdate::Remove),
        ]
    );
    let removed = client.get(disk.clone()).await.unwrap();
    assert_eq!(removed.state(), MemberDiskState::Removed);
    assert!(removed.shrink_requested());
    stop(client, task).await;
}

#[tokio::test]
async fn a_new_up_event_rejoins_a_disk_removed_by_shrink() {
    let (client, task, _, _, _) = runtime(std::time::Duration::ZERO, false, false);
    let disk = DiskUuid::new("disk-1");

    client
        .submit(MemberDiskEvent::Shrink { disk: disk.clone() })
        .await
        .unwrap();
    client.wait_idle(disk.clone()).await.unwrap();
    assert_eq!(
        client.get(disk.clone()).await.unwrap().state(),
        MemberDiskState::Removed
    );

    // The physical value was already Up before removal. This new event is
    // nevertheless an explicit rejoin request and clears the Shrink intent.
    client
        .submit(physical(PhysicalState::Up, 3_000))
        .await
        .unwrap();
    client.wait_idle(disk.clone()).await.unwrap();

    let rejoined = client.get(disk).await.unwrap();
    assert_eq!(rejoined.state(), MemberDiskState::UpActive);
    assert!(!rejoined.shrink_requested());
    stop(client, task).await;
}

#[tokio::test]
async fn down_during_shrink_settles_then_resumes_from_the_real_state() {
    let (client, task, _, nodes, virtual_disks) = runtime(std::time::Duration::ZERO, true, false);
    let disk = DiskUuid::new("disk-1");

    client
        .submit(MemberDiskEvent::Shrink { disk: disk.clone() })
        .await
        .unwrap();
    virtual_disks.wait_started().await;

    client
        .submit(physical(PhysicalState::Down, 2_000))
        .await
        .unwrap();
    nodes.wait_down().await;
    virtual_disks.wait_started().await;

    assert_eq!(virtual_disks.call_count(), 2);
    assert_eq!(virtual_disks.max_active.load(Ordering::SeqCst), 1);

    virtual_disks.release_one();
    client.wait_idle(disk.clone()).await.unwrap();
    assert_eq!(
        client.get(disk).await.unwrap().state(),
        MemberDiskState::Removed
    );
    stop(client, task).await;
}

#[tokio::test]
async fn failed_sdb_change_is_not_published_to_memory() {
    let (client, task, _, _, _) = runtime(std::time::Duration::ZERO, false, true);
    let disk = DiskUuid::new("disk-1");

    client
        .submit(physical(PhysicalState::Down, 1_000))
        .await
        .unwrap();
    assert_eq!(
        client.wait_idle(disk.clone()).await,
        Err(MemberDiskServiceError::Metadata(MetadataError::new(
            "SDB unavailable"
        )))
    );
    assert_eq!(
        client.get(disk).await.unwrap().state(),
        MemberDiskState::UpActive
    );
    stop(client, task).await;
}

#[tokio::test]
async fn evacuation_must_reach_its_declared_finish_state() {
    let (client, task, _, _, virtual_disks) = runtime(std::time::Duration::ZERO, false, false);
    let disk = DiskUuid::new("disk-1");
    virtual_disks.keep_references_after_success();

    client
        .submit(MemberDiskEvent::Shrink { disk: disk.clone() })
        .await
        .unwrap();

    assert!(matches!(
        client.wait_idle(disk.clone()).await,
        Err(MemberDiskServiceError::TransitionIncomplete(_))
    ));
    assert_eq!(
        client.get(disk).await.unwrap().state(),
        MemberDiskState::UpInactive
    );
    stop(client, task).await;
}

fn current_time() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX)
}
