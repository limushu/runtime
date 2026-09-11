use super::*;
use crate::service::{CallError, Lifecycle, Service, ServiceActivity};
use async_trait::async_trait;
use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct MetadataFake {
    batches: Mutex<Vec<Vec<MemberDiskCommit>>>,
    fail_next: AtomicBool,
}

#[async_trait]
impl MemberDiskMetadata for MetadataFake {
    async fn commit(&self, commits: Vec<MemberDiskCommit>) -> Result<(), PortError> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(PortError("SDB unavailable".into()));
        }
        self.batches.lock().unwrap().push(commits);
        Ok(())
    }
}

#[derive(Default)]
struct PoolNodesFake {
    state_batches: Mutex<Vec<Vec<DiskStateChange>>>,
    open_batches: Mutex<Vec<Vec<DiskUuid>>>,
    open_failures: Mutex<HashSet<DiskUuid>>,
    push_failures_left: AtomicUsize,
    push_attempts: AtomicUsize,
}

#[async_trait]
impl PoolNodes for PoolNodesFake {
    async fn open_disks(&self, disks: Vec<DiskUuid>) -> Result<Vec<DiskOpenResult>, PortError> {
        self.open_batches.lock().unwrap().push(disks.clone());
        let failures = self.open_failures.lock().unwrap().clone();
        Ok(disks
            .into_iter()
            .map(|disk| {
                let result = if failures.contains(&disk) {
                    Err(PortError(format!("failed to open {disk}")))
                } else {
                    Ok(())
                };
                DiskOpenResult { disk, result }
            })
            .collect())
    }

    async fn push_disk_states(&self, changes: Vec<DiskStateChange>) -> Result<(), PortError> {
        self.push_attempts.fetch_add(1, Ordering::SeqCst);
        if self
            .push_failures_left
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok()
        {
            return Err(PortError("temporary network failure".into()));
        }
        self.state_batches.lock().unwrap().push(changes);
        Ok(())
    }
}

#[derive(Default)]
struct VirtualDisksFake {
    evacuations: AtomicUsize,
    in_flight: AtomicUsize,
    max_in_flight: AtomicUsize,
    delay_ms: AtomicUsize,
}

#[async_trait]
impl VirtualDisks for VirtualDisksFake {
    async fn evacuate(
        &self,
        _disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), PortError> {
        let in_flight = self.in_flight.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_in_flight.fetch_max(in_flight, Ordering::SeqCst);
        let result = tokio::select! {
            _ = cancel.cancelled() => Err(PortError("cancelled".into())),
            _ = tokio::time::sleep(Duration::from_millis(
                self.delay_ms.load(Ordering::SeqCst) as u64
            )) => {
                self.evacuations.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        };
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        result
    }
}

struct Fixture {
    service: MemberDiskService,
    disks: Vec<DiskUuid>,
    metadata: Arc<MetadataFake>,
    nodes: Arc<PoolNodesFake>,
    virtual_disks: Arc<VirtualDisksFake>,
}

fn fixture(count: usize, recovery_window: Duration) -> Fixture {
    let disks: Vec<_> = (1..=count)
        .map(|index| DiskUuid::new(format!("disk-{index}")))
        .collect();
    let metadata = Arc::new(MetadataFake::default());
    let nodes = Arc::new(PoolNodesFake::default());
    let virtual_disks = Arc::new(VirtualDisksFake::default());
    let service = MemberDiskService::new(
        disks.iter().cloned().map(|uuid| MemberDiskSeed {
            uuid,
            pool_id: "pool-1".into(),
        }),
        metadata.clone(),
        nodes.clone(),
        virtual_disks.clone(),
        MemberDiskConfig {
            recovery_window,
            mandatory_retry_delay: Duration::from_millis(1),
        },
    );
    Fixture {
        service,
        disks,
        metadata,
        nodes,
        virtual_disks,
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

async fn state(
    endpoint: &crate::service::ServiceEndpoint<MemberDiskService>,
    disk: &DiskUuid,
) -> MemberDiskState {
    match endpoint
        .query(MemberDiskQuery::Get(disk.clone()))
        .await
        .unwrap()
    {
        MemberDiskQueryReply::One(view) => view.state,
        MemberDiskQueryReply::List(_) => panic!("expected one disk"),
    }
}

async fn wait_for_state(
    endpoint: &crate::service::ServiceEndpoint<MemberDiskService>,
    disk: &DiskUuid,
    expected: MemberDiskState,
) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if state(endpoint, disk).await == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

fn disk_result(
    reply: &MemberDiskReply,
    disk: &DiskUuid,
) -> Result<MemberDiskState, MemberDiskServiceError> {
    reply.outcome(disk).unwrap().result.clone()
}

#[tokio::test]
async fn query_uses_the_service_boundary() {
    let fixture = fixture(1, Duration::ZERO);
    let disk = fixture.disks[0].clone();
    let running = fixture.service.start(Default::default());

    assert_eq!(
        state(&running.endpoint, &disk).await,
        MemberDiskState::UpActive
    );

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn offline_keeps_the_batch_and_evacuates_disks_concurrently() {
    let fixture = fixture(3, Duration::ZERO);
    fixture.virtual_disks.delay_ms.store(20, Ordering::SeqCst);
    let disks = fixture.disks.clone();
    let nodes = fixture.nodes.clone();
    let metadata = fixture.metadata.clone();
    let virtual_disks = fixture.virtual_disks.clone();
    let running = fixture.service.start(Default::default());

    let reply = running
        .endpoint
        .call(MemberDiskCommand::DiskDown {
            disks: disks.clone(),
            observed_at: now_millis(),
        })
        .await
        .unwrap();

    assert!(
        disks
            .iter()
            .all(|disk| disk_result(&reply, disk) == Ok(MemberDiskState::Removed))
    );
    assert_eq!(nodes.state_batches.lock().unwrap().len(), 1);
    assert_eq!(nodes.state_batches.lock().unwrap()[0].len(), 3);
    assert_eq!(metadata.batches.lock().unwrap()[0].len(), 3);
    assert_eq!(virtual_disks.max_in_flight.load(Ordering::SeqCst), 3);

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn online_cancels_offline_at_a_stable_boundary() {
    let fixture = fixture(1, Duration::from_secs(60));
    let disk = fixture.disks[0].clone();
    let nodes = fixture.nodes.clone();
    let running = fixture.service.start(Default::default());

    running
        .endpoint
        .submit(MemberDiskCommand::DiskDown {
            disks: vec![disk.clone()],
            observed_at: now_millis(),
        })
        .await
        .unwrap();
    wait_for_state(&running.endpoint, &disk, MemberDiskState::DownActive).await;

    let reply = running
        .endpoint
        .call(MemberDiskCommand::DiskUp {
            disks: vec![disk.clone()],
        })
        .await
        .unwrap();

    assert_eq!(disk_result(&reply, &disk), Ok(MemberDiskState::UpActive));
    assert_eq!(nodes.open_batches.lock().unwrap().len(), 1);
    assert_eq!(
        nodes
            .state_batches
            .lock()
            .unwrap()
            .iter()
            .filter(|batch| batch[0].state == DiskIoState::Down)
            .count(),
        1
    );

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn online_opens_once_and_publishes_only_successful_disks() {
    let fixture = fixture(3, Duration::from_secs(60));
    let disks = fixture.disks.clone();
    let nodes = fixture.nodes.clone();
    let running = fixture.service.start(Default::default());

    running
        .endpoint
        .submit(MemberDiskCommand::DiskDown {
            disks: disks.clone(),
            observed_at: now_millis(),
        })
        .await
        .unwrap();
    for disk in &disks {
        wait_for_state(&running.endpoint, disk, MemberDiskState::DownActive).await;
    }

    nodes.state_batches.lock().unwrap().clear();
    nodes.open_failures.lock().unwrap().insert(disks[1].clone());

    let reply = running
        .endpoint
        .call(MemberDiskCommand::DiskUp {
            disks: disks.clone(),
        })
        .await
        .unwrap();

    assert_eq!(
        disk_result(&reply, &disks[0]),
        Ok(MemberDiskState::UpActive)
    );
    assert!(matches!(
        disk_result(&reply, &disks[1]),
        Err(MemberDiskServiceError::PoolNodes(_))
    ));
    assert_eq!(
        disk_result(&reply, &disks[2]),
        Ok(MemberDiskState::UpActive)
    );
    assert_eq!(*nodes.open_batches.lock().unwrap(), vec![disks.clone()]);
    assert_eq!(nodes.state_batches.lock().unwrap().len(), 1);
    assert_eq!(nodes.state_batches.lock().unwrap()[0].len(), 2);

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn shrink_is_one_explicit_batch_flow() {
    let fixture = fixture(2, Duration::ZERO);
    let disks = fixture.disks.clone();
    let nodes = fixture.nodes.clone();
    let metadata = fixture.metadata.clone();
    let running = fixture.service.start(Default::default());

    let reply = running
        .endpoint
        .call(MemberDiskCommand::Shrink {
            disks: disks.clone(),
        })
        .await
        .unwrap();

    assert!(
        disks
            .iter()
            .all(|disk| disk_result(&reply, disk) == Ok(MemberDiskState::Removed))
    );
    assert_eq!(nodes.state_batches.lock().unwrap().len(), 1);
    {
        let batches = metadata.batches.lock().unwrap();
        assert_eq!(batches.len(), 4);
        assert!(batches.iter().all(|batch| batch.len() == disks.len()));
        assert!(
            batches[0]
                .iter()
                .all(|commit| commit.mutation == MemberDiskMutation::RequestShrink)
        );
        assert!(
            batches[1]
                .iter()
                .all(|commit| commit.mutation == MemberDiskMutation::DisableAllocation)
        );
    }

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn down_during_shrink_takes_over_and_keeps_the_removal_target() {
    let fixture = fixture(1, Duration::from_secs(60));
    fixture.virtual_disks.delay_ms.store(100, Ordering::SeqCst);
    let disk = fixture.disks[0].clone();
    let running = fixture.service.start(Default::default());

    running
        .endpoint
        .submit(MemberDiskCommand::Shrink {
            disks: vec![disk.clone()],
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if running
                .observer
                .snapshot()
                .tasks
                .iter()
                .any(|task| task.detail.as_deref() == Some("evacuating VirtualDisk references"))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();

    let reply = running
        .endpoint
        .call(MemberDiskCommand::DiskDown {
            disks: vec![disk.clone()],
            observed_at: now_millis(),
        })
        .await
        .unwrap();

    assert_eq!(disk_result(&reply, &disk), Ok(MemberDiskState::Removed));

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn online_waits_for_shrink_and_then_rejoins() {
    let fixture = fixture(1, Duration::ZERO);
    fixture.virtual_disks.delay_ms.store(20, Ordering::SeqCst);
    let disk = fixture.disks[0].clone();
    let nodes = fixture.nodes.clone();
    let running = fixture.service.start(Default::default());

    running
        .endpoint
        .submit(MemberDiskCommand::Shrink {
            disks: vec![disk.clone()],
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if running
                .observer
                .snapshot()
                .tasks
                .iter()
                .any(|task| task.kind == "shrink")
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();

    let reply = running
        .endpoint
        .call(MemberDiskCommand::DiskUp {
            disks: vec![disk.clone()],
        })
        .await
        .unwrap();

    assert_eq!(disk_result(&reply, &disk), Ok(MemberDiskState::UpActive));
    assert_eq!(nodes.open_batches.lock().unwrap().len(), 1);

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn duplicate_operation_joins_the_existing_disk_operation() {
    let fixture = fixture(1, Duration::from_secs(60));
    let disk = fixture.disks[0].clone();
    let nodes = fixture.nodes.clone();
    let running = fixture.service.start(Default::default());

    running
        .endpoint
        .submit(MemberDiskCommand::DiskDown {
            disks: vec![disk.clone()],
            observed_at: now_millis(),
        })
        .await
        .unwrap();
    wait_for_state(&running.endpoint, &disk, MemberDiskState::DownActive).await;

    let duplicate = running.endpoint.call(MemberDiskCommand::DiskDown {
        disks: vec![disk.clone()],
        observed_at: now_millis(),
    });
    let online = running.endpoint.call(MemberDiskCommand::DiskUp {
        disks: vec![disk.clone()],
    });
    let (duplicate, online) = tokio::join!(duplicate, online);

    assert_eq!(
        disk_result(&duplicate.unwrap(), &disk),
        Err(MemberDiskServiceError::Cancelled)
    );
    assert_eq!(
        disk_result(&online.unwrap(), &disk),
        Ok(MemberDiskState::UpActive)
    );
    assert_eq!(
        nodes
            .state_batches
            .lock()
            .unwrap()
            .iter()
            .filter(|batch| batch[0].state == DiskIoState::Down)
            .count(),
        1
    );

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn mandatory_down_publication_retries_before_sdb_commit() {
    let fixture = fixture(1, Duration::ZERO);
    fixture.nodes.push_failures_left.store(2, Ordering::SeqCst);
    let disk = fixture.disks[0].clone();
    let nodes = fixture.nodes.clone();
    let running = fixture.service.start(Default::default());

    let reply = running
        .endpoint
        .call(MemberDiskCommand::DiskDown {
            disks: vec![disk.clone()],
            observed_at: now_millis(),
        })
        .await
        .unwrap();

    assert_eq!(disk_result(&reply, &disk), Ok(MemberDiskState::Removed));
    assert_eq!(nodes.push_attempts.load(Ordering::SeqCst), 3);

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn failed_sdb_commit_is_not_published_in_memory() {
    let fixture = fixture(1, Duration::ZERO);
    fixture.metadata.fail_next.store(true, Ordering::SeqCst);
    let disk = fixture.disks[0].clone();
    let running = fixture.service.start(Default::default());

    let reply = running
        .endpoint
        .call(MemberDiskCommand::DiskDown {
            disks: vec![disk.clone()],
            observed_at: now_millis(),
        })
        .await
        .unwrap();

    assert!(matches!(
        disk_result(&reply, &disk),
        Err(MemberDiskServiceError::Metadata(_))
    ));
    assert_eq!(
        state(&running.endpoint, &disk).await,
        MemberDiskState::UpActive
    );

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn lifecycle_remains_owned_by_the_service_sdk() {
    let fixture = fixture(1, Duration::ZERO);
    let disk = fixture.disks[0].clone();
    let running = fixture.service.start(Default::default());

    running.control.pause().await.unwrap();
    assert_eq!(running.observer.snapshot().lifecycle, Lifecycle::Paused);
    assert_eq!(
        state(&running.endpoint, &disk).await,
        MemberDiskState::UpActive
    );
    assert!(matches!(
        running
            .endpoint
            .submit(MemberDiskCommand::Shrink { disks: vec![disk] })
            .await,
        Err(CallError::Unavailable(Lifecycle::Paused))
    ));
    assert_eq!(running.observer.snapshot().activity, ServiceActivity::Idle);

    running.control.drain().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn aborting_the_root_task_drops_all_domain_operations() {
    let fixture = fixture(1, Duration::from_secs(60));
    let disk = fixture.disks[0].clone();
    let running = fixture.service.start(Default::default());

    running
        .endpoint
        .submit(MemberDiskCommand::DiskDown {
            disks: vec![disk.clone()],
            observed_at: now_millis(),
        })
        .await
        .unwrap();
    wait_for_state(&running.endpoint, &disk, MemberDiskState::DownActive).await;
    assert_eq!(running.observer.snapshot().tasks.len(), 1);

    running.abort();
    assert!(running.task.await.is_err());
    assert!(running.observer.snapshot().tasks.is_empty());
}
