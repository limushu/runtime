use super::*;
use crate::service::{CallError, CommandReceipt, Lifecycle, Service, ServiceActivity, TaskState};
use async_trait::async_trait;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct MetadataFake {
    commits: Mutex<Vec<MemberDiskMutation>>,
    fail_next: AtomicBool,
}

#[async_trait]
impl MemberDiskMetadata for MetadataFake {
    async fn commit(
        &self,
        _disk: &DiskUuid,
        mutation: &MemberDiskMutation,
    ) -> Result<(), PortError> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            return Err(PortError("SDB unavailable".into()));
        }
        self.commits.lock().unwrap().push(mutation.clone());
        Ok(())
    }
}

#[derive(Default)]
struct PoolNodesFake {
    downs: AtomicUsize,
    opens: AtomicUsize,
    ups: AtomicUsize,
}

#[async_trait]
impl PoolNodes for PoolNodesFake {
    async fn set_disk_down(&self, _disk: &DiskUuid) -> Result<(), PortError> {
        self.downs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn open_disk(
        &self,
        _disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), PortError> {
        if cancel.is_cancelled() {
            return Err(PortError("cancelled".into()));
        }
        self.opens.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn publish_disk_up(
        &self,
        _disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), PortError> {
        if cancel.is_cancelled() {
            return Err(PortError("cancelled".into()));
        }
        self.ups.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(Default)]
struct VirtualDisksFake {
    references: AtomicBool,
    evacuations: AtomicUsize,
    evacuation_delay_ms: AtomicUsize,
}

#[async_trait]
impl VirtualDisks for VirtualDisksFake {
    async fn has_references(&self, _disk: &DiskUuid) -> Result<bool, PortError> {
        Ok(self.references.load(Ordering::SeqCst))
    }

    async fn evacuate(
        &self,
        _disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), PortError> {
        tokio::select! {
            _ = cancel.cancelled() => Err(PortError("cancelled".into())),
            _ = tokio::time::sleep(Duration::from_millis(
                self.evacuation_delay_ms.load(Ordering::SeqCst) as u64
            )) => {
                self.evacuations.fetch_add(1, Ordering::SeqCst);
                self.references.store(false, Ordering::SeqCst);
                Ok(())
            }
        }
    }
}

struct Fixture {
    service: MemberDiskService,
    disk: DiskUuid,
    metadata: Arc<MetadataFake>,
    nodes: Arc<PoolNodesFake>,
    vds: Arc<VirtualDisksFake>,
}

fn fixture(recovery_window: Duration) -> Fixture {
    let disk = DiskUuid::new("disk-1");
    let metadata = Arc::new(MetadataFake::default());
    let nodes = Arc::new(PoolNodesFake::default());
    let vds = Arc::new(VirtualDisksFake::default());
    let service = MemberDiskService::new(
        [MemberDiskSeed {
            uuid: disk.clone(),
            pool_id: "pool-1".into(),
        }],
        metadata.clone(),
        nodes.clone(),
        vds.clone(),
        MemberDiskConfig {
            recovery_window,
            mandatory_retry_delay: Duration::from_millis(1),
        },
    );
    Fixture {
        service,
        disk,
        metadata,
        nodes,
        vds,
    }
}

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

async fn get_state(
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

#[tokio::test]
async fn query_uses_the_service_boundary() {
    let fixture = fixture(Duration::ZERO);
    let running = fixture.service.start(Default::default());

    assert_eq!(
        get_state(&running.endpoint, &fixture.disk).await,
        MemberDiskState::UpActive
    );
    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn duplicate_down_joins_and_up_cooperatively_replaces_it() {
    let fixture = fixture(Duration::from_secs(60));
    let nodes = fixture.nodes.clone();
    let disk = fixture.disk.clone();
    let mut running = fixture.service.start(Default::default());

    let first = running
        .endpoint
        .submit(MemberDiskCommand::DiskDown {
            disk: disk.clone(),
            observed_at: now_millis(),
        })
        .await
        .unwrap();
    assert!(matches!(first, CommandReceipt::Started { .. }));

    tokio::time::timeout(
        Duration::from_secs(1),
        running.observer.wait_for(|snapshot| {
            snapshot.tasks.iter().any(|task| {
                task.subject == "disk-1"
                    && task.detail.as_deref() == Some("stop IO and publish disk DOWN")
            })
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let duplicate = running
        .endpoint
        .submit(MemberDiskCommand::DiskDown {
            disk: disk.clone(),
            observed_at: now_millis(),
        })
        .await
        .unwrap();
    assert!(matches!(duplicate, CommandReceipt::Joined { .. }));

    let reply = running
        .endpoint
        .call(MemberDiskCommand::DiskUp { disk: disk.clone() })
        .await
        .unwrap();
    assert_eq!(reply.state, MemberDiskState::UpActive);
    assert_eq!(nodes.downs.load(Ordering::SeqCst), 1);
    assert_eq!(nodes.opens.load(Ordering::SeqCst), 1);
    assert_eq!(nodes.ups.load(Ordering::SeqCst), 1);
    assert!(running.observer.snapshot().tasks.is_empty());

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn shrink_is_an_explicit_state_machine_to_removed() {
    let fixture = fixture(Duration::ZERO);
    fixture.vds.references.store(true, Ordering::SeqCst);
    let metadata = fixture.metadata.clone();
    let vds = fixture.vds.clone();
    let disk = fixture.disk.clone();
    let running = fixture.service.start(Default::default());

    let reply = running
        .endpoint
        .call(MemberDiskCommand::Shrink { disk: disk.clone() })
        .await
        .unwrap();
    assert_eq!(reply.state, MemberDiskState::Removed);
    assert_eq!(vds.evacuations.load(Ordering::SeqCst), 1);
    assert_eq!(
        *metadata.commits.lock().unwrap(),
        vec![
            MemberDiskMutation::RequestShrink,
            MemberDiskMutation::DisableAllocation,
            MemberDiskMutation::SetIo(DiskIoState::Down),
            MemberDiskMutation::Remove,
        ]
    );

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn down_during_shrink_stops_io_then_continues_the_same_removal_target() {
    let fixture = fixture(Duration::ZERO);
    fixture.vds.references.store(true, Ordering::SeqCst);
    fixture.vds.evacuation_delay_ms.store(100, Ordering::SeqCst);
    let disk = fixture.disk.clone();
    let nodes = fixture.nodes.clone();
    let vds = fixture.vds.clone();
    let mut running = fixture.service.start(Default::default());

    running
        .endpoint
        .submit(MemberDiskCommand::Shrink { disk: disk.clone() })
        .await
        .unwrap();

    tokio::time::timeout(
        Duration::from_secs(1),
        running.observer.wait_for(|snapshot| {
            snapshot
                .tasks
                .iter()
                .any(|task| task.detail.as_deref() == Some("disable new BLK allocation"))
        }),
    )
    .await
    .unwrap()
    .unwrap();

    let reply = running
        .endpoint
        .call(MemberDiskCommand::DiskDown {
            disk: disk.clone(),
            observed_at: now_millis(),
        })
        .await
        .unwrap();

    assert_eq!(reply.state, MemberDiskState::Removed);
    assert_eq!(nodes.downs.load(Ordering::SeqCst), 1);
    assert_eq!(vds.evacuations.load(Ordering::SeqCst), 1);

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn failed_sdb_commit_is_not_published_in_memory() {
    let fixture = fixture(Duration::ZERO);
    fixture.metadata.fail_next.store(true, Ordering::SeqCst);
    let disk = fixture.disk.clone();
    let running = fixture.service.start(Default::default());

    let result = running
        .endpoint
        .call(MemberDiskCommand::DiskDown {
            disk: disk.clone(),
            observed_at: now_millis(),
        })
        .await;
    assert!(matches!(
        result,
        Err(CallError::Business(MemberDiskServiceError::Metadata(_)))
    ));
    assert_eq!(
        get_state(&running.endpoint, &disk).await,
        MemberDiskState::UpActive
    );
    assert_eq!(
        running.observer.snapshot().tasks[0].state,
        TaskState::Failed
    );

    running.control.stop().await.unwrap();
    running.task.await.unwrap();
}

#[tokio::test]
async fn lifecycle_is_framework_owned_and_queries_remain_available_while_paused() {
    let fixture = fixture(Duration::ZERO);
    let disk = fixture.disk.clone();
    let running = fixture.service.start(Default::default());

    running.control.pause().await.unwrap();
    assert_eq!(running.observer.snapshot().lifecycle, Lifecycle::Paused);
    assert_eq!(
        get_state(&running.endpoint, &disk).await,
        MemberDiskState::UpActive
    );
    assert!(matches!(
        running
            .endpoint
            .submit(MemberDiskCommand::Shrink { disk })
            .await,
        Err(CallError::Unavailable(Lifecycle::Paused))
    ));
    assert_eq!(running.observer.snapshot().activity, ServiceActivity::Idle);

    running.control.drain().await.unwrap();
    running.task.await.unwrap();
}
