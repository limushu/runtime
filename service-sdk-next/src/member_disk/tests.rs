use async_trait::async_trait;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::runtime::{
    Activity, CallError, CommandId, CommandReply, IncomingCommand, Lifecycle, NewRuns, Service,
    ServiceConfig,
};

use super::*;

#[derive(Default)]
struct TestPorts {
    commits: Mutex<Vec<Vec<MemberDiskCommit>>>,
    broadcasts: Mutex<Vec<Vec<DiskStateChange>>>,
}

#[async_trait]
impl MemberDiskMetadata for TestPorts {
    async fn commit(&self, changes: Vec<MemberDiskCommit>) -> Vec<Result<(), PortError>> {
        self.commits.lock().unwrap().push(changes.clone());
        changes.into_iter().map(|_| Ok(())).collect()
    }
}

#[derive(Default)]
struct FailSecondCommitOnce {
    calls: AtomicUsize,
    commits: Mutex<Vec<Vec<MemberDiskCommit>>>,
}

#[async_trait]
impl MemberDiskMetadata for FailSecondCommitOnce {
    async fn commit(&self, changes: Vec<MemberDiskCommit>) -> Vec<Result<(), PortError>> {
        let call = self.calls.fetch_add(1, Ordering::Relaxed);
        self.commits.lock().unwrap().push(changes.clone());
        changes
            .into_iter()
            .map(|_| {
                if call == 1 {
                    Err(PortError("transient SDB failure".into()))
                } else {
                    Ok(())
                }
            })
            .collect()
    }
}

#[async_trait]
impl PoolNodes for TestPorts {
    async fn open_disks(&self, disks: Vec<DiskUuid>) -> Result<Vec<DiskOpenResult>, PortError> {
        Ok(disks
            .into_iter()
            .map(|disk| DiskOpenResult {
                disk,
                result: Ok(()),
            })
            .collect())
    }

    async fn push_disk_states(&self, changes: Vec<DiskStateChange>) -> Result<(), PortError> {
        self.broadcasts.lock().unwrap().push(changes);
        Ok(())
    }
}

struct ImmediateEvacuation;

#[async_trait]
impl VirtualDisks for ImmediateEvacuation {
    async fn evacuate(
        &self,
        _disk: &DiskUuid,
        _cancel: &CancellationToken,
    ) -> Result<(), PortError> {
        Ok(())
    }
}

#[derive(Default)]
struct StableCancellation {
    started: Notify,
    settle: Notify,
}

#[async_trait]
impl VirtualDisks for StableCancellation {
    async fn evacuate(
        &self,
        _disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), PortError> {
        self.started.notify_one();
        cancel.cancelled().await;
        self.settle.notified().await;
        Err(PortError(
            "evacuation cancelled at a stable boundary".into(),
        ))
    }
}

fn disk(id: &str) -> MemberDisk {
    MemberDisk::new(id.into(), "pool-1", 16)
}

fn service(
    disks: Vec<MemberDisk>,
    ports: Arc<TestPorts>,
    virtual_disks: Arc<dyn VirtualDisks>,
    recovery_window: Duration,
) -> MemberDiskService {
    MemberDiskService::new(
        disks,
        ports.clone(),
        ports,
        virtual_disks,
        MemberDiskConfig {
            recovery_window,
            retry_delay: Duration::from_millis(1),
            max_retries: 2,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn batch_down_uses_one_initial_broadcast_and_completes_each_disk() {
    let ports = Arc::new(TestPorts::default());
    let instance = service(
        vec![disk("a"), disk("b")],
        ports.clone(),
        Arc::new(ImmediateEvacuation),
        Duration::ZERO,
    )
    .start(ServiceConfig::default());

    let reply = instance
        .client
        .call(MemberDiskCommand::ApplyEvents(vec![
            DiskEvent {
                disk: "a".into(),
                state: PhysicalState::Down,
                observed_at: now_millis(),
            },
            DiskEvent {
                disk: "b".into(),
                state: PhysicalState::Down,
                observed_at: now_millis(),
            },
        ]))
        .await
        .unwrap();

    let MemberDiskReply::Disks(outcomes) = reply else {
        panic!("disk events returned the wrong reply kind");
    };
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|outcome| {
        outcome
            .result
            .as_ref()
            .is_ok_and(|disk| disk.state() == MemberDiskState::Removed)
    }));

    {
        let broadcasts = ports.broadcasts.lock().unwrap();
        assert_eq!(broadcasts.first().map(Vec::len), Some(2));
    }

    instance.control.drain().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn allocation_uses_the_same_command_lifecycle_and_replies_on_completion() {
    let ports = Arc::new(TestPorts::default());
    let instance = service(
        vec![disk("a"), disk("b")],
        ports,
        Arc::new(ImmediateEvacuation),
        Duration::ZERO,
    )
    .start(ServiceConfig::default());

    let reply = instance
        .client
        .call(MemberDiskCommand::Allocate(AllocateBlocks { count: 3 }))
        .await
        .unwrap();
    let MemberDiskReply::Allocated(blocks) = reply else {
        panic!("allocation returned the wrong reply kind");
    };
    assert_eq!(blocks.len(), 3);

    let MemberDiskQueryReply::Disks(disks) =
        instance.client.query(MemberDiskQuery::List).await.unwrap()
    else {
        panic!("list query returned the wrong reply kind");
    };
    assert_eq!(
        disks
            .iter()
            .map(|disk| disk.allocated_blocks().count())
            .sum::<usize>(),
        3
    );

    instance.control.drain().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn allocation_retries_only_the_uncommitted_remainder() {
    let nodes = Arc::new(TestPorts::default());
    let metadata = Arc::new(FailSecondCommitOnce::default());
    let service = MemberDiskService::new(
        vec![
            MemberDisk::new("a".into(), "pool-1", 2),
            MemberDisk::new("b".into(), "pool-1", 2),
        ],
        metadata.clone(),
        nodes,
        Arc::new(ImmediateEvacuation),
        MemberDiskConfig {
            retry_delay: Duration::from_millis(1),
            ..MemberDiskConfig::default()
        },
    )
    .unwrap();
    let instance = service.start(ServiceConfig::default());

    let reply = instance
        .client
        .call(MemberDiskCommand::Allocate(AllocateBlocks { count: 3 }))
        .await
        .unwrap();
    let MemberDiskReply::Allocated(blocks) = reply else {
        panic!("allocation returned the wrong reply kind");
    };
    assert_eq!(blocks.len(), 3);

    let committed_disks: Vec<_> = metadata
        .commits
        .lock()
        .unwrap()
        .iter()
        .map(|batch| batch[0].disk.clone())
        .collect();
    assert_eq!(
        committed_disks,
        vec![
            DiskUuid::from("a"),
            DiskUuid::from("b"),
            DiskUuid::from("b")
        ]
    );

    instance.control.drain().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn every_request_remains_visible_while_one_disk_is_cancelling() {
    let ports = Arc::new(TestPorts::default());
    let evacuation = Arc::new(StableCancellation::default());
    let mut member = disk("a");
    member.apply_committed(MemberDiskChange::SetIo(IoState::Down));
    member.apply_committed(MemberDiskChange::DisableAllocation);

    let instance = service(vec![member], ports, evacuation.clone(), Duration::ZERO)
        .start(ServiceConfig::default());

    let client = instance.client.clone();
    let first = tokio::spawn(async move {
        client
            .call(MemberDiskCommand::ApplyEvents(vec![DiskEvent {
                disk: "a".into(),
                state: PhysicalState::Down,
                observed_at: now_millis(),
            }]))
            .await
    });
    evacuation.started.notified().await;

    for state in [PhysicalState::Up, PhysicalState::Down, PhysicalState::Up] {
        instance
            .client
            .submit(MemberDiskCommand::ApplyEvents(vec![DiskEvent {
                disk: "a".into(),
                state,
                observed_at: now_millis(),
            }]))
            .await
            .unwrap();
    }

    let MemberDiskQueryReply::Work(work) =
        instance.client.query(MemberDiskQuery::Work).await.unwrap()
    else {
        panic!("work query returned the wrong reply kind");
    };
    assert_eq!(work.len(), 4, "no command may be overwritten silently");
    assert!(
        work.iter()
            .any(|request| { matches!(request.status, RequestStatus::Cancelling { .. }) })
    );

    evacuation.settle.notify_one();
    let first = tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .expect("cancelled request did not settle")
        .unwrap()
        .unwrap();
    let MemberDiskReply::Disks(outcomes) = first else {
        panic!("disk event returned the wrong reply kind");
    };
    assert_eq!(outcomes[0].result, Err(MemberDiskError::Cancelled));

    tokio::time::timeout(Duration::from_secs(2), instance.control.stop())
        .await
        .expect("service did not stop")
        .unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn down_cancels_an_unrelated_inflight_shrink_command() {
    let ports = Arc::new(TestPorts::default());
    let evacuation = Arc::new(StableCancellation::default());
    let instance = service(
        vec![disk("a"), disk("b")],
        ports,
        evacuation.clone(),
        Duration::from_secs(60),
    )
    .start(ServiceConfig::default());

    let client = instance.client.clone();
    let shrink = tokio::spawn(async move {
        client
            .call(MemberDiskCommand::Shrink(vec!["a".into()]))
            .await
    });
    evacuation.started.notified().await;

    instance
        .client
        .submit(MemberDiskCommand::ApplyEvents(vec![DiskEvent {
            disk: "b".into(),
            state: PhysicalState::Down,
            observed_at: now_millis(),
        }]))
        .await
        .unwrap();

    let MemberDiskQueryReply::Commands(commands) = instance
        .client
        .query(MemberDiskQuery::InFlightCommands)
        .await
        .unwrap()
    else {
        panic!("command query returned the wrong reply kind");
    };
    assert!(commands.iter().any(|command| {
        command.kind == CommandKind::Shrink && command.status == CommandStatus::Cancelling
    }));

    evacuation.settle.notify_one();
    let reply = shrink.await.unwrap().unwrap();
    let MemberDiskReply::Disks(outcomes) = reply else {
        panic!("shrink returned the wrong reply kind");
    };
    assert_eq!(outcomes[0].result, Err(MemberDiskError::Cancelled));

    instance.control.stop().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn pause_rejects_commands_but_keeps_queries_available() {
    let ports = Arc::new(TestPorts::default());
    let instance = service(
        vec![disk("a")],
        ports,
        Arc::new(ImmediateEvacuation),
        Duration::ZERO,
    )
    .start(ServiceConfig::default());

    instance.control.pause().await.unwrap();
    assert_eq!(instance.observer.snapshot().lifecycle, Lifecycle::Paused);
    assert!(
        instance
            .client
            .submit(MemberDiskCommand::Shrink(vec!["a".into()]))
            .await
            .is_err()
    );
    assert!(matches!(
        instance.client.query(MemberDiskQuery::List).await,
        Ok(MemberDiskQueryReply::Disks(_))
    ));

    instance.control.resume().await.unwrap();
    instance.control.drain().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn aborting_the_root_task_drops_owned_work_and_closes_the_reply() {
    let ports = Arc::new(TestPorts::default());
    let evacuation = Arc::new(StableCancellation::default());
    let mut member = disk("a");
    member.apply_committed(MemberDiskChange::SetIo(IoState::Down));
    member.apply_committed(MemberDiskChange::DisableAllocation);

    let mut instance = service(vec![member], ports, evacuation.clone(), Duration::ZERO)
        .start(ServiceConfig::default());
    let client = instance.client.clone();
    let call = tokio::spawn(async move {
        client
            .call(MemberDiskCommand::ApplyEvents(vec![DiskEvent {
                disk: "a".into(),
                state: PhysicalState::Down,
                observed_at: now_millis(),
            }]))
            .await
    });

    evacuation.started.notified().await;
    assert_eq!(instance.observer.snapshot().activity, Activity::Busy);
    instance.task.abort();

    assert!(matches!(call.await.unwrap(), Err(CallError::Aborted)));
}

#[tokio::test]
async fn synchronous_completion_keeps_advancing_waiting_requests() {
    let ports = Arc::new(TestPorts::default());
    let mut service = service(
        vec![disk("a")],
        ports,
        Arc::new(ImmediateEvacuation),
        Duration::ZERO,
    );
    let up = || {
        MemberDiskCommand::ApplyEvents(vec![DiskEvent {
            disk: "a".into(),
            state: PhysicalState::Up,
            observed_at: now_millis(),
        }])
    };
    let (first_reply, first_result) = tokio::sync::oneshot::channel();
    let (second_reply, second_result) = tokio::sync::oneshot::channel();

    service.receive_command(IncomingCommand {
        id: CommandId(1),
        payload: up(),
        reply: CommandReply::Completed(Some(first_reply)),
    });
    service.receive_command(IncomingCommand {
        id: CommandId(2),
        payload: up(),
        reply: CommandReply::Completed(Some(second_reply)),
    });
    service.admit();

    let mut runs = NewRuns::default();
    service.start_ready(&mut runs);

    assert!(runs.is_empty());
    assert!(service.is_idle());
    assert!(first_result.await.unwrap().is_ok());
    assert!(second_result.await.unwrap().is_ok());
}
