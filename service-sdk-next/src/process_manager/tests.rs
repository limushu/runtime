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

use crate::runtime::{Activity, CallError, Lifecycle, ServiceConfig};

use super::*;
use crate::pool::{Pool, PoolConfig};

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
    fn is_retryable(&self, _error: &PortError) -> bool {
        true
    }
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
) -> Pool {
    Pool::new(
        "pool-1",
        disks,
        ports.clone(),
        ports,
        virtual_disks,
        PoolConfig {
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
        .call(ProcessCommand::ApplyEvents(vec![
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

    let ProcessReply::Disks(outcomes) = reply else {
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
        .call(ProcessCommand::Allocate(AllocateBlocks { count: 3 }))
        .await
        .unwrap();
    let ProcessReply::Allocated(blocks) = reply else {
        panic!("allocation returned the wrong reply kind");
    };
    assert_eq!(blocks.len(), 3);

    let ProcessQueryReply::Disks(disks) = instance.client.query(ProcessQuery::List).await.unwrap()
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
    let service = Pool::new(
        "pool-1",
        vec![
            MemberDisk::new("a".into(), "pool-1", 2),
            MemberDisk::new("b".into(), "pool-1", 2),
        ],
        metadata.clone(),
        nodes,
        Arc::new(ImmediateEvacuation),
        PoolConfig {
            retry_delay: Duration::from_millis(1),
            ..PoolConfig::default()
        },
    )
    .unwrap();
    let instance = service.start(ServiceConfig::default());

    let reply = instance
        .client
        .call(ProcessCommand::Allocate(AllocateBlocks { count: 3 }))
        .await
        .unwrap();
    let ProcessReply::Allocated(blocks) = reply else {
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
            .call(ProcessCommand::ApplyEvents(vec![DiskEvent {
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
            .submit(ProcessCommand::ApplyEvents(vec![DiskEvent {
                disk: "a".into(),
                state,
                observed_at: now_millis(),
            }]))
            .await
            .unwrap();
    }

    let ProcessQueryReply::Work(work) = instance.client.query(ProcessQuery::Work).await.unwrap()
    else {
        panic!("work query returned the wrong reply kind");
    };
    assert_eq!(work.len(), 4, "no command may be overwritten silently");
    assert!(
        work.iter()
            .any(|workflow| { matches!(workflow.status, WorkflowStatus::Cancelling) })
    );

    evacuation.settle.notify_one();
    let first = tokio::time::timeout(Duration::from_secs(2), first)
        .await
        .expect("cancelled request did not settle")
        .unwrap()
        .unwrap();
    let ProcessReply::Disks(outcomes) = first else {
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
    let shrink =
        tokio::spawn(async move { client.call(ProcessCommand::Shrink(vec!["a".into()])).await });
    evacuation.started.notified().await;

    instance
        .client
        .submit(ProcessCommand::ApplyEvents(vec![DiskEvent {
            disk: "b".into(),
            state: PhysicalState::Down,
            observed_at: now_millis(),
        }]))
        .await
        .unwrap();

    let ProcessQueryReply::Commands(commands) = instance
        .client
        .query(ProcessQuery::InFlightCommands)
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
    let ProcessReply::Disks(outcomes) = reply else {
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
            .submit(ProcessCommand::Shrink(vec!["a".into()]))
            .await
            .is_err()
    );
    assert!(matches!(
        instance.client.query(ProcessQuery::List).await,
        Ok(ProcessQueryReply::Disks(_))
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
            .call(ProcessCommand::ApplyEvents(vec![DiskEvent {
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
async fn immediate_workflows_keep_advancing_without_another_command() {
    let ports = Arc::new(TestPorts::default());
    let instance = service(
        vec![disk("a")],
        ports,
        Arc::new(ImmediateEvacuation),
        Duration::ZERO,
    )
    .start(ServiceConfig::default());
    let up = || {
        ProcessCommand::ApplyEvents(vec![DiskEvent {
            disk: "a".into(),
            state: PhysicalState::Up,
            observed_at: now_millis(),
        }])
    };
    let (first, second) = tokio::time::timeout(Duration::from_secs(2), async {
        tokio::join!(instance.client.call(up()), instance.client.call(up()))
    })
    .await
    .unwrap();
    assert!(first.is_ok());
    assert!(second.is_ok());
    instance.control.drain().await.unwrap();
    instance.join().await.unwrap();
}

fn event(id: &str, state: PhysicalState) -> ProcessCommand {
    ProcessCommand::ApplyEvents(vec![DiskEvent {
        disk: id.into(),
        state,
        observed_at: now_millis(),
    }])
}

fn outcomes(reply: ProcessReply) -> Vec<DiskOutcome> {
    match reply {
        ProcessReply::Disks(disks) => disks,
        _ => panic!("expected disk outcomes"),
    }
}

#[derive(Default)]
struct ControlledMetadata {
    started: Notify,
    finish: Notify,
    calls: AtomicUsize,
    mode: usize,
}

#[async_trait]
impl MemberDiskMetadata for ControlledMetadata {
    fn is_retryable(&self, _: &PortError) -> bool {
        self.mode == 3
    }
    async fn commit(&self, changes: Vec<MemberDiskCommit>) -> Vec<Result<(), PortError>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.mode == 0 && call == 0 {
            self.started.notify_one();
            self.finish.notified().await;
        }
        if self.mode == 1 {
            return Vec::new();
        }
        if self.mode == 2 {
            panic!("simulated uncertain provider panic");
        }
        changes
            .into_iter()
            .map(|_| {
                if (self.mode == 3 && call > 0) || self.mode == 4 {
                    Err(PortError("write failed".into()))
                } else {
                    Ok(())
                }
            })
            .collect()
    }
}

fn with_metadata(
    metadata: Arc<dyn MemberDiskMetadata>,
) -> crate::runtime::ServiceInstance<ProcessManager> {
    Pool::new(
        "pool-1",
        vec![
            MemberDisk::new("a".into(), "pool-1", 2),
            MemberDisk::new("b".into(), "pool-1", 2),
        ],
        metadata,
        Arc::new(TestPorts::default()),
        Arc::new(ImmediateEvacuation),
        PoolConfig {
            recovery_window: Duration::ZERO,
            retry_delay: Duration::from_millis(1),
            max_retries: 1,
        },
    )
    .unwrap()
    .start(ServiceConfig::default())
}

#[tokio::test]
async fn pause_stops_next_activity_not_the_commit_in_progress() {
    let metadata = Arc::new(ControlledMetadata::default());
    let instance = with_metadata(metadata.clone());
    instance
        .client
        .submit(event("a", PhysicalState::Down))
        .await
        .unwrap();
    metadata.started.notified().await;
    instance.control.pause().await.unwrap();
    let ProcessQueryReply::Disk(Some(before)) = instance
        .client
        .query(ProcessQuery::Get("a".into()))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        before.state(),
        MemberDiskState::UpActive,
        "unconfirmed commit must not be published"
    );
    metadata.finish.notify_one();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let ProcessQueryReply::Disk(Some(member)) = instance
                .client
                .query(ProcessQuery::Get("a".into()))
                .await
                .unwrap()
            else {
                panic!()
            };
            if member.state() == MemberDiskState::DownActive {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        metadata.calls.load(Ordering::SeqCst),
        1,
        "no next activity while paused"
    );
    instance.control.drain().await.unwrap();
    assert!(
        instance
            .observer
            .snapshot()
            .service
            .workflows
            .iter()
            .any(|w| w.status == WorkflowStatus::Completed)
    );
    instance.join().await.unwrap();
}

#[tokio::test]
async fn malformed_commit_blocks_disk_but_other_disks_can_complete() {
    let metadata = Arc::new(ControlledMetadata {
        mode: 1,
        ..Default::default()
    });
    let instance = with_metadata(metadata.clone());
    let failed = outcomes(
        instance
            .client
            .call(event("a", PhysicalState::Down))
            .await
            .unwrap(),
    );
    assert!(matches!(
        failed[0].result,
        Err(MemberDiskError::NeedsRecovery(_))
    ));
    let blocked = outcomes(
        instance
            .client
            .call(event("a", PhysicalState::Up))
            .await
            .unwrap(),
    );
    assert!(matches!(
        blocked[0].result,
        Err(MemberDiskError::NeedsRecovery(_))
    ));
    assert_eq!(
        metadata.calls.load(Ordering::SeqCst),
        1,
        "uncertain resource must not be retried"
    );
    assert!(matches!(
        instance
            .client
            .call(ProcessCommand::Allocate(AllocateBlocks { count: 1 }))
            .await,
        Err(CallError::Business(MemberDiskError::NeedsRecovery(_)))
    ));
    let other = outcomes(
        instance
            .client
            .call(event("b", PhysicalState::Up))
            .await
            .unwrap(),
    );
    assert!(other[0].result.is_ok());
    instance.control.stop().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn provider_panic_isolated_and_retains_resource_block() {
    let metadata = Arc::new(ControlledMetadata {
        mode: 2,
        ..Default::default()
    });
    let instance = with_metadata(metadata.clone());
    let failed = outcomes(
        instance
            .client
            .call(event("a", PhysicalState::Down))
            .await
            .unwrap(),
    );
    assert!(matches!(
        failed[0].result,
        Err(MemberDiskError::NeedsRecovery(_))
    ));
    let blocked = outcomes(
        instance
            .client
            .call(event("a", PhysicalState::Up))
            .await
            .unwrap(),
    );
    assert!(matches!(
        blocked[0].result,
        Err(MemberDiskError::NeedsRecovery(_))
    ));
    assert_eq!(metadata.calls.load(Ordering::SeqCst), 1);
    assert!(instance.client.query(ProcessQuery::List).await.is_ok());
    instance.control.stop().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn partial_allocation_failure_returns_committed_blocks() {
    let metadata = Arc::new(ControlledMetadata {
        mode: 3,
        ..Default::default()
    });
    let instance = with_metadata(metadata.clone());
    let error = instance
        .client
        .call(ProcessCommand::Allocate(AllocateBlocks { count: 3 }))
        .await
        .unwrap_err();
    let CallError::Business(MemberDiskError::PartialSpace {
        allocated,
        committed,
        cause,
    }) = error
    else {
        panic!("partial progress lost")
    };
    assert!(allocated);
    assert_eq!(committed.len(), 2);
    assert!(committed.iter().all(|b| b.disk == DiskUuid::from("a")));
    assert!(matches!(*cause, MemberDiskError::Metadata(_)));
    assert_eq!(metadata.calls.load(Ordering::SeqCst), 3);
    instance.control.drain().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn unclassified_metadata_error_is_not_retried() {
    let metadata = Arc::new(ControlledMetadata {
        mode: 4,
        ..Default::default()
    });
    let instance = with_metadata(metadata.clone());
    let failed = outcomes(
        instance
            .client
            .call(ProcessCommand::Shrink(vec!["a".into()]))
            .await
            .unwrap(),
    );
    assert!(matches!(
        failed[0].result,
        Err(MemberDiskError::NeedsRecovery(_))
    ));
    assert_eq!(metadata.calls.load(Ordering::SeqCst), 1);
    instance.control.stop().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn batch_preserves_input_order_duplicate_commands_and_unknown_disks() {
    let instance = with_metadata(Arc::new(TestPorts::default()));
    let command = ProcessCommand::ApplyEvents(
        ["b", "missing", "a", "b"]
            .into_iter()
            .map(|id| DiskEvent {
                disk: id.into(),
                state: PhysicalState::Up,
                observed_at: now_millis(),
            })
            .collect(),
    );
    let result = outcomes(instance.client.call(command).await.unwrap());
    assert_eq!(
        result
            .iter()
            .map(|o| o.disk.to_string())
            .collect::<Vec<_>>(),
        vec!["b", "missing", "a", "b"]
    );
    assert!(matches!(
        result[1].result,
        Err(MemberDiskError::UnknownDisk(_))
    ));
    assert!(result[0].result.is_ok() && result[2].result.is_ok() && result[3].result.is_ok());
    let ProcessQueryReply::Work(work) = instance.client.query(ProcessQuery::Work).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(work.len(), 4);
    assert!(work.iter().all(|w| w.result.is_some()));
    instance.control.drain().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn no_space_preflight_counts_only_eligible_members() {
    let metadata = Arc::new(TestPorts::default());
    let mut inactive = MemberDisk::new("b".into(), "pool-1", 100);
    inactive.apply_committed(MemberDiskChange::DisableAllocation);
    let instance = service(
        vec![MemberDisk::new("a".into(), "pool-1", 1), inactive],
        metadata.clone(),
        Arc::new(ImmediateEvacuation),
        Duration::ZERO,
    )
    .start(ServiceConfig::default());
    assert!(matches!(
        instance
            .client
            .call(ProcessCommand::Allocate(AllocateBlocks { count: 2 }))
            .await,
        Err(CallError::Business(MemberDiskError::NoSpace))
    ));
    assert!(metadata.commits.lock().unwrap().is_empty());
    instance.control.drain().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn stop_waits_for_final_commit_and_preserves_success() {
    let metadata = Arc::new(ControlledMetadata::default());
    let instance = with_metadata(metadata.clone());
    let client = instance.client.clone();
    let allocate = tokio::spawn(async move {
        client
            .call(ProcessCommand::Allocate(AllocateBlocks { count: 1 }))
            .await
    });
    metadata.started.notified().await;
    let control = instance.control.clone();
    let stop = tokio::spawn(async move { control.stop().await });
    let mut observer = instance.observer.clone();
    tokio::time::timeout(Duration::from_secs(2), async {
        while observer.snapshot().lifecycle != Lifecycle::Stopping {
            observer.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    assert!(
        !stop.is_finished(),
        "stop must wait for the in-flight write"
    );
    metadata.finish.notify_one();
    let reply = tokio::time::timeout(Duration::from_secs(2), allocate)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(reply, ProcessReply::Allocated(blocks) if blocks.len() == 1));
    stop.await.unwrap().unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn unknown_down_cannot_cancel_an_active_allocation() {
    let metadata = Arc::new(ControlledMetadata::default());
    let instance = with_metadata(metadata.clone());
    let client = instance.client.clone();
    let allocate = tokio::spawn(async move {
        client
            .call(ProcessCommand::Allocate(AllocateBlocks { count: 3 }))
            .await
    });
    metadata.started.notified().await;
    let unknown = outcomes(
        instance
            .client
            .call(event("missing", PhysicalState::Down))
            .await
            .unwrap(),
    );
    assert!(matches!(
        unknown[0].result,
        Err(MemberDiskError::UnknownDisk(_))
    ));
    metadata.finish.notify_one();
    let reply = tokio::time::timeout(Duration::from_secs(2), allocate)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(reply, ProcessReply::Allocated(blocks) if blocks.len() == 3));
    instance.control.drain().await.unwrap();
    instance.join().await.unwrap();
}

#[tokio::test]
async fn dropped_channels_do_not_starve_owned_work() {
    let metadata = Arc::new(ControlledMetadata::default());
    let instance = with_metadata(metadata.clone());
    instance
        .client
        .submit(ProcessCommand::Allocate(AllocateBlocks { count: 1 }))
        .await
        .unwrap();
    metadata.started.notified().await;
    let crate::runtime::ServiceInstance {
        client,
        control,
        observer,
        mut task,
    } = instance;
    drop(client);
    drop(control);
    metadata.finish.notify_one();
    tokio::time::timeout(Duration::from_secs(2), task.join())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(observer.snapshot().lifecycle, Lifecycle::Stopped);
    assert_eq!(
        observer.snapshot().service.workflows[0].status,
        WorkflowStatus::Completed
    );
}

#[tokio::test]
async fn release_reports_partial_progress_and_rejects_invalid_plan_before_writes() {
    let metadata = Arc::new(ControlledMetadata {
        mode: 3,
        ..Default::default()
    });
    let mut a = MemberDisk::new("a".into(), "pool-1", 2);
    let mut b = MemberDisk::new("b".into(), "pool-1", 2);
    a.apply_committed(MemberDiskChange::Allocate(vec![BlockId(0)]));
    b.apply_committed(MemberDiskChange::Allocate(vec![BlockId(0)]));
    let instance = Pool::new(
        "pool-1",
        vec![a, b],
        metadata.clone(),
        Arc::new(TestPorts::default()),
        Arc::new(ImmediateEvacuation),
        PoolConfig {
            retry_delay: Duration::from_millis(1),
            max_retries: 0,
            ..Default::default()
        },
    )
    .unwrap()
    .start(ServiceConfig::default());
    let block = |id: &str| AllocatedBlock {
        disk: id.into(),
        block: BlockId(0),
    };
    let invalid = instance
        .client
        .call(ProcessCommand::Release(ReleaseBlocks {
            blocks: vec![block("a"), block("missing")],
        }))
        .await;
    assert!(matches!(
        invalid,
        Err(CallError::Business(MemberDiskError::UnknownDisk(_)))
    ));
    assert_eq!(metadata.calls.load(Ordering::SeqCst), 0);
    let result = instance
        .client
        .call(ProcessCommand::Release(ReleaseBlocks {
            blocks: vec![block("a"), block("b")],
        }))
        .await;
    assert!(
        matches!(result, Err(CallError::Business(MemberDiskError::PartialSpace { allocated: false, committed, .. })) if committed == vec![block("a")])
    );
    instance.control.drain().await.unwrap();
    instance.join().await.unwrap();
}

struct PerDiskEvacuation(Arc<StableCancellation>);

#[async_trait]
impl VirtualDisks for PerDiskEvacuation {
    async fn evacuate(&self, disk: &DiskUuid, cancel: &CancellationToken) -> Result<(), PortError> {
        if *disk == DiskUuid::from("a") {
            self.0.evacuate(disk, cancel).await
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn another_disk_progresses_while_conflicting_work_waits_for_stable_cancel() {
    let gate = Arc::new(StableCancellation::default());
    let instance = service(
        vec![disk("a"), disk("b")],
        Arc::new(TestPorts::default()),
        Arc::new(PerDiskEvacuation(gate.clone())),
        Duration::ZERO,
    )
    .start(ServiceConfig::default());
    let first = instance
        .client
        .submit(event("a", PhysicalState::Down))
        .await
        .unwrap();
    gate.started.notified().await;
    instance
        .client
        .submit(event("a", PhysicalState::Up))
        .await
        .unwrap();
    let other = tokio::time::timeout(
        Duration::from_secs(2),
        instance.client.call(event("b", PhysicalState::Down)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        outcomes(other)[0].result.as_ref().unwrap().state(),
        MemberDiskState::Removed
    );
    let ProcessQueryReply::Work(work) = instance.client.query(ProcessQuery::Work).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(
        work.iter().find(|w| w.command_id == first).unwrap().status,
        WorkflowStatus::Cancelling
    );
    assert!(
        work.iter()
            .any(|w| w.disk == Some("a".into()) && w.status == WorkflowStatus::Waiting)
    );
    gate.settle.notify_one();
    tokio::time::timeout(Duration::from_secs(2), instance.control.drain())
        .await
        .unwrap()
        .unwrap();
    let work = instance.observer.snapshot().service.workflows;
    assert_eq!(
        work.iter().find(|w| w.command_id == first).unwrap().result,
        Some(Err(MemberDiskError::Cancelled))
    );
    assert!(
        work.iter()
            .any(|w| w.disk == Some("a".into()) && w.status == WorkflowStatus::Completed)
    );
    instance.join().await.unwrap();
}

#[tokio::test]
async fn terminal_history_is_bounded_without_losing_large_batch_results() {
    let instance = with_metadata(Arc::new(TestPorts::default()));
    let events = (0..270)
        .map(|_| DiskEvent {
            disk: "a".into(),
            state: PhysicalState::Up,
            observed_at: now_millis(),
        })
        .collect();
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        instance.client.call(ProcessCommand::ApplyEvents(events)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(outcomes(result).len(), 270);
    let ProcessQueryReply::Work(work) = instance.client.query(ProcessQuery::Work).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(work.len(), 256);
    assert!(work.iter().all(|w| w.status == WorkflowStatus::Completed));
    instance.control.drain().await.unwrap();
    instance.join().await.unwrap();
}
