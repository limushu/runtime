use super::*;
use crate::workflows::*;
use async_trait::async_trait;
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct Ports {
    commits: Mutex<Vec<MemberDiskCommit>>,
    calls: AtomicUsize,
    gate_first: bool,
    started: Notify,
    finish: Notify,
    fail_call: Option<usize>,
    malformed: bool,
    retryable: bool,
}
#[async_trait]
impl MemberDiskMetadata for Ports {
    fn is_retryable(&self, _: &PortError) -> bool {
        self.retryable
    }
    async fn commit(&self, changes: Vec<MemberDiskCommit>) -> Vec<Result<(), PortError>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if self.gate_first && call == 0 {
            self.started.notify_one();
            self.finish.notified().await;
        }
        if self.malformed {
            return vec![];
        }
        if self.fail_call == Some(call) {
            return changes
                .into_iter()
                .map(|_| Err(PortError("commit failure".into())))
                .collect();
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
        vec![
            MemberDisk::new("a".into(), id, 8),
            MemberDisk::new("b".into(), id, 8),
        ],
        ports.clone(),
        ports.clone(),
        ports,
        PoolConfig {
            recovery_window: Duration::ZERO,
            retry_delay: Duration::from_millis(1),
            max_retries: 0,
        },
    )
    .unwrap()
}
fn allocation(
    handle: &PoolHandle,
    count: usize,
) -> (
    WorkflowTask,
    oneshot::Receiver<Result<Vec<AllocatedBlock>, MemberDiskError>>,
) {
    let (tx, rx) = oneshot::channel();
    let disks = handle.disks.clone();
    let task = WorkflowTask::new(
        "space.allocate",
        vec![disk_collection()],
        move |mut ctx| async move {
            let result = disks.allocate(&mut ctx, count).await;
            let outcome = result.clone().map(|_| ()).map_err(WorkflowError::from);
            let _ = tx.send(result);
            outcome
        },
    );
    (task, rx)
}

#[test]
fn member_scope_and_identity_are_validated() {
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
    assert!(make(" ", vec![]).is_err());
    assert!(make("a", vec![MemberDisk::new("a".into(), "b", 1)]).is_err());
    assert!(make("a", vec![MemberDisk::new("a".into(), "a", 1); 2]).is_err());
}

#[tokio::test]
async fn pools_are_isolated_and_own_three_independent_runtimes() {
    let mut manager = PoolManager::default();
    let a = manager
        .insert(
            pool("one", Arc::new(Ports::default())),
            ServiceConfig::default(),
        )
        .unwrap();
    let b = manager
        .insert(
            pool("two", Arc::new(Ports::default())),
            ServiceConfig::default(),
        )
        .unwrap();
    assert!(
        manager
            .insert(
                pool("one", Arc::new(Ports::default())),
                ServiceConfig::default()
            )
            .is_err()
    );
    a.client
        .call(ProcessCommand::Start(offline(&a, "a".into())))
        .await
        .unwrap();
    assert_eq!(
        a.disks.get("a".into()).await.unwrap().state(),
        MemberDiskState::Removed
    );
    assert_eq!(
        b.disks.get("a".into()).await.unwrap().state(),
        MemberDiskState::UpActive
    );
    assert!(a.disk_observer.snapshot().service.completed >= 4);
    assert_eq!(a.vd_observer.snapshot().service.completed, 1);
    manager.remove("one").await.unwrap();
    assert_eq!(a.disk_observer.snapshot().lifecycle, Lifecycle::Stopped);
    assert_eq!(a.vd_observer.snapshot().lifecycle, Lifecycle::Stopped);
    let (task, output) = allocation(&b, 2);
    b.client.call(ProcessCommand::Start(task)).await.unwrap();
    assert_eq!(output.await.unwrap().unwrap().len(), 2);
    assert!(a.disks.list().await.is_err());
    manager.remove("two").await.unwrap();
    assert!(manager.ids().is_empty());
}

#[tokio::test]
async fn collection_lock_prevents_duplicate_allocation_and_precommit_publication() {
    let ports = Arc::new(Ports {
        gate_first: true,
        ..Default::default()
    });
    let mut instance = pool("p", ports.clone()).start(ServiceConfig::default());
    let h = &instance.handle;
    let (first, out1) = allocation(h, 2);
    h.client.submit(ProcessCommand::Start(first)).await.unwrap();
    ports.started.notified().await;
    let (second, out2) = allocation(h, 2);
    h.client
        .submit(ProcessCommand::Start(second))
        .await
        .unwrap();
    assert_eq!(h.disks.get("a".into()).await.unwrap().free_blocks(), 8);
    assert_eq!(ports.calls.load(Ordering::SeqCst), 1);
    ports.finish.notify_one();
    let a = out1.await.unwrap().unwrap();
    let b = out2.await.unwrap().unwrap();
    assert!(!a.iter().any(|block| b.contains(block)));
    assert_eq!(h.disks.get("a".into()).await.unwrap().free_blocks(), 4);
    instance.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancelled_pool_removal_retains_tasks_until_final_commit() {
    let ports = Arc::new(Ports {
        gate_first: true,
        ..Default::default()
    });
    let mut manager = PoolManager::default();
    let h = manager
        .insert(pool("p", ports.clone()), ServiceConfig::default())
        .unwrap();
    let (task, output) = allocation(&h, 1);
    h.client.submit(ProcessCommand::Start(task)).await.unwrap();
    ports.started.notified().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), manager.remove("p"))
            .await
            .is_err()
    );
    assert!(manager.get("p").is_ok());
    assert_eq!(h.disk_observer.snapshot().lifecycle, Lifecycle::Running);
    ports.finish.notify_one();
    tokio::time::timeout(Duration::from_secs(2), manager.remove("p"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output.await.unwrap().unwrap().len(), 1);
    assert_eq!(
        h.observer.snapshot().service.workflows[0].status,
        WorkflowStatus::Completed
    );
}

#[tokio::test]
async fn malformed_commit_fences_collection_but_not_an_unrelated_namespace() {
    let ports = Arc::new(Ports {
        malformed: true,
        ..Default::default()
    });
    let mut instance = pool("p", ports.clone()).start(ServiceConfig::default());
    let h = &instance.handle;
    let (task, _) = allocation(h, 1);
    assert!(matches!(
        h.client.call(ProcessCommand::Start(task)).await,
        Err(CallError::Business(WorkflowError::NeedsRecovery(_)))
    ));
    assert!(
        h.client
            .call(ProcessCommand::Start(online(h, "b".into())))
            .await
            .is_err()
    );
    assert_eq!(ports.calls.load(Ordering::SeqCst), 1);
    h.client
        .call(ProcessCommand::Start(WorkflowTask::new(
            "node-check",
            vec![ObjectClaim::exclusive(ObjectKey::new("node", "n"))],
            |_| async { Ok(()) },
        )))
        .await
        .unwrap();
    instance.shutdown().await.unwrap();
}

#[tokio::test]
async fn partial_space_progress_stays_typed_and_unknown_errors_are_not_retried() {
    let ports = Arc::new(Ports {
        fail_call: Some(1),
        ..Default::default()
    });
    let mut instance = pool("p", ports.clone()).start(ServiceConfig::default());
    let (task, output) = allocation(&instance.handle, 10);
    assert!(
        instance
            .handle
            .client
            .call(ProcessCommand::Start(task))
            .await
            .is_err()
    );
    let result = output.await.unwrap();
    assert!(
        matches!(result, Err(MemberDiskError::PartialSpace { ref committed, .. }) if committed.len() == 8)
    );
    assert_eq!(ports.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        instance
            .handle
            .disks
            .get("a".into())
            .await
            .unwrap()
            .free_blocks(),
        0
    );
    assert_eq!(
        instance
            .handle
            .disks
            .get("b".into())
            .await
            .unwrap()
            .free_blocks(),
        8
    );
    instance.shutdown().await.unwrap();
}

#[tokio::test]
async fn retryable_failure_retries_only_uncommitted_changes() {
    let ports = Arc::new(Ports {
        fail_call: Some(1),
        retryable: true,
        ..Default::default()
    });
    // Build with explicit retry configuration; don't change a domain at runtime.
    let p = Pool::new(
        "p",
        vec![
            MemberDisk::new("a".into(), "p", 8),
            MemberDisk::new("b".into(), "p", 8),
        ],
        ports.clone(),
        ports.clone(),
        ports.clone(),
        PoolConfig {
            max_retries: 2,
            retry_delay: Duration::ZERO,
            ..Default::default()
        },
    )
    .unwrap();
    let mut instance = p.start(ServiceConfig::default());
    let (task, output) = allocation(&instance.handle, 10);
    instance
        .handle
        .client
        .call(ProcessCommand::Start(task))
        .await
        .unwrap();
    assert_eq!(output.await.unwrap().unwrap().len(), 10);
    assert_eq!(ports.calls.load(Ordering::SeqCst), 3);
    assert_eq!(ports.commits.lock().unwrap().len(), 2);
    instance.shutdown().await.unwrap();
}

#[tokio::test]
async fn release_validation_happens_before_any_commit() {
    let ports = Arc::new(Ports::default());
    let mut instance = pool("p", ports.clone()).start(ServiceConfig::default());
    let h = &instance.handle;
    let (task, output) = allocation(h, 1);
    h.client.call(ProcessCommand::Start(task)).await.unwrap();
    let blocks = output.await.unwrap().unwrap();
    let disks = h.disks.clone();
    let block = blocks[0].clone();
    let invalid = WorkflowTask::new(
        "invalid-release",
        vec![disk_collection()],
        move |mut ctx| async move {
            disks.release(&mut ctx, vec![block.clone(), block]).await?;
            Ok(())
        },
    );
    assert!(h.client.call(ProcessCommand::Start(invalid)).await.is_err());
    assert_eq!(ports.calls.load(Ordering::SeqCst), 1);
    let disks = h.disks.clone();
    h.client
        .call(ProcessCommand::Start(WorkflowTask::new(
            "release",
            vec![disk_collection()],
            move |mut ctx| async move {
                disks.release(&mut ctx, blocks).await?;
                Ok(())
            },
        )))
        .await
        .unwrap();
    assert_eq!(h.disks.get("a".into()).await.unwrap().free_blocks(), 8);
    instance.shutdown().await.unwrap();
}

#[tokio::test]
async fn old_proof_cannot_remove_a_newly_online_member() {
    let mut instance = pool("p", Arc::new(Ports::default())).start(ServiceConfig::default());
    let h = &instance.handle;
    let disks = h.disks.clone();
    let vd = h.virtual_disks.clone();
    let task = WorkflowTask::new(
        "stale-proof",
        vec![disk_object(&"a".into())],
        move |mut ctx| async move {
            disks.disable_allocation(&mut ctx, "a".into()).await?;
            let ticket = disks.prepare_drain(&mut ctx, "a".into()).await?;
            let proof = vd.evacuate(&mut ctx, ticket).await?;
            disks.bring_online(&mut ctx, "a".into()).await?;
            let result = disks.remove(&mut ctx, proof).await;
            assert!(matches!(
                result,
                Err(MemberDiskError::InvalidState {
                    reason: "stale drain proof",
                    ..
                })
            ));
            Ok(())
        },
    );
    h.client.call(ProcessCommand::Start(task)).await.unwrap();
    assert_eq!(
        h.disks.get("a".into()).await.unwrap().state(),
        MemberDiskState::UpActive
    );
    instance.shutdown().await.unwrap();
}

#[tokio::test]
async fn wrong_claim_and_foreign_pool_client_never_reach_disk_provider() {
    let ports = Arc::new(Ports::default());
    let mut a = pool("a", ports.clone()).start(ServiceConfig::default());
    let mut b = pool("b", Arc::new(Ports::default())).start(ServiceConfig::default());
    let disks = a.handle.disks.clone();
    let task = WorkflowTask::new(
        "wrong-pool",
        vec![disk_collection()],
        move |mut ctx| async move {
            disks.allocate(&mut ctx, 1).await?;
            Ok(())
        },
    );
    assert!(
        b.handle
            .client
            .call(ProcessCommand::Start(task))
            .await
            .is_err()
    );
    let disks = a.handle.disks.clone();
    let task = WorkflowTask::new(
        "wrong-object",
        vec![disk_object(&"a".into())],
        move |mut ctx| async move {
            disks.allocate(&mut ctx, 1).await?;
            Ok(())
        },
    );
    assert!(
        a.handle
            .client
            .call(ProcessCommand::Start(task))
            .await
            .is_err()
    );
    assert_eq!(ports.calls.load(Ordering::SeqCst), 0);
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[derive(Default)]
struct StableVd {
    entered: Notify,
    cancelled: Notify,
    settle: Notify,
}
#[async_trait]
impl VirtualDisks for StableVd {
    async fn evacuate(&self, _: &DiskUuid, cancel: &CancellationToken) -> Result<(), PortError> {
        self.entered.notify_one();
        cancel.cancelled().await;
        self.cancelled.notify_one();
        self.settle.notified().await;
        Ok(())
    }
}
#[tokio::test]
async fn explicit_cancel_waits_for_vd_runtime_before_handing_disk_to_online() {
    let ports = Arc::new(Ports::default());
    let vd = Arc::new(StableVd::default());
    let mut instance = Pool::new(
        "p",
        vec![MemberDisk::new("a".into(), "p", 8)],
        ports.clone(),
        ports,
        vd.clone(),
        PoolConfig::default(),
    )
    .unwrap()
    .start(ServiceConfig::default());
    let h = &instance.handle;
    let first = h
        .client
        .submit(ProcessCommand::Start(shrink(h, "a".into())))
        .await
        .unwrap();
    vd.entered.notified().await;
    h.client
        .submit(ProcessCommand::Start(online(h, "a".into())))
        .await
        .unwrap();
    h.client.call(ProcessCommand::Cancel(first)).await.unwrap();
    vd.cancelled.notified().await;
    assert_eq!(
        h.disks.get("a".into()).await.unwrap().state(),
        MemberDiskState::UpInactive
    );
    let snapshot = h.client.query(()).await.unwrap();
    assert_eq!(snapshot.workflows[0].status, WorkflowStatus::Cancelling);
    assert_eq!(snapshot.workflows[1].status, WorkflowStatus::Waiting);
    vd.settle.notify_one();
    h.control.drain().await.unwrap();
    assert_eq!(
        h.disks.get("a".into()).await.unwrap().state(),
        MemberDiskState::UpActive
    );
    assert_eq!(
        h.observer.snapshot().service.workflows[0].status,
        WorkflowStatus::Cancelled
    );
    assert_eq!(
        h.observer.snapshot().service.workflows[1].status,
        WorkflowStatus::Completed
    );
    instance.shutdown().await.unwrap();
}

#[tokio::test]
async fn same_named_object_in_another_pool_progresses_while_first_is_blocked() {
    let ports = Arc::new(Ports {
        gate_first: true,
        ..Default::default()
    });
    let mut a = pool("a", ports.clone()).start(ServiceConfig::default());
    let mut b = pool("b", Arc::new(Ports::default())).start(ServiceConfig::default());
    a.handle
        .client
        .submit(ProcessCommand::Start(shrink(&a.handle, "a".into())))
        .await
        .unwrap();
    ports.started.notified().await;
    tokio::time::timeout(
        Duration::from_secs(2),
        b.handle
            .client
            .call(ProcessCommand::Start(shrink(&b.handle, "a".into()))),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        b.handle.disks.get("a".into()).await.unwrap().state(),
        MemberDiskState::Removed
    );
    assert_eq!(
        a.handle.disks.get("a".into()).await.unwrap().state(),
        MemberDiskState::UpActive
    );
    ports.finish.notify_one();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
}

#[tokio::test]
async fn duplicate_lifecycle_workflows_are_idempotent_and_shutdown_handles_paused_workers() {
    let ports = Arc::new(Ports::default());
    let mut instance = pool("p", ports.clone()).start(ServiceConfig::default());
    for _ in 0..2 {
        instance
            .handle
            .client
            .call(ProcessCommand::Start(offline(&instance.handle, "a".into())))
            .await
            .unwrap();
    }
    assert_eq!(ports.calls.load(Ordering::SeqCst), 3);
    instance.handle.disk_control.pause().await.unwrap();
    instance.handle.vd_control.pause().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), instance.shutdown())
        .await
        .unwrap()
        .unwrap();
}
