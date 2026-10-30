use std::{sync::Arc, time::Duration};
use storage_pool::{adapters::*, *};
use workflow_foundation::{RunState, persistence::*};

struct GatedMetadata {
    inner: MemoryMetadata,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[async_trait::async_trait]
impl storage_pool::ports::MetadataStore for GatedMetadata {
    async fn commit(
        &self,
        pool: &PoolId,
        change: storage_pool::ports::MetadataChange,
    ) -> Outcome<()> {
        self.inner.commit(pool, change).await
    }
    async fn disks(&self, pool: &PoolId) -> Outcome<Option<storage_pool::ports::DiskMetadata>> {
        if pool.as_str() == "new" {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.disks(pool).await
    }
    async fn volumes(&self, pool: &PoolId) -> Outcome<Option<storage_pool::ports::VdMetadata>> {
        self.inner.volumes(pool).await
    }
}

#[tokio::test]
async fn catalogue_change_during_activation_cannot_overwrite_an_existing_name() {
    let metadata = Arc::new(GatedMetadata {
        inner: MemoryMetadata::default(),
        entered: Default::default(),
        release: Default::default(),
    });
    let manager = PoolManager::new(
        Config::default(),
        metadata.clone(),
        Arc::new(SimulatedDataPlane),
    )
    .with_task_ids(TaskIds::for_leader(1).unwrap())
    .with_task_store(Arc::new(MemoryTaskStore::default()));
    let existing = manager.activate(empty("existing")).await.unwrap();
    let activating = manager.clone();
    let pending = tokio::spawn(async move { activating.activate(empty("new")).await });
    tokio::time::timeout(Duration::from_secs(4), metadata.entered.notified())
        .await
        .unwrap();
    existing
        .rename(PoolName::new("new").unwrap(), 0)
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    metadata.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(4), pending)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_err());
    assert_eq!(manager.list().len(), 1);
    assert_eq!(
        manager.get(PoolName::new("new").unwrap()).unwrap().uuid(),
        existing.uuid()
    );
    assert!(manager.get(PoolId::new("new").unwrap()).is_err());
    manager.demote().await.unwrap();
}

fn manager(metadata: Arc<MemoryMetadata>, store: Arc<dyn TaskStore>, epoch: u64) -> PoolManager {
    PoolManager::new(
        Config::default(),
        metadata.clone(),
        Arc::new(SimulatedDataPlane),
    )
    .with_task_ids(TaskIds::for_leader(epoch).unwrap())
    .with_task_store(store)
    .with_balance(Arc::new(SimulatedBalance::new(metadata)))
}
fn empty(name: &str) -> PoolRecovery {
    PoolRecovery::empty(CreatePool::new(
        PoolId::new(name).unwrap(),
        PoolName::new(name).unwrap(),
    ))
}

#[tokio::test]
async fn ordinary_pool_task_history_is_queryable_after_leadership_change_without_scheduling_it() {
    let metadata = Arc::new(MemoryMetadata::default());
    let store = Arc::new(MemoryTaskStore::default());
    let primary = manager(metadata.clone(), store.clone(), 1);
    let pool = primary.activate(empty("history")).await.unwrap();
    let tier = TierId::new("tier").unwrap();
    let mut create = pool.create_tier(tier.clone()).await.unwrap();
    let id = create.id();
    create.result().await.unwrap();
    let before = pool.history_task(id).await.unwrap();
    assert!(!before.recoverable);
    assert_eq!(before.outcome, StoredOutcome::Completed);
    assert!(before.metadata["kind"].get("CreateTier").is_some());
    let snapshot = pool.recovery_snapshot().await.unwrap();
    primary.demote().await.unwrap();

    let standby = manager(metadata, store, 2);
    let recovered = standby.activate(snapshot).await.unwrap();
    assert!(recovered.tasks().is_empty());
    assert_eq!(recovered.scheduling().queued, 0);
    assert_eq!(recovered.scheduling().running, 0);
    let after = recovered.history_task(id).await.unwrap();
    assert_eq!(after.observation, before.observation);
    assert_eq!(after.id, id);
    assert!(recovered.resume(id).is_err());
    recovered
        .delete_tier(tier)
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    let page = recovered
        .history(TaskQuery {
            business: Some(id.business().into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(page.tasks.len(), 1);
    assert_eq!(page.tasks[0].id, id);
    assert_eq!(
        recovered
            .history(TaskQuery::default())
            .await
            .unwrap()
            .tasks
            .len(),
        2
    );
    standby.demote().await.unwrap();
}
async fn wait_for(pool: &PoolHandle, id: TaskId, predicate: impl Fn(&TaskSnapshot) -> bool) {
    let mut observer = pool.observe_task(id).unwrap();
    tokio::time::timeout(Duration::from_secs(6), async {
        loop {
            let task = observer.snapshot();
            assert!(
                !matches!(task.state, RunState::Failed(_)),
                "{:?}",
                task.state
            );
            if predicate(&task) {
                break;
            }
            observer.changed().await.unwrap();
        }
    })
    .await
    .expect("leadership transition stalled");
}

#[tokio::test]
async fn demoting_active_migration_restores_same_task_without_user_resume() {
    let metadata = Arc::new(MemoryMetadata::default());
    let store = Arc::new(MemoryTaskStore::default());
    let primary = manager(metadata.clone(), store.clone(), 20);
    let pool = primary.activate(empty("pool")).await.unwrap();
    let tier = TierId::new("tier").unwrap();
    pool.create_tier(tier.clone())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    for name in ["a", "b", "c", "d"] {
        let node = NodeId::new(format!("node-{name}")).unwrap();
        pool.attach_node(node.clone(), 1, true)
            .await
            .unwrap()
            .result()
            .await
            .unwrap();
        pool.load_node(node.clone())
            .await
            .unwrap()
            .result()
            .await
            .unwrap();
        pool.add_disk(DiskSpec {
            id: DiskId::new(name).unwrap(),
            node,
            tier: tier.clone(),
            capacity_blks: 16,
        })
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    }
    let targets = vec![DiskId::new("a").unwrap(), DiskId::new("b").unwrap()];
    pool.create_volume(CreateVolume {
        id: VolumeId::new("data").unwrap(),
        disks: targets.clone(),
    })
    .await
    .unwrap()
    .result()
    .await
    .unwrap();
    let snapshot = pool.recovery_snapshot().await.unwrap();
    let mut run = pool
        .shrink(ShrinkRequest::new(targets).unwrap())
        .await
        .unwrap();
    let id = run.id();
    wait_for(&pool, id, |t| {
        t.progress.stage.as_deref() == Some("migrating") && t.progress.completed >= 2
    })
    .await;
    primary.demote().await.unwrap();
    assert_eq!(run.result().await, Err(Failure::Cancelled));
    let saved = store.get(id).unwrap().unwrap();
    assert_eq!(saved.outcome, StoredOutcome::Pending);
    assert!(!saved.user_paused && !saved.cancel_requested);
    assert!(
        pool.shrink(ShrinkRequest::new(vec![DiskId::new("c").unwrap()]).unwrap())
            .await
            .is_err()
    );

    let next = manager(metadata, store.clone(), 21);
    let recovered = next.activate(snapshot).await.unwrap();
    assert_eq!(recovered.nodes().await.unwrap().len(), 4);
    assert_eq!(recovered.tiers().await.unwrap().len(), 1);
    wait_for(&recovered, id, |t| t.state == RunState::Completed).await;
    assert_eq!(store.get(id).unwrap().unwrap().owner_epoch, 21);
    assert_eq!(
        store.get(id).unwrap().unwrap().outcome,
        StoredOutcome::Completed
    );
    // Pool-level removal gates, routing and global disk ownership are restored too.
    recovered
        .detach_node(NodeId::new("node-a").unwrap())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    assert_eq!(recovered.disks().await.unwrap().len(), 2);
    next.demote().await.unwrap();
}

#[tokio::test]
async fn activation_serializes_duplicate_identity_and_rejects_reused_epoch() {
    let metadata = Arc::new(MemoryMetadata::default());
    let store = Arc::new(MemoryTaskStore::default());
    let primary = manager(metadata.clone(), store.clone(), 1);
    assert!(
        primary
            .create_pool(PoolId::new("sync-bypass").unwrap())
            .is_err()
    );
    let recovery = empty("pool");
    let (a, b) = tokio::join!(
        primary.activate(recovery.clone()),
        primary.activate(recovery.clone())
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert_eq!(primary.list().len(), 1);
    primary.demote().await.unwrap();
    let stale = manager(metadata.clone(), store.clone(), 1);
    assert!(stale.activate(recovery.clone()).await.is_err());
    assert!(stale.list().is_empty());
    let next = manager(metadata, store, 2);
    next.activate(recovery).await.unwrap();
    next.demote().await.unwrap();
}

#[tokio::test]
async fn invalid_topology_does_not_publish_a_pool_or_consume_its_epoch() {
    let metadata = Arc::new(MemoryMetadata::default());
    let store = Arc::new(MemoryTaskStore::default());
    let primary = manager(metadata.clone(), store.clone(), 1);
    let pool = primary.activate(empty("pool")).await.unwrap();
    let tier = TierId::new("tier").unwrap();
    let node = NodeId::new("node").unwrap();
    pool.create_tier(tier.clone())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    pool.attach_node(node.clone(), 1, true)
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    pool.load_node(node.clone())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    pool.add_disk(DiskSpec {
        id: DiskId::new("disk").unwrap(),
        node,
        tier,
        capacity_blks: 8,
    })
    .await
    .unwrap()
    .result()
    .await
    .unwrap();
    let snapshot = pool.recovery_snapshot().await.unwrap();
    primary.demote().await.unwrap();
    let next = manager(metadata, store, 2);
    let mut incomplete = snapshot.clone();
    incomplete.nodes.clear();
    assert!(next.activate(incomplete).await.is_err());
    assert!(next.list().is_empty());
    let pool = next.activate(snapshot).await.unwrap();
    pool.online_disk(DiskId::new("disk").unwrap())
        .await
        .unwrap()
        .result()
        .await
        .unwrap();
    next.demote().await.unwrap();
}
