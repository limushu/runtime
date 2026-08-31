use control_runtime::{RuntimeConfig, RuntimeError};
use pool_control_plane::{
    ByteCount, FailureDomainId, InMemoryControlPlaneStore, MediaClass, MemberDiskId,
    MemberDiskSpec, MemberDiskState, PhysicalDiskId, PhysicalState, PoolId, PoolManager, PoolPatch,
    PoolSpec, TierId,
};
use std::sync::Arc;
use std::time::Duration;

fn member_disk(pool: &PoolId, id: &str, physical: &str) -> MemberDiskSpec {
    MemberDiskSpec::new(
        MemberDiskId::new(id),
        PhysicalDiskId::new(physical),
        pool.clone(),
        TierId::new("capacity"),
        MediaClass::new("hdd"),
        ByteCount::new(4 * 1024 * 1024 * 1024),
        vec![FailureDomainId::new("rack-a")],
    )
}

fn manager(store: Arc<InMemoryControlPlaneStore>) -> PoolManager {
    PoolManager::new(store, RuntimeConfig::default())
}

#[tokio::test]
async fn pool_manager_routes_one_disk_fact_to_its_pool() {
    let store = Arc::new(InMemoryControlPlaneStore::default());
    let manager = manager(store);
    let pool_a = PoolId::new("pool-a");
    let pool_b = PoolId::new("pool-b");
    let disk_a = member_disk(&pool_a, "md-a", "pd-a");
    let disk_b = member_disk(&pool_b, "md-b", "pd-b");
    manager
        .create_pool(PoolSpec::new(pool_a.clone(), "A"), vec![disk_a])
        .await
        .unwrap();
    manager
        .create_pool(PoolSpec::new(pool_b.clone(), "B"), vec![disk_b])
        .await
        .unwrap();

    let routed = manager
        .route_disk_fact(&PhysicalDiskId::new("pd-a"), PhysicalState::Up)
        .await
        .unwrap();
    assert_eq!(routed.len(), 1);
    assert_eq!(routed[0].pool, pool_a);
    assert_eq!(
        manager
            .pool(&PoolId::new("pool-a"))
            .unwrap()
            .member_disks()
            .get(MemberDiskId::new("md-a"))
            .await
            .unwrap()
            .operational_state,
        MemberDiskState::Ua
    );
    assert_eq!(
        manager
            .pool(&pool_b)
            .unwrap()
            .member_disks()
            .get(MemberDiskId::new("md-b"))
            .await
            .unwrap()
            .operational_state,
        MemberDiskState::Da
    );
}

#[tokio::test]
async fn shared_cache_fans_out_but_exclusive_ownership_cannot_be_mixed() {
    let store = Arc::new(InMemoryControlPlaneStore::default());
    let manager = manager(store);
    let pool_a = PoolId::new("pool-a");
    let pool_b = PoolId::new("pool-b");
    manager
        .create_pool(
            PoolSpec::new(pool_a.clone(), "A"),
            vec![member_disk(&pool_a, "cache-a", "shared-cache").shared_cache()],
        )
        .await
        .unwrap();
    manager
        .create_pool(
            PoolSpec::new(pool_b.clone(), "B"),
            vec![member_disk(&pool_b, "cache-b", "shared-cache").shared_cache()],
        )
        .await
        .unwrap();

    let routed = manager
        .route_disk_fact(&PhysicalDiskId::new("shared-cache"), PhysicalState::Up)
        .await
        .unwrap();
    assert_eq!(routed.len(), 2);

    let pool_c = PoolId::new("pool-c");
    assert!(matches!(
        manager
            .create_pool(
                PoolSpec::new(pool_c.clone(), "C"),
                vec![member_disk(&pool_c, "md-c", "shared-cache")],
            )
            .await,
        Err(RuntimeError::Rejected(_))
    ));
}

#[tokio::test]
async fn member_disk_keeps_core_metadata_and_allocation_bitmap() {
    let store = Arc::new(InMemoryControlPlaneStore::default());
    let manager = manager(store);
    let pool_id = PoolId::new("pool-a");
    let pool = manager
        .create_pool(
            PoolSpec::new(pool_id.clone(), "A"),
            vec![member_disk(&pool_id, "md-a", "pd-a")],
        )
        .await
        .unwrap();
    manager
        .route_disk_fact(&PhysicalDiskId::new("pd-a"), PhysicalState::Up)
        .await
        .unwrap();

    let blk = pool
        .member_disks()
        .allocate(MemberDiskId::new("md-a"))
        .await
        .unwrap();
    let allocated = pool
        .member_disks()
        .get(MemberDiskId::new("md-a"))
        .await
        .unwrap();
    assert_eq!(allocated.spec.physical_disk, PhysicalDiskId::new("pd-a"));
    assert_eq!(allocated.spec.tier, TierId::new("capacity"));
    assert_eq!(allocated.spec.media_class.as_str(), "hdd");
    assert_eq!(allocated.total_blocks, 4);
    assert_eq!(allocated.allocated_blocks, 1);

    pool.member_disks()
        .release(MemberDiskId::new("md-a"), blk)
        .await
        .unwrap();
    assert_eq!(
        pool.member_disks()
            .get(MemberDiskId::new("md-a"))
            .await
            .unwrap()
            .allocated_blocks,
        0
    );
}

#[tokio::test]
async fn conflicting_disk_intent_waits_for_offline_to_settle() {
    let store = Arc::new(InMemoryControlPlaneStore::default());
    let manager = manager(store);
    let pool_id = PoolId::new("pool-a");
    let pool = manager
        .create_pool(
            PoolSpec::new(pool_id.clone(), "A"),
            vec![member_disk(&pool_id, "md-a", "pd-a")],
        )
        .await
        .unwrap();
    let disks = pool.member_disks().clone();
    disks
        .apply_physical(MemberDiskId::new("md-a"), PhysicalState::Up)
        .await
        .unwrap();

    let offline_disks = disks.clone();
    let offline = tokio::spawn(async move {
        offline_disks
            .apply_physical(MemberDiskId::new("md-a"), PhysicalState::Down)
            .await
    });
    tokio::time::sleep(Duration::from_millis(20)).await;
    let online = disks
        .apply_physical(MemberDiskId::new("md-a"), PhysicalState::Up)
        .await
        .unwrap();

    assert!(matches!(
        offline.await.unwrap(),
        Err(RuntimeError::Cancelled)
    ));
    assert_eq!(online.operational_state, MemberDiskState::Ua);
    let stats = pool.virtual_disks().stats().await.unwrap();
    assert_eq!(stats.cancelled, 1);
    assert_eq!(stats.stable_stops, 1);
}

#[tokio::test]
async fn pool_metadata_crud_and_cold_restore_use_the_store() {
    let store = Arc::new(InMemoryControlPlaneStore::default());
    let pool_id = PoolId::new("pool-a");
    let first = manager(store.clone());
    let pool = first
        .create_pool(
            PoolSpec::new(pool_id.clone(), "before"),
            vec![member_disk(&pool_id, "md-a", "pd-a")],
        )
        .await
        .unwrap();
    first
        .update_pool(
            &pool_id,
            PoolPatch {
                name: Some("after".into()),
            },
        )
        .await
        .unwrap();
    pool.shutdown().await.unwrap();
    drop(first);

    let restored = manager(store);
    assert_eq!(restored.restore_all().await.unwrap(), vec![pool_id.clone()]);
    let snapshot = restored.get_pool(&pool_id).await.unwrap();
    assert_eq!(snapshot.metadata.spec.name.as_ref(), "after");
    assert_eq!(snapshot.member_disk_count, 1);
    assert_eq!(
        restored
            .pool(&pool_id)
            .unwrap()
            .member_disks()
            .get(MemberDiskId::new("md-a"))
            .await
            .unwrap()
            .operational_state,
        MemberDiskState::Da
    );
}
