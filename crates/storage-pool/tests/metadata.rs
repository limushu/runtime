use async_trait::async_trait;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use storage_pool::TaskRun;
use storage_pool::{adapters::SimulatedDataPlane, domains::vd::BgEntry, metadata::*, *};
use tokio::sync::Barrier;

fn pool(id: &str) -> PoolId {
    PoolId::new(id).unwrap()
}
fn disk(id: &str) -> DiskId {
    DiskId::new(id).unwrap()
}
fn tier(id: &str) -> TierId {
    TierId::new(id).unwrap()
}
async fn done<T>(mut run: TaskRun<T>) -> Outcome<T> {
    run.result().await
}
fn member(pool: PoolId) -> MemberDisk {
    MemberDisk {
        id: disk("physical-disk"),
        pool,
        node: NodeId::new("node").unwrap(),
        tier: tier("ssd"),
        capacity_blks: 8,
        io: IoState::Down,
        allocation: AllocationState::Active,
        allocated: Default::default(),
        revision: 0,
        shrink: None,
        membership: DiskMembership::Member,
    }
}
async fn setup(metadata: Arc<dyn MetadataStore>) -> (PoolManager, PoolHandle) {
    let manager = PoolManager::new(Config::default(), metadata, Arc::new(SimulatedDataPlane));
    let p = manager.create_pool(pool("pool/a 中文")).unwrap();
    done(p.create_tier(tier("ssd")).await.unwrap())
        .await
        .unwrap();
    for i in 1..=3 {
        let node = NodeId::new(format!("n{i}")).unwrap();
        done(p.attach_node(node.clone(), 1, true).await.unwrap())
            .await
            .unwrap();
        done(p.load_node(node.clone()).await.unwrap())
            .await
            .unwrap();
        done(
            p.add_disk(DiskSpec {
                id: disk(&format!("d{i}")),
                node,
                tier: tier("ssd"),
                capacity_blks: 8,
            })
            .await
            .unwrap(),
        )
        .await
        .unwrap();
    }
    done(
        p.create_volume(CreateVolume {
            id: VolumeId::new("volume").unwrap(),
            disks: vec![disk("d1"), disk("d2")],
        })
        .await
        .unwrap(),
    )
    .await
    .unwrap();
    (manager, p)
}

async fn backend_contract(kv: Arc<dyn KvStore>) {
    assert!(
        kv.transact(KvTransaction {
            checks: vec![KvCheck {
                key: b"a/1".to_vec(),
                expected: None
            }],
            writes: vec![
                KvWrite {
                    key: b"a/1".to_vec(),
                    value: Some(b"one".to_vec())
                },
                KvWrite {
                    key: b"b/2".to_vec(),
                    value: Some(b"two".to_vec())
                }
            ]
        })
        .await
        .unwrap()
    );
    let before = kv
        .snapshot(&[b"a/".to_vec(), b"b/".to_vec()])
        .await
        .unwrap();
    assert_eq!(before.len(), 2);
    assert!(
        !kv.transact(KvTransaction {
            checks: vec![KvCheck {
                key: b"a/1".to_vec(),
                expected: None
            }],
            writes: vec![
                KvWrite {
                    key: b"a/1".to_vec(),
                    value: None
                },
                KvWrite {
                    key: b"b/3".to_vec(),
                    value: Some(b"bad".to_vec())
                }
            ]
        })
        .await
        .unwrap()
    );
    assert_eq!(
        before,
        kv.snapshot(&[b"a/".to_vec(), b"b/".to_vec()])
            .await
            .unwrap()
    );
    assert!(
        kv.transact(KvTransaction {
            checks: vec![KvCheck {
                key: b"a/1".to_vec(),
                expected: Some(b"one".to_vec())
            }],
            writes: vec![KvWrite {
                key: b"a/1".to_vec(),
                value: None
            }]
        })
        .await
        .unwrap()
    );
    assert!(kv.snapshot(&[b"a/".to_vec()]).await.unwrap().is_empty());
}
#[tokio::test]
async fn memory_backend_has_atomic_conditions_batches_and_consistent_prefix_reads() {
    backend_contract(Arc::new(MemoryKv::default())).await;
}

// Force both repository instances to read the same pre-commit view. A per-instance
// mutex cannot satisfy these tests; the backend must enforce the comparisons.
struct ConcurrentKv {
    inner: MemoryKv,
    reads: AtomicUsize,
    barrier: Barrier,
}
impl ConcurrentKv {
    fn new() -> Self {
        Self {
            inner: MemoryKv::default(),
            reads: AtomicUsize::new(0),
            barrier: Barrier::new(2),
        }
    }
}
#[async_trait]
impl KvStore for ConcurrentKv {
    async fn snapshot(&self, prefixes: &[Vec<u8>]) -> Outcome<KvSnapshot> {
        let snapshot = self.inner.snapshot(prefixes).await?;
        if self.reads.fetch_add(1, Ordering::SeqCst) < 2 {
            self.barrier.wait().await;
        }
        Ok(snapshot)
    }
    async fn transact(&self, tx: KvTransaction) -> Outcome<bool> {
        self.inner.transact(tx).await
    }
}
#[tokio::test]
async fn independent_repositories_cannot_claim_the_same_physical_disk_in_different_pools() {
    let kv = Arc::new(ConcurrentKv::new());
    let a = KvMetadata::new(kv.clone());
    let b = KvMetadata::new(kv);
    let pool_a = pool("a");
    let pool_b = pool("b");
    let (x, y) = tokio::join!(
        a.commit(&pool_a, MetadataChange::AddDisk(member(pool_a.clone()))),
        b.commit(&pool_b, MetadataChange::AddDisk(member(pool_b.clone())))
    );
    assert_ne!(x.is_ok(), y.is_ok());
    let count = a.snapshot(&pool("a")).await.unwrap().disks.len()
        + b.snapshot(&pool("b")).await.unwrap().disks.len();
    assert_eq!(count, 1);
}
#[tokio::test]
async fn predicate_revision_contention_revalidates_without_losing_independent_changes() {
    let kv = Arc::new(ConcurrentKv::new());
    let a = KvMetadata::new(kv.clone());
    let b = KvMetadata::new(kv);
    let p = pool("same");
    let (x, y) = tokio::join!(
        a.commit(
            &p,
            MetadataChange::CreateTier(Tier {
                id: tier("a"),
                pool: p.clone(),
                revision: 0
            })
        ),
        b.commit(
            &p,
            MetadataChange::CreateTier(Tier {
                id: tier("b"),
                pool: p.clone(),
                revision: 0
            })
        )
    );
    x.unwrap();
    y.unwrap();
    assert_eq!(a.snapshot(&p).await.unwrap().tiers.len(), 2);
}

#[tokio::test]
async fn remap_is_atomic_and_reopened_repository_sees_placement_allocation_and_owner() {
    let kv = Arc::new(MemoryKv::default());
    let repository = Arc::new(KvMetadata::new(kv.clone()));
    let (manager, p) = setup(repository.clone()).await;
    let before = repository.snapshot(p.id()).await.unwrap();
    let volume = &before.volumes[0];
    let source = volume.groups[0]
        .entries
        .iter()
        .find(|e| e.disk == disk("d1"))
        .unwrap()
        .clone();
    let mutation = |target_revision| MetadataChange::Remap {
        volume: volume.id.clone(),
        expected_revision: volume.revision,
        group: 0,
        source: source.clone(),
        target: BgEntry {
            disk: disk("d3"),
            blk: 0,
            data: DataState::Valid,
        },
        source_revision: before.disk(&disk("d1")).unwrap().revision,
        target_revision,
    };
    assert!(repository.commit(p.id(), mutation(999)).await.is_err());
    assert_eq!(before, repository.snapshot(p.id()).await.unwrap());
    repository
        .commit(p.id(), mutation(before.disk(&disk("d3")).unwrap().revision))
        .await
        .unwrap();
    let after = KvMetadata::new(kv).snapshot(p.id()).await.unwrap();
    assert!(
        !after
            .disk(&disk("d1"))
            .unwrap()
            .allocated
            .contains(&source.blk)
    );
    assert!(after.disk(&disk("d3")).unwrap().allocated.contains(&0));
    assert!(
        after
            .allocations
            .iter()
            .any(|a| a.disk == disk("d3") && a.volume == volume.id)
    );
    assert!(!after.allocations.iter().any(|a| a.disk == disk("d1")));
    assert!(
        after.volumes[0].groups[0]
            .entries
            .iter()
            .any(|e| e.disk == disk("d3"))
    );
    assert!(repository.commit(p.id(), mutation(0)).await.is_err());
    assert_eq!(after, repository.snapshot(p.id()).await.unwrap());
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn deletion_evidence_survives_recreation_and_old_cleanup_cannot_free_new_volume() {
    let kv = Arc::new(MemoryKv::default());
    let repository = Arc::new(KvMetadata::new(kv.clone()));
    let (manager, p) = setup(repository.clone()).await;
    let descriptor = p.descriptor();
    let original = VolumeId::new("volume").unwrap();
    done(p.delete_volume(original.clone()).await.unwrap())
        .await
        .unwrap();
    manager.shutdown().await.unwrap();
    drop(p);
    drop(manager);
    drop(repository);
    let reopened = Arc::new(KvMetadata::new(kv));
    let snapshot = reopened.snapshot(&descriptor.id).await.unwrap();
    assert_eq!(snapshot.deleted_volumes[0].id, original);
    assert_eq!(snapshot.released.len(), 2);
    let manager = PoolManager::new(
        Config::default(),
        reopened.clone(),
        Arc::new(SimulatedDataPlane),
    );
    let p = manager
        .create(CreatePool {
            id: descriptor.id.clone(),
            uuid: descriptor.uuid,
            name: descriptor.name,
        })
        .unwrap();
    p.restore_metadata().await.unwrap();
    for i in 1..=3 {
        let node = NodeId::new(format!("n{i}")).unwrap();
        done(p.attach_node(node.clone(), 2, true).await.unwrap())
            .await
            .unwrap();
        done(p.load_node(node).await.unwrap()).await.unwrap();
    }
    done(
        p.create_volume(CreateVolume {
            id: VolumeId::new("new").unwrap(),
            disks: vec![disk("d1"), disk("d2")],
        })
        .await
        .unwrap(),
    )
    .await
    .unwrap();
    let before = reopened.snapshot(p.id()).await.unwrap();
    done(p.delete_volume(original).await.unwrap())
        .await
        .unwrap();
    assert_eq!(before, reopened.snapshot(p.id()).await.unwrap());
    manager.shutdown().await.unwrap();
}

struct CommitFailure {
    inner: MemoryKv,
    attempts: AtomicUsize,
    applied: bool,
}
#[async_trait]
impl KvStore for CommitFailure {
    async fn snapshot(&self, prefixes: &[Vec<u8>]) -> Outcome<KvSnapshot> {
        self.inner.snapshot(prefixes).await
    }
    async fn transact(&self, tx: KvTransaction) -> Outcome<bool> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        if self.applied {
            self.inner.transact(tx).await?;
            Err(Failure::Uncertain("commit reply lost".into()))
        } else {
            Err(Failure::Failed("transaction rejected".into()))
        }
    }
}
#[tokio::test]
async fn known_failure_has_no_partial_writes_and_ambiguous_commit_is_never_replayed() {
    for applied in [false, true] {
        let kv = Arc::new(CommitFailure {
            inner: MemoryKv::default(),
            attempts: AtomicUsize::new(0),
            applied,
        });
        let repo = KvMetadata::new(kv.clone());
        let p = pool("p");
        let result = repo
            .commit(
                &p,
                MetadataChange::CreateTier(Tier {
                    id: tier("ssd"),
                    pool: p.clone(),
                    revision: 0,
                }),
            )
            .await;
        assert!(if applied {
            matches!(result, Err(Failure::Uncertain(_)))
        } else {
            matches!(result, Err(Failure::Failed(_)))
        });
        assert_eq!(kv.attempts.load(Ordering::SeqCst), 1);
        assert_eq!(
            repo.snapshot(&p).await.unwrap().tiers.len(),
            usize::from(applied)
        );
    }
}
#[tokio::test]
async fn pool_identity_encoding_and_metadata_namespace_are_isolated() {
    let kv = Arc::new(MemoryKv::default());
    let repo = KvMetadata::new(kv.clone());
    kv.transact(KvTransaction {
        checks: vec![],
        writes: vec![KvWrite {
            key: b"app/other".to_vec(),
            value: Some(b"retained".to_vec()),
        }],
    })
    .await
    .unwrap();
    for id in ["a", "a/b", "a 中文"] {
        let p = pool(id);
        repo.commit(
            &p,
            MetadataChange::CreateTier(Tier {
                id: tier("ssd/x"),
                pool: p.clone(),
                revision: 0,
            }),
        )
        .await
        .unwrap();
        assert_eq!(repo.snapshot(&p).await.unwrap().tiers.len(), 1);
    }
    assert_eq!(
        kv.snapshot(&[b"app/".to_vec()]).await.unwrap()[b"app/other".as_slice()],
        b"retained"
    );
}

#[cfg(feature = "kv-redb")]
#[tokio::test]
async fn redb_backend_obeys_the_same_contract() {
    let path = std::env::temp_dir().join(format!(
        "runtime-metadata-contract-{}.redb",
        uuid::Uuid::new_v4()
    ));
    backend_contract(Arc::new(RedbKv::open(&path).unwrap())).await;
    std::fs::remove_file(path).unwrap();
}
#[cfg(feature = "kv-redb")]
#[tokio::test]
async fn redb_reopen_restores_evacuated_intent_and_completes_shrink() {
    use storage_pool::{adapters::SimulatedBalancer, ports::Balancer};
    let path = std::env::temp_dir().join(format!(
        "runtime-metadata-reopen-{}.redb",
        uuid::Uuid::new_v4()
    ));
    let descriptor;
    {
        let repo = Arc::new(KvMetadata::new(Arc::new(RedbKv::open(&path).unwrap())));
        // Initialize using the real facade; a second manager can attach to the same
        // persisted facts only after the first owner has stopped.
        let (manager, p) = setup(repo.clone()).await;
        descriptor = p.descriptor();
        let balance = SimulatedBalancer::new(repo.clone(), std::time::Duration::ZERO);
        let job = storage_pool::domains::balance::EvacuationId::default();
        let d = p.disk(disk("d1")).await.unwrap();
        repo.commit(
            p.id(),
            MetadataChange::ShrinkIntent {
                disk: d.id.clone(),
                expected_revision: d.revision,
                job,
            },
        )
        .await
        .unwrap();
        let d = p.disk(disk("d1")).await.unwrap();
        repo.commit(
            p.id(),
            MetadataChange::DisableAllocation {
                disk: d.id,
                expected_revision: d.revision,
                job,
            },
        )
        .await
        .unwrap();
        balance
            .evacuate(p.id(), &disk("d1"), job)
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        // The formal workflow attaches to this same completed job after takeover.
        manager.shutdown().await.unwrap();
    }
    {
        let repo = Arc::new(KvMetadata::new(Arc::new(RedbKv::open(&path).unwrap())));
        let before = repo.snapshot(&descriptor.id).await.unwrap();
        assert!(before.disk(&disk("d1")).unwrap().allocated.is_empty());
        assert!(
            before.volumes[0].groups[0]
                .entries
                .iter()
                .all(|e| e.disk != disk("d1"))
        );
        let balancer = Arc::new(SimulatedBalancer::new(
            repo.clone(),
            std::time::Duration::ZERO,
        ));
        let manager = PoolManager::new(
            Config::default(),
            repo.clone(),
            Arc::new(SimulatedDataPlane),
        )
        .with_balancer(balancer)
        .unwrap();
        let p = manager
            .open(
                CreatePool {
                    id: descriptor.id,
                    uuid: descriptor.uuid,
                    name: descriptor.name,
                },
                (1..=3)
                    .map(|i| {
                        TopologyEvent::new(
                            2,
                            10,
                            TopologyChange::NodeConnectivity {
                                node: NodeId::new(format!("n{i}")).unwrap(),
                                connected: true,
                            },
                        )
                    })
                    .collect(),
            )
            .await
            .unwrap();
        let removed = done(p.shrink_disk(disk("d1")).await.unwrap())
            .await
            .unwrap();
        assert_eq!(removed.membership, DiskMembership::Removed);
        manager.shutdown().await.unwrap();
    }
    let repo = KvMetadata::new(Arc::new(RedbKv::open(&path).unwrap()));
    assert_eq!(
        repo.snapshot(&pool("pool/a 中文"))
            .await
            .unwrap()
            .disk(&disk("d1"))
            .unwrap()
            .membership,
        DiskMembership::Removed
    );
    drop(repo);
    std::fs::remove_file(path).unwrap();
}
