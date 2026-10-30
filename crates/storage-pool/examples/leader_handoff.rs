//! Real file TaskStore + public PoolManager activation; simulated metadata/data plane.
//! The externally elected epochs are explicit. This example does not implement election.
use std::{path::Path, sync::Arc, time::Duration};
use storage_pool::{adapters::*, ports::*, *};
use workflow_foundation::{RunState, persistence::*};

async fn wait_for(
    pool: &PoolHandle,
    id: TaskId,
    test: impl Fn(&TaskSnapshot) -> bool,
) -> Outcome<TaskSnapshot> {
    let mut observer = pool.observe_task(id)?;
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let snapshot = observer.snapshot();
            if test(&snapshot) {
                return Ok(snapshot);
            }
            if let RunState::Failed(error) = snapshot.state {
                return Err(error);
            }
            observer.changed().await?;
        }
    })
    .await
    .map_err(|_| Failure::Failed("handoff demo stalled".into()))?
}

pub async fn run_handoff(path: &Path) -> Outcome<()> {
    let metadata = Arc::new(MemoryMetadata::default());
    let balance = Arc::new(SimulatedBalance::new(metadata.clone()));
    let store = Arc::new(FileTaskStore::open(path)?);
    let primary = PoolManager::new(
        Config::default(),
        metadata.clone(),
        Arc::new(SimulatedDataPlane),
    )
    .with_task_ids(TaskIds::for_leader(11)?)
    .with_task_store(store.clone())
    .with_balance(balance.clone());
    let pool = primary
        .activate(PoolRecovery::empty(CreatePool::new(
            PoolId::new("handoff")?,
            PoolName::new("handoff")?,
        )))
        .await?;
    println!(
        "[1] primary epoch=11 activated through PoolManager; file journal={}",
        path.display()
    );
    let tier = TierId::new("tier")?;
    pool.create_tier(tier.clone()).await?.result().await?;
    for name in ["a", "b", "c", "d"] {
        let node = NodeId::new(format!("node-{name}"))?;
        pool.attach_node(node.clone(), 1, true)
            .await?
            .result()
            .await?;
        pool.load_node(node.clone()).await?.result().await?;
        pool.add_disk(DiskSpec {
            id: DiskId::new(name)?,
            node,
            tier: tier.clone(),
            capacity_blks: 16,
        })
        .await?
        .result()
        .await?;
    }
    let targets = vec![DiskId::new("a")?, DiskId::new("b")?];
    pool.create_volume(CreateVolume {
        id: VolumeId::new("data")?,
        disks: targets.clone(),
    })
    .await?
    .result()
    .await?;
    let mut run = pool.shrink(ShrinkRequest::new(targets.clone())?).await?;
    let id = run.id();
    let old_ref = pool.task_ref(id);
    wait_for(&pool, id, |s| {
        s.progress.stage.as_deref() == Some("migrating") && s.progress.completed >= 2
    })
    .await?;
    pool.suspend(id)?;
    pool.wait_suspended(id).await?;
    let snapshot = pool.recovery_snapshot().await?;
    let copied = pool.task(id)?.details["migration"]["progress"]["completed"].clone();
    assert!(copied.as_u64().unwrap() >= 2);
    println!("[2] Task {id} paused after real migration settlement; saved progress={copied}");
    primary.demote().await?;
    assert_eq!(run.result().await, Err(Failure::Cancelled));
    assert!(
        pool.create_tier(TierId::new("old-primary-rejected")?)
            .await
            .is_err()
    );
    let old_record = store.get(id)?.expect("shrink record");
    assert_eq!(old_record.outcome, StoredOutcome::Pending);
    assert!(old_record.user_paused);
    println!(
        "[3] primary demoted; old handle rejects work; Task remains Pending, not user-cancelled"
    );
    drop(run);
    drop(pool);
    drop(primary);
    drop(store);

    // Fresh observation after leadership was lost: migration conditions changed.
    let disk = metadata
        .disks(&snapshot.descriptor.id)
        .await?
        .unwrap()
        .disks
        .into_iter()
        .find(|d| d.id == targets[0])
        .unwrap();
    metadata
        .commit(
            &snapshot.descriptor.id,
            MetadataChange::DiskIo {
                disk: disk.id,
                expected_revision: disk.revision,
                io: IoState::Down,
            },
        )
        .await?;
    let store = Arc::new(FileTaskStore::open(path)?);
    let standby = PoolManager::new(
        Config::default(),
        metadata.clone(),
        Arc::new(SimulatedDataPlane),
    )
    .with_task_ids(TaskIds::for_leader(12)?)
    .with_task_store(store.clone())
    .with_balance(balance.clone());
    let pool = standby.activate(snapshot).await?;
    pool.wait_suspended(id).await?;
    assert_eq!(pool.current_shrink().unwrap().id, id);
    assert!(pool.task_by_ref(old_ref).is_err());
    assert_eq!(
        pool.task(id)?.details["migration"]["progress"]["completed"],
        copied
    );
    assert_eq!(pool.history_task(id).await?.owner_epoch, 12);
    let mut stale = old_record.clone();
    stale.revision += 1;
    assert!(store.save(stale, Some(old_record.revision)).is_err());
    println!(
        "[4] standby promoted to epoch=12; same TaskId/checkpoint/user pause; stale writes rejected"
    );
    let before = balance.submissions();
    assert_eq!(before, 1);
    pool.resume(id)?;
    wait_for(&pool, id, |s| {
        matches!(s.state, RunState::WaitingConditions(_))
    })
    .await?;
    assert_eq!(balance.submissions(), before);
    println!("[5] resume rechecks admission: target a is down, no migration submitted");
    let mut online = pool.online_disk(targets[0].clone()).await?;
    assert!(online.id() > id);
    online.result().await?;
    pool.recheck_task(id)?;
    wait_for(&pool, id, |s| s.state == RunState::Completed).await?;
    assert_eq!(
        pool.history_task(id).await?.outcome,
        StoredOutcome::Completed
    );
    assert_eq!(pool.disks().await?.len(), 2);
    assert_eq!(balance.submissions(), 2);
    // Recovered Pool ownership/routing must be cleaned too, not only metadata.
    let mut detach = pool.detach_node(NodeId::new("node-a")?).await?;
    detach.result().await?;
    println!(
        "[6] topology online completed; original Task resumed and removed a/b; membership indexes cleaned"
    );
    println!(
        "[7] migration submissions={}, fresh task epoch={}, completed Task={id}",
        balance.submissions(),
        online.id().epoch
    );
    standby.demote().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Outcome<()> {
    let path =
        std::env::temp_dir().join(format!("workflow-handoff-{}.journal", uuid::Uuid::new_v4()));
    run_handoff(&path).await
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn file_backed_handoff_through_public_pool_api() {
        let path = std::env::temp_dir().join(format!(
            "workflow-handoff-test-{}.journal",
            uuid::Uuid::new_v4()
        ));
        super::run_handoff(&path).await.unwrap();
        std::fs::remove_file(path).unwrap();
    }
}
