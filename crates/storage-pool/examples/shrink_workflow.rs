//! Real workflow + simulated storage/C bridge. Run with `cargo run -p storage-pool --example shrink_workflow`.
use std::{sync::Arc, time::Duration};
use storage_pool::{adapters::*, *};
use workflow_foundation::{Outcome, RunState};

#[tokio::main]
async fn main() -> Outcome<()> {
    let storage = Arc::new(SimulatedShrinkStorage::default());
    let balance = Arc::new(SimulatedBalance::new(storage.clone()));
    let manager = PoolManager::new(
        Config::default(),
        Arc::new(MemoryMetadata::default()),
        Arc::new(SimulatedDataPlane),
    )
    .with_shrink(storage.clone(), balance.clone());
    let pool = manager.create_pool(PoolId::new("pool-demo")?)?;
    let targets = vec![DiskId::new("disk-a")?, DiskId::new("disk-b")?];
    let mut run = pool.shrink(ShrinkRequest::new(targets)?).await?;
    let mut observer = pool.observe_task(run.id())?;
    loop {
        let snapshot = observer.changed().await?;
        if snapshot.progress.stage.as_deref() == Some("migrating")
            && snapshot.progress.completed >= 2
        {
            break;
        }
    }
    println!("Task {}: migrating the whole disk set", run.id());

    // A real topology workflow uses the same TaskCenter policy, pauses migration,
    // waits for its invocation to end, then completes and releases the pause reason.
    let mut topo = pool.create_tier(TierId::new("new-tier")?).await?;
    topo.result().await?;
    println!("topology completed after migration stopped; shrink continues automatically");

    pool.suspend(run.id())?;
    pool.wait_suspended(run.id()).await?;
    storage.set_ready(false);
    pool.resume(run.id())?;
    loop {
        if matches!(pool.task(run.id())?.state, RunState::WaitingConditions(_)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    println!("resume requested, waiting for placement conditions");
    storage.set_ready(true);
    pool.recheck_task(run.id())?;
    run.result().await?;
    println!(
        "Task {} completed in {} migration invocations",
        run.id(),
        balance.submissions()
    );
    for (stage, disks) in storage.steps() {
        println!("  {stage:?}: {} disks", disks.len());
    }
    manager.shutdown().await
}
