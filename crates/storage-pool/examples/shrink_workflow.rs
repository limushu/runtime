//! Remove pool member disks using simulated C migration, preserving the existing VD and BGs.
use std::sync::Arc;
use storage_pool::{adapters::*, *};
use workflow_foundation::Outcome;

#[tokio::main]
async fn main() -> Outcome<()> {
    let metadata = Arc::new(MemoryMetadata::default());
    let balance = Arc::new(SimulatedBalance::new(metadata.clone()));
    let manager = PoolManager::new(Config::default(), metadata, Arc::new(SimulatedDataPlane))
        .with_balance(balance.clone());
    let pool = manager.create_pool(PoolId::new("pool-demo")?)?;
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
    pool.create_tier(TierId::new("new-tier")?)
        .await?
        .result()
        .await?;
    println!("topology completed after native migration stopped; shrink continues automatically");
    run.result().await?;
    println!(
        "Task {} completed in {} migration invocations",
        run.id(),
        balance.submissions()
    );
    println!(
        "Remaining disks: {:?}",
        pool.disks()
            .await?
            .iter()
            .map(|d| d.id.as_str())
            .collect::<Vec<_>>()
    );
    println!(
        "VD replicas after migration: {:?}",
        pool.volumes().await?[0].groups[0].entries
    );
    manager.shutdown().await
}
