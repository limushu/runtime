use std::sync::Arc;
use storage_pool::{adapters::*, *};

#[tokio::main]
async fn main() -> Outcome<()> {
    let metadata = Arc::new(MemoryMetadata::default());
    let mut manager = PoolManager::new(
        Config::default(),
        metadata.clone(),
        Arc::new(SimulatedDataPlane),
    );
    let pool = manager.create_pool(PoolId::new("pool-a")?)?;
    let mut disks = Vec::new();
    for index in 1..=2 {
        let node = NodeId::new(format!("node-{index}"))?;
        pool.observe_node(node.clone(), 1, true)
            .await?
            .result()
            .await?;
        pool.load_node(node.clone()).await?.result().await?;
        let disk = DiskId::new(format!("disk-{index}"))?;
        manager
            .online_disk(
                pool.id(),
                DiskSpec {
                    id: disk.clone(),
                    node,
                    tier: TierId::new("ssd")?,
                    capacity_blks: 8,
                },
            )
            .await?
            .result()
            .await?;
        disks.push(disk);
    }
    let volume = pool
        .create_volume(CreateVolume {
            id: VolumeId::new("data")?,
            disks: disks.clone(),
        })
        .await?
        .result()
        .await?;
    println!("SIMULATION: created {:?}", volume);
    pool.offline_disk(disks[0].clone()).await?.result().await?;
    pool.online_disk(disks[0].clone()).await?.result().await?;
    println!(
        "After offline/online (replica still Invalid): {:?}",
        pool.volumes().await?
    );
    println!(
        "Acknowledged in-memory metadata changes: {}",
        metadata.committed().len()
    );
    manager.shutdown().await
}
