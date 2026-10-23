//! Runs the production Workflow/Pool facade with explicit metadata/data-plane/balancing
//! simulators. No physical IO is performed. Replace the injected ports in production.
use std::{sync::Arc, time::Duration};
use storage_pool::{
    adapters::{MemoryMetadata, SimulatedBalancer, SimulatedDataPlane},
    task_center::persistence::*,
    *,
};

async fn finish<T>(mut run: Run<T>) -> Outcome<T> {
    run.result().await
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Outcome<()> {
    let metadata = Arc::new(MemoryMetadata::default());
    let balancer = Arc::new(SimulatedBalancer::new(
        metadata.clone(),
        Duration::from_millis(250),
    ));
    let manager = PoolManager::new(Config::default(), metadata, Arc::new(SimulatedDataPlane))
        .with_balancer(balancer.clone())?
        .with_task_store(
            Arc::new(MemoryTaskStore::default()),
            PersistenceConfig::default(),
        )?;
    let pool = manager.create_pool(PoolId::new("pool-1")?)?;
    pool.recover_tasks().await?;
    let tier = TierId::new("ssd")?;
    finish(pool.create_tier(tier.clone()).await?).await?;
    for index in 1..=3 {
        let node = NodeId::new(format!("n{index}"))?;
        finish(pool.attach_node(node.clone(), 1, true).await?).await?;
        finish(pool.load_node(node.clone()).await?).await?;
        finish(
            pool.add_disk(DiskSpec {
                id: DiskId::new(format!("d{index}"))?,
                node,
                tier: tier.clone(),
                capacity_blks: 16,
            })
            .await?,
        )
        .await?;
    }
    for index in 1..=3 {
        finish(
            pool.create_volume(CreateVolume {
                id: VolumeId::new(format!("v{index}"))?,
                disks: vec![DiskId::new("d1")?, DiskId::new("d2")?],
            })
            .await?,
        )
        .await?;
    }
    let mut shrink = pool.shrink_disk(DiskId::new("d1")?).await?;
    let task_id = pool.persistent_task_id(shrink.id())?;
    let mut observer = pool.observe_task(shrink.id())?;
    loop {
        let task = observer.snapshot();
        println!("Task {task_id}: {:?}, {:?}", task.state, task.progress);
        if task
            .progress
            .stage
            .as_ref()
            .is_some_and(|s| s.starts_with("evacuating "))
            && task.progress.completed >= 1
        {
            break;
        }
        observer.changed().await?;
    }
    println!("submit a real Pool topo workflow while evacuation is in flight");
    finish(pool.load_node(NodeId::new("n2")?).await?).await?;
    let removed = shrink.result().await?;
    println!(
        "Result: {} {:?}, IO {:?}, allocation {:?}",
        removed.id.as_str(),
        removed.membership,
        removed.io,
        removed.allocation
    );
    for activity in &pool.task(shrink.id())?.activities.entries {
        println!(
            "  {:2} {:38} {:?}",
            activity.id.sequence, activity.name, activity.state
        );
    }
    for volume in pool.volumes().await? {
        let disks: Vec<_> = volume
            .groups
            .iter()
            .flat_map(|g| &g.entries)
            .map(|e| e.disk.as_str())
            .collect();
        println!("  {} now references {disks:?}", volume.id.as_str());
    }
    pool.flush_task_history().await?;
    let saved = pool.task_detail(task_id).await?;
    println!(
        "History: {:?}, {} execution(s), {} balancing job(s)",
        saved.status,
        saved.executions.len(),
        balancer.jobs_started()
    );
    manager.shutdown().await
}
