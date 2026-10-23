//! One application-owned database, two repositories: pool metadata and task history.
//! TaskRun twice with the same path to observe persisted Removed state and old tasks.
use std::{sync::Arc, time::Duration};
use storage_pool::TaskRun;
use storage_pool::{
    adapters::{SimulatedBalancer, SimulatedDataPlane},
    metadata::*,
    task_center::persistence::*,
    *,
};
async fn done<T>(mut run: TaskRun<T>) -> Outcome<T> {
    run.result().await
}
#[tokio::main(flavor = "current_thread")]
async fn main() -> Outcome<()> {
    let path = std::env::args()
        .nth(1)
        .ok_or_else(|| Failure::Rejected("usage: metadata_kv <database-path>".into()))?;
    let db = Arc::new(redb::Database::create(&path).map_err(|e| Failure::Failed(e.to_string()))?);
    let metadata = Arc::new(KvMetadata::new(Arc::new(RedbKv::from_database(
        db.clone(),
    )?)));
    let history = Arc::new(RedbTaskStore::from_database(db)?);
    let balancer = Arc::new(SimulatedBalancer::new(
        metadata.clone(),
        Duration::from_millis(50),
    ));
    let manager = PoolManager::new(
        Config::default(),
        metadata.clone(),
        Arc::new(SimulatedDataPlane),
    )
    .with_balancer(balancer)?
    .with_task_store(history, PersistenceConfig::default())?;
    let id = PoolId::new("metadata-demo")?;
    let known = metadata.snapshot(&id).await?;
    let nodes: std::collections::BTreeSet<_> = known.disks.iter().map(|d| d.node.clone()).collect();
    // This example explicitly simulates fresh connectivity for these nodes.
    // Production must obtain the facts from its topology source, never from persisted IO Up.
    let topology = nodes
        .into_iter()
        .map(|node| {
            TopologyEvent::new(
                2,
                10,
                TopologyChange::NodeConnectivity {
                    node,
                    connected: true,
                },
            )
        })
        .collect();
    let p = manager
        .open(
            CreatePool {
                id,
                name: PoolName::new("metadata-demo")?,
                uuid: "556c6101-6983-45c6-b856-52596df956d3".parse()?,
            },
            topology,
        )
        .await?;
    let report = p.recovery_report().unwrap();
    println!(
        "Automatically reconstructed {} task(s), {} blocked",
        report.executions.len(),
        report.blocked.len()
    );
    let existing = metadata.snapshot(p.id()).await?;
    if existing
        .disks
        .iter()
        .any(|d| d.membership == DiskMembership::Removed)
    {
        println!(
            "Reopened {path}: d1 is Removed; {} volumes retained",
            p.volumes().await?.len()
        );
    } else {
        let tier = TierId::new("ssd")?;
        done(p.create_tier(tier.clone()).await?).await?;
        for i in 1..=3 {
            let node = NodeId::new(format!("n{i}"))?;
            // This example supplies simulated live topology; database records alone
            // never establish connectivity or loaded state.
            done(p.attach_node(node.clone(), 1, true).await?).await?;
            done(p.load_node(node.clone()).await?).await?;
            done(
                p.add_disk(DiskSpec {
                    id: DiskId::new(format!("d{i}"))?,
                    node,
                    tier: tier.clone(),
                    capacity_blks: 8,
                })
                .await?,
            )
            .await?;
        }
        done(
            p.create_volume(CreateVolume {
                id: VolumeId::new("v1")?,
                disks: vec![DiskId::new("d1")?, DiskId::new("d2")?],
            })
            .await?,
        )
        .await?;
        let removed = done(p.shrink_disk(DiskId::new("d1")?).await?).await?;
        println!(
            "Stored {} as {:?} in {path}",
            removed.id.as_str(),
            removed.membership
        );
    }
    p.flush_task_history().await?;
    let history = p
        .task_history(TaskQuery {
            limit: 5,
            ..Default::default()
        })
        .await?;
    println!("Latest {} durable task records", history.tasks.len());
    manager.shutdown().await?;
    p.close_task_store().await
}
