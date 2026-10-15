//! cargo run --manifest-path storage-pool/Cargo.toml --example coordination_rules
use std::sync::Arc;
use storage_pool::{adapters::*, coordination::*, *};
use workflow_foundation::{AdmissionDecision, AdmissionPolicy, ObjectKey};

#[tokio::main]
async fn main() -> Outcome<()> {
    // Configuration is regular Rust data. First matching rule wins.
    let mut rules = PoolRules::default();
    rules.rules.insert(
        0,
        CoordinationRule::new(
            "maintenance-offline-waits-for-online",
            OperationSelector::Class(OperationClass::OfflineDisk),
            OperationSelector::Class(OperationClass::OnlineDisk),
            RuleRelation::SameDisk,
            AdmissionDecision::Queue,
        ),
    );
    let policy = RulePolicy::new(rules.clone())?;
    let disk = DiskId::new("disk-1")?;
    let operation = |kind| PoolOperation {
        kind,
        objects: vec![ObjectKey::new("disk", disk.as_str())],
    };
    let evaluation = policy.evaluate(
        &operation(OperationKind::OfflineDisk(disk.clone())),
        &operation(OperationKind::OnlineDisk(disk.clone())),
    );
    println!("Configured admission: {evaluation:?}");

    let mut manager = PoolManager::new(
        Config::default(),
        Arc::new(MemoryMetadata::default()),
        Arc::new(SimulatedDataPlane),
    )
    .with_rules(rules)?;
    let pool = manager.create_pool(PoolId::new("pool-a")?)?;
    let mut run = pool.observe_node(NodeId::new("node-1")?, 1, true).await?;
    let id = run.id();
    let mut observer = pool.observe_task(id)?;
    run.result().await?;
    let snapshot = observer.snapshot();
    println!("Observed task: {snapshot:?}");
    assert_eq!(snapshot.id, id);
    assert_eq!(snapshot.state, workflow_foundation::RunState::Completed);
    assert!(observer.changed().await.is_ok());
    manager.shutdown().await
}
