//! cargo run --locked -p storage-pool --example task_center
use storage_pool::{task_center::*, *};
use workflow_foundation::ObjectKey;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let disk = DiskId::new("d1")?;
    let operation = |kind| PoolOperation {
        kind,
        objects: vec![ObjectKey::new("disk", disk.as_str())],
    };
    let online = operation(OperationKind::OnlineDisk(disk.clone()));
    let offline = operation(OperationKind::OfflineDisk(disk.clone()));
    let decision = TaskCenter::conflict(&offline, &online);
    assert_eq!(decision.decision, ConflictDecision::CancelThenWait);
    println!("disk offline versus online: {decision:?}");
    // Extend business rules in task_center/conflicts.rs, not by implementing a policy.
    Ok(())
}
