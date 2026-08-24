use std::time::Duration;

use mdc_runtime::{
    ServiceShutdown,
    demo::{BgBackend, DemoBlueprint, DiskId, default_demo_catalog},
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let backend = BgBackend::new(Duration::from_millis(10));
    let system = DemoBlueprint::install()?
        .spawn(default_demo_catalog(), backend.clone(), 2)
        .await?;

    let (disk_1, disk_2) = tokio::join!(
        system.disk_offline(DiskId::new("disk-1"), 1),
        system.disk_offline(DiskId::new("disk-2"), 2),
    );

    println!("disk-1: {:?}", disk_1?);
    println!("disk-2: {:?}", disk_2?);
    println!("BG history: {:?}", backend.snapshot());
    println!("rebuild state: {:?}", system.rebuild_snapshot(3).await?);

    system
        .services
        .shutdown_all(ServiceShutdown::Immediate)
        .await?;
    Ok(())
}
