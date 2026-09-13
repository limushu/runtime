use async_trait::async_trait;
use service_sdk_next::{member_disk::*, runtime::ServiceConfig};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

struct DemoPorts;

#[async_trait]
impl MemberDiskMetadata for DemoPorts {
    async fn commit(&self, changes: Vec<MemberDiskCommit>) -> Vec<Result<(), PortError>> {
        println!("SDB       {changes:?}");
        changes.into_iter().map(|_| Ok(())).collect()
    }
}

#[async_trait]
impl PoolNodes for DemoPorts {
    async fn open_disks(&self, disks: Vec<DiskUuid>) -> Result<Vec<DiskOpenResult>, PortError> {
        println!("OPEN      {disks:?}");
        Ok(disks
            .into_iter()
            .map(|disk| DiskOpenResult {
                disk,
                result: Ok(()),
            })
            .collect())
    }

    async fn push_disk_states(&self, changes: Vec<DiskStateChange>) -> Result<(), PortError> {
        println!("USER_DP   {changes:?}");
        Ok(())
    }
}

#[async_trait]
impl VirtualDisks for DemoPorts {
    async fn evacuate(
        &self,
        disk: &DiskUuid,
        _cancel: &CancellationToken,
    ) -> Result<(), PortError> {
        println!("VDM       evacuate {disk}");
        Ok(())
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let ports = Arc::new(DemoPorts);
    let service = MemberDiskService::new(
        vec![
            MemberDisk::new("disk-a".into(), "pool-1", 32),
            MemberDisk::new("disk-b".into(), "pool-1", 32),
        ],
        ports.clone(),
        ports.clone(),
        ports,
        MemberDiskConfig {
            recovery_window: Duration::ZERO,
            ..MemberDiskConfig::default()
        },
    )
    .unwrap()
    .start(ServiceConfig::default());

    let allocation = service
        .client
        .call(MemberDiskCommand::Allocate(AllocateBlocks { count: 4 }))
        .await
        .unwrap();
    println!("ALLOCATED {allocation:?}");

    let offline = service
        .client
        .call(MemberDiskCommand::ApplyEvents(vec![
            DiskEvent {
                disk: "disk-a".into(),
                state: PhysicalState::Down,
                observed_at: now_millis(),
            },
            DiskEvent {
                disk: "disk-b".into(),
                state: PhysicalState::Down,
                observed_at: now_millis(),
            },
        ]))
        .await
        .unwrap();
    println!("OFFLINE   {offline:#?}");

    println!("SNAPSHOT  {:#?}", service.observer.snapshot());
    service.control.drain().await.unwrap();
    service.join().await.unwrap();
}
