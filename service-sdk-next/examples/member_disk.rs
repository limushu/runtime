use async_trait::async_trait;
use service_sdk_next::{
    member_disk::*,
    pool::{Pool, PoolConfig},
    process_manager::*,
    runtime::ServiceConfig,
    workflows,
};
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
    let mut service = Pool::new(
        "pool-1",
        vec![
            MemberDisk::new("disk-a".into(), "pool-1", 32),
            MemberDisk::new("disk-b".into(), "pool-1", 32),
        ],
        ports.clone(),
        ports.clone(),
        ports,
        PoolConfig {
            recovery_window: Duration::ZERO,
            ..PoolConfig::default()
        },
    )
    .unwrap()
    .start(ServiceConfig::default());

    let h = &service.handle;
    let disks = h.disks.clone();
    h.client
        .call(ProcessCommand::Start(WorkflowTask::new(
            "allocate",
            vec![workflows::disk_collection()],
            move |mut ctx| async move {
                println!("ALLOCATED {:?}", disks.allocate(&mut ctx, 4).await?);
                Ok(())
            },
        )))
        .await
        .unwrap();
    for disk in ["disk-a", "disk-b"] {
        h.client
            .submit(ProcessCommand::Start(workflows::offline(h, disk.into())))
            .await
            .unwrap();
    }
    h.control.drain().await.unwrap();
    println!("WORKFLOWS {:#?}", h.observer.snapshot());
    println!("DISKS     {:#?}", h.disks.list().await.unwrap());
    println!("DISK RUNTIME {:?}", h.disk_observer.snapshot());
    println!("VD RUNTIME   {:?}", h.vd_observer.snapshot());
    service.shutdown().await.unwrap();
}
