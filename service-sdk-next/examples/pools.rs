//! Formal library entry point: cargo run --example pools
use async_trait::async_trait;
use service_sdk_next::{
    member_disk::*,
    pool::{Pool, PoolConfig, PoolManager},
    process_manager::*,
    runtime::ServiceConfig,
    workflows,
};
use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

struct MemoryPorts {
    pool: &'static str,
}
#[async_trait]
impl MemberDiskMetadata for MemoryPorts {
    async fn commit(&self, changes: Vec<MemberDiskCommit>) -> Vec<Result<(), PortError>> {
        println!("{} commits: {changes:?}", self.pool);
        changes.into_iter().map(|_| Ok(())).collect()
    }
}
#[async_trait]
impl PoolNodes for MemoryPorts {
    async fn open_disks(&self, disks: Vec<DiskUuid>) -> Result<Vec<DiskOpenResult>, PortError> {
        Ok(disks
            .into_iter()
            .map(|disk| DiskOpenResult {
                disk,
                result: Ok(()),
            })
            .collect())
    }
    async fn push_disk_states(&self, _: Vec<DiskStateChange>) -> Result<(), PortError> {
        Ok(())
    }
}
#[async_trait]
impl VirtualDisks for MemoryPorts {
    async fn evacuate(&self, _: &DiskUuid, _: &CancellationToken) -> Result<(), PortError> {
        Ok(())
    }
}

fn make_pool(id: &'static str) -> Pool {
    // Separate provider scope per Pool, even with equal local member names.
    let ports = Arc::new(MemoryPorts { pool: id });
    Pool::new(
        id,
        vec![MemberDisk::new("disk-1".into(), id, 16)],
        ports.clone(),
        ports.clone(),
        ports,
        PoolConfig {
            recovery_window: Duration::ZERO,
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut manager = PoolManager::default();
    let a = manager
        .insert(make_pool("pool-a"), ServiceConfig::default())
        .unwrap();
    let b = manager
        .insert(make_pool("pool-b"), ServiceConfig::default())
        .unwrap();
    a.client
        .call(ProcessCommand::Start(workflows::offline(
            &a,
            "disk-1".into(),
        )))
        .await
        .unwrap();
    let a_disk = a.disks.get("disk-1".into()).await.unwrap();
    let b_disk = b.disks.get("disk-1".into()).await.unwrap();
    assert_eq!(a_disk.state(), MemberDiskState::Removed);
    assert_eq!(b_disk.state(), MemberDiskState::UpActive);
    println!("pool-a: {:?}, pool-b: {:?}", a_disk.state(), b_disk.state());
    manager.remove("pool-a").await.unwrap();
    let disks = b.disks.clone();
    b.client
        .call(ProcessCommand::Start(WorkflowTask::new(
            "allocate",
            vec![workflows::disk_collection()],
            move |mut ctx| async move {
                let blocks = disks.allocate(&mut ctx, 2).await?;
                println!("pool-a stopped; pool-b still allocates: {blocks:?}");
                Ok(())
            },
        )))
        .await
        .unwrap();
    manager.remove("pool-b").await.unwrap();
}
