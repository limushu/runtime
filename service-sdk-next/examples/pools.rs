//! Formal library entry point: cargo run --example pools
use async_trait::async_trait;
use service_sdk_next::{
    member_disk::*,
    pool::{Pool, PoolConfig, PoolManager},
    process_manager::*,
    runtime::ServiceConfig,
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
        .call(ProcessCommand::ApplyEvents(vec![DiskEvent {
            disk: "disk-1".into(),
            state: PhysicalState::Down,
            observed_at: now_millis(),
        }]))
        .await
        .unwrap();
    let ProcessQueryReply::Disk(Some(a_disk)) = a
        .client
        .query(ProcessQuery::Get("disk-1".into()))
        .await
        .unwrap()
    else {
        panic!()
    };
    let ProcessQueryReply::Disk(Some(b_disk)) = b
        .client
        .query(ProcessQuery::Get("disk-1".into()))
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(a_disk.state(), MemberDiskState::Removed);
    assert_eq!(b_disk.state(), MemberDiskState::UpActive);
    println!("pool-a: {:?}, pool-b: {:?}", a_disk.state(), b_disk.state());
    manager.remove("pool-a").await.unwrap();
    let allocation = b
        .client
        .call(ProcessCommand::Allocate(AllocateBlocks { count: 2 }))
        .await
        .unwrap();
    println!("pool-a stopped; pool-b still allocates: {allocation:?}");
    manager.remove("pool-b").await.unwrap();
}
