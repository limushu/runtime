use pool_control_plane::member_disk::{
    Accepted, AllocateBlks, Allocation, DiskUuid, MemberDisk, MemberDiskClient, MemberDiskEvent,
};
use pool_control_plane::runtime::OperationContext;

#[allow(dead_code)]
async fn facade_calls_are_usable_without_the_private_protocol(
    client: &MemberDiskClient,
    operation: &OperationContext,
    event: MemberDiskEvent,
    disk: DiskUuid,
) {
    let _: Accepted = client.submit_in(operation, event).await.unwrap();
    let _: MemberDisk = client.get(disk).await.unwrap();
    let _: Allocation = client
        .allocate_blks_in(operation, AllocateBlks::new("tier-ssd", 1))
        .await
        .unwrap();
}

#[test]
fn public_api_compiles() {}
