use pool_control_plane::member_disk::{
    Accepted, AllocateBlks, Allocation, GetMemberDisk, MemberDisk, MemberDiskClient,
    MemberDiskEvent,
};

#[allow(dead_code)]
async fn typed_calls_are_usable_without_the_private_protocol(
    client: &MemberDiskClient,
    event: MemberDiskEvent,
    get: GetMemberDisk,
) {
    let _: Accepted = client.call(event).await.unwrap();
    let _: MemberDisk = client.call(get).await.unwrap();
    let _: Allocation = client.call(AllocateBlks::new("tier-ssd", 1)).await.unwrap();
}

#[test]
fn public_api_compiles() {}
