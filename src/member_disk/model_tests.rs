use super::*;

const GIB: u64 = 1024 * 1024 * 1024;
const TIB: u64 = 1024 * GIB;

fn disk(capacity_bytes: u64) -> MemberDisk {
    MemberDisk::new(
        DiskUuid::new("disk-1"),
        "pool-1",
        "tier-ssd",
        "ssd",
        capacity_bytes,
        vec![
            FailureDomain::new("node", "node-1"),
            FailureDomain::new("rack", "rack-1"),
        ],
    )
    .unwrap()
}

#[test]
fn a_new_member_disk_is_one_authoritative_object() {
    let disk = disk(8 * TIB);

    assert_eq!(disk.uuid(), &DiskUuid::new("disk-1"));
    assert_eq!(disk.state(), MemberDiskState::DownActive);
    assert!(!disk.can_serve_io());
    assert!(!disk.can_allocate());
    assert_eq!(disk.blk_size(), BlkSize::GiB1);
    assert_eq!(disk.allocation_bitmap().total_blks(), 8192);
    assert_eq!(disk.allocation_bitmap().allocated_blks(), 0);
}

#[test]
fn a_disk_larger_than_eight_tib_uses_two_gib_blks() {
    let disk = disk(10 * TIB);

    assert_eq!(disk.state(), MemberDiskState::DownActive);
    assert!(!disk.can_serve_io());
    assert!(!disk.can_allocate());
    assert_eq!(disk.blk_size(), BlkSize::GiB2);
    assert_eq!(disk.allocation_bitmap().total_blks(), 5120);
}

#[test]
fn removed_is_a_projection_not_a_second_stored_state() {
    assert_eq!(
        MemberDiskState::project(
            DiskIoState::Up,
            AllocationState::Inactive,
            MembershipState::Removed,
        ),
        MemberDiskState::Removed
    );
}

#[test]
fn io_up_is_committed_only_after_the_online_workflow() {
    let mut disk = disk(8 * TIB);

    assert!(disk.mark_io_up().unwrap());
    assert_eq!(disk.state(), MemberDiskState::UpActive);
    assert_eq!(disk.allocation_state(), AllocationState::Active);
    assert_eq!(disk.membership_state(), MembershipState::Member);
    assert!(!disk.mark_io_up().unwrap());
}

#[test]
fn allocation_requires_up_active_member() {
    let mut disk = disk(8 * TIB);

    assert_eq!(
        disk.allocate_blk(),
        Err(MemberDiskError::NotAllocatable {
            state: MemberDiskState::DownActive,
        })
    );

    disk.mark_io_up().unwrap();
    disk.disable_allocation().unwrap();
    assert_eq!(disk.state(), MemberDiskState::UpInactive);
    assert!(matches!(
        disk.allocate_blk(),
        Err(MemberDiskError::NotAllocatable { .. })
    ));
}

#[test]
fn allocated_blk_can_be_released_while_draining() {
    let mut disk = disk(8 * TIB);
    disk.mark_io_up().unwrap();
    let first = disk.allocate_blk().unwrap();
    let second = disk.allocate_blk().unwrap();

    assert_eq!(first, BlkId::new(0));
    assert_eq!(second, BlkId::new(1));
    assert_eq!(disk.allocation_bitmap().allocated_blks(), 2);

    disk.disable_allocation().unwrap();
    disk.release_blk(first).unwrap();

    assert_eq!(disk.allocation_bitmap().allocated_blks(), 1);
    assert!(!disk.allocation_bitmap().is_allocated(first));
    assert!(disk.allocation_bitmap().is_allocated(second));
}

#[test]
fn removal_requires_inactive_empty_disk() {
    let mut disk = disk(8 * TIB);
    disk.mark_io_up().unwrap();
    let blk = disk.allocate_blk().unwrap();

    assert_eq!(disk.remove(), Err(MemberDiskError::AllocationStillActive));

    disk.disable_allocation().unwrap();
    assert_eq!(
        disk.remove(),
        Err(MemberDiskError::BlksStillAllocated { allocated_blks: 1 })
    );

    disk.release_blk(blk).unwrap();
    disk.remove().unwrap();
    assert_eq!(disk.state(), MemberDiskState::Removed);
    assert_eq!(disk.remove(), Err(MemberDiskError::AlreadyRemoved));
}

#[test]
fn cancelling_drain_can_enable_allocation_while_down() {
    let mut disk = disk(8 * TIB);
    disk.disable_allocation().unwrap();
    assert_eq!(disk.state(), MemberDiskState::DownInactive);

    disk.enable_allocation().unwrap();
    assert_eq!(disk.state(), MemberDiskState::DownActive);
    assert!(!disk.can_allocate());
}

#[test]
fn planned_removal_stops_effective_io() {
    let mut disk = disk(8 * TIB);
    disk.mark_io_up().unwrap();
    disk.disable_allocation().unwrap();

    disk.remove().unwrap();

    assert_eq!(disk.io_state(), DiskIoState::Down);
    assert_eq!(disk.state(), MemberDiskState::Removed);
}

#[test]
fn removed_disk_can_rejoin_as_up_active() {
    let mut disk = disk(8 * TIB);
    disk.disable_allocation().unwrap();
    disk.remove().unwrap();

    assert!(disk.rejoin().unwrap());
    assert_eq!(disk.state(), MemberDiskState::UpActive);
    assert_eq!(disk.membership_state(), MembershipState::Member);
    assert!(disk.can_allocate());
    assert!(!disk.rejoin().unwrap());
}
