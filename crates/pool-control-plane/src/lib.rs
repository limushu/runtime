//! Executable business model for the Monitor Pool control plane.

pub mod domains;
pub mod kernel;
pub mod pool;
pub mod pool_manager;
pub mod ports;

pub use domains::member_disk::model::{
    AllocationState, BlkSize, DiskSharing, MediaClass, MemberDiskPatch, MemberDiskRecord,
    MemberDiskSnapshot, MemberDiskSpec, MemberDiskState, MembershipState, PhysicalState,
};
pub use domains::member_disk::MemberDiskService;
pub use domains::pool_node::{MemberDiskIoAvailability, PoolNodeService};
pub use domains::virtual_disk::{EvacuationResult, VirtualDiskService, VirtualDiskStats};
pub use kernel::{
    BgId, BlkId, ByteCount, FailureDomainId, MemberDiskId, NodeId, PhysicalDiskId, PoolId, TierId,
    VirtualDiskId,
};
pub use pool::{Pool, PoolLifecycle, PoolMetadata, PoolPatch, PoolSnapshot, PoolSpec};
pub use pool_manager::{DiskFactResult, PoolManager};
pub use ports::{ControlPlaneStore, InMemoryControlPlaneStore};
