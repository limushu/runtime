//! Explicit simulators, not persistence or real user_dp implementations.
use crate::support::reject;
use crate::{ports::*, *};
use async_trait::async_trait;
mod balancer;
mod data_plane;
mod memory_metadata;
pub use balancer::SimulatedBalancer;
pub use data_plane::SimulatedDataPlane;
pub use memory_metadata::MemoryMetadata;
pub(crate) struct UnconfiguredBalancer;
#[async_trait]
impl Balancer for UnconfiguredBalancer {
    async fn check_shrink(
        &self,
        _: &PoolId,
        _: &DiskId,
    ) -> Outcome<crate::domains::balance::ShrinkReadiness> {
        Err(reject("balancing service is not configured"))
    }
    async fn evacuate(
        &self,
        _: &PoolId,
        _: &DiskId,
        _: crate::domains::balance::EvacuationId,
    ) -> Outcome<ArcJob> {
        Err(reject("balancing service is not configured"))
    }
    async fn find(
        &self,
        _: &PoolId,
        _: crate::domains::balance::EvacuationId,
    ) -> Outcome<Option<ArcJob>> {
        Err(reject("balancing service is not configured"))
    }
}
