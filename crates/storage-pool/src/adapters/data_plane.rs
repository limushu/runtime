use crate::{ports::*, *};
use async_trait::async_trait;
use tokio_util::sync::CancellationToken;
#[derive(Default)]
pub struct SimulatedDataPlane;
fn ready(cancel: &CancellationToken) -> Outcome<()> {
    if cancel.is_cancelled() {
        Err(Failure::Cancelled)
    } else {
        Ok(())
    }
}
#[async_trait]
impl DataPlane for SimulatedDataPlane {
    async fn unload_pool(&self, _: &PoolId, _: &NodeId, cancel: &CancellationToken) -> Outcome<()> {
        ready(cancel)
    }
    async fn close_disk(&self, _: &PoolId, _: &DiskId, cancel: &CancellationToken) -> Outcome<()> {
        ready(cancel)
    }
    async fn delete_volume(
        &self,
        _: &PoolId,
        _: &VolumeId,
        cancel: &CancellationToken,
    ) -> Outcome<()> {
        ready(cancel)
    }
    async fn open_disk(&self, _: &PoolId, _: &DiskId, cancel: &CancellationToken) -> Outcome<()> {
        ready(cancel)
    }
    async fn load_pool(&self, _: &PoolId, _: &NodeId, cancel: &CancellationToken) -> Outcome<()> {
        ready(cancel)
    }
    async fn set_disk_io(
        &self,
        _: &PoolId,
        _: &DiskId,
        _: IoState,
        cancel: &CancellationToken,
    ) -> Outcome<()> {
        ready(cancel)
    }
    async fn initialize_bg(
        &self,
        _: &PoolId,
        _: &VolumeId,
        _: &[BgEntry],
        cancel: &CancellationToken,
    ) -> Outcome<()> {
        ready(cancel)
    }
}
