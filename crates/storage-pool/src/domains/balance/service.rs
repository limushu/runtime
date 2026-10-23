use super::*;
use crate::{DiskId, Outcome, PoolId, ports::*};
use std::sync::Arc;

pub(crate) struct BalanceService {
    pub pool: PoolId,
    pub port: Arc<dyn Balancer>,
}
impl BalanceService {
    pub async fn check(&self, disk: &DiskId) -> Outcome<ShrinkReadiness> {
        self.port.check_shrink(&self.pool, disk).await
    }
    pub async fn open(&self, disk: &DiskId, id: EvacuationId) -> Outcome<ArcJob> {
        self.port.evacuate(&self.pool, disk, id).await
    }
    pub async fn find(&self, id: EvacuationId) -> Outcome<Option<ArcJob>> {
        self.port.find(&self.pool, id).await
    }
}

pub(crate) enum BalanceCommand {
    Evacuate { disk: DiskId, id: EvacuationId },
    Cancel { disk: DiskId, id: EvacuationId },
}
