//! Composition of shrink capabilities; workflows depend only on typed clients.
use crate::{
    PoolId,
    domains::{balance::*, shrink::*},
};
use std::sync::Arc;
use workflow_foundation::{Outcome, WorkflowDriver};

#[derive(Clone)]
pub struct ShrinkServices {
    pub(crate) pool: PoolId,
    pub(crate) storage: ShrinkStorageClient,
    pub(crate) balance: BalanceClient,
    pub(crate) conditions: Arc<dyn ShrinkStorage>,
}
impl ShrinkServices {
    /// Bind normal domain services. Durable hosts also call define::<ShrinkWorkflow>
    /// with these dependencies before restore(), just like any other durable workflow.
    pub fn register(
        driver: &mut WorkflowDriver,
        pool: PoolId,
        storage: Arc<dyn ShrinkStorage>,
        balance: Arc<dyn BalanceBackend>,
    ) -> Outcome<Self> {
        Ok(Self {
            pool,
            storage: driver.register("shrink-storage", ShrinkStorageHost(storage.clone()))?,
            balance: driver.register("balance", BalanceHost(balance))?,
            conditions: storage,
        })
    }
}
