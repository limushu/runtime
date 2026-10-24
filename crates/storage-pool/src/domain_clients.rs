//! Pool composition: one binding of the existing domain services for every workflow.
use crate::{
    domains::{
        balance::{BalanceBackend, BalanceClient, BalanceHost},
        disk::{
            runtime::DiskHost,
            service::{DiskCommand, DiskReply, DiskService},
        },
        node::{
            runtime::NodeHost,
            service::{NodeCommand, NodeService},
        },
        tier::{TierCommand, TierReply, TierService, runtime::TierHost},
        vd::{
            runtime::VdHost,
            service::{VdCommand, VdReply, VdService},
        },
    },
    ports::*,
    *,
};
use std::sync::Arc;
use workflow_foundation::{ActivityClient, WorkflowDriver};

#[derive(Clone)]
pub struct Domains {
    pub(crate) pool: PoolId,
    pub(crate) tier: ActivityClient<TierCommand, TierReply, Option<TierId>, Vec<Tier>>,
    pub(crate) disk: ActivityClient<DiskCommand, DiskReply, Option<DiskId>, Vec<MemberDisk>>,
    pub(crate) node: ActivityClient<NodeCommand, PoolNode, Option<NodeId>, Vec<PoolNode>>,
    pub(crate) vd: ActivityClient<VdCommand, VdReply, Option<VolumeId>, Vec<VirtualDisk>>,
    pub(crate) balance: Option<BalanceClient>,
    pub(crate) disk_state: Arc<DiskService>,
    pub(crate) vd_state: Arc<VdService>,
}
impl Domains {
    /// Register once per Pool. A standalone durable host also supplies this same
    /// binding to WorkflowDriver::define before restoring tasks.
    pub fn register(
        driver: &mut WorkflowDriver,
        pool: PoolId,
        metadata: Arc<dyn MetadataStore>,
        data_plane: Arc<dyn DataPlane>,
        balance: Option<Arc<dyn BalanceBackend>>,
    ) -> Outcome<Self> {
        let disk_state = Arc::new(DiskService::new(
            pool.clone(),
            metadata.clone(),
            data_plane.clone(),
        ));
        let vd_state = Arc::new(VdService::new(
            pool.clone(),
            metadata.clone(),
            data_plane.clone(),
        ));
        Ok(Self {
            tier: driver.register("tier", TierHost(TierService::new(pool.clone(), metadata)))?,
            disk: driver.register("disk", DiskHost(disk_state.clone()))?,
            node: driver.register("node", NodeHost(NodeService::new(pool.clone(), data_plane)))?,
            vd: driver.register("vd", VdHost(vd_state.clone()))?,
            balance: balance
                .map(|backend| driver.register("balance", BalanceHost(backend)))
                .transpose()?,
            pool,
            disk_state,
            vd_state,
        })
    }
    pub(crate) async fn disk(&self, id: &DiskId) -> Outcome<MemberDisk> {
        self.disks()
            .await?
            .into_iter()
            .find(|d| &d.id == id)
            .ok_or_else(|| crate::support::reject("unknown disk"))
    }
    pub(crate) async fn volume(&self, id: &VolumeId) -> Outcome<VirtualDisk> {
        self.vd_state.read_one(id).await
    }
    pub async fn disks(&self) -> Outcome<Vec<MemberDisk>> {
        self.disk_state.read().await
    }
    pub async fn volumes(&self) -> Outcome<Vec<VirtualDisk>> {
        self.vd_state.read().await
    }
}
