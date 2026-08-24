use std::time::Duration;

use crate::{
    HandlerRegistry, MessageContext, OperationId, PoolServices, RuntimeError, ServiceRef,
    TraceContext,
};

use super::state::{BgState, EventState, RebuildState};
use super::{
    BgBackend, BgSnapshot, DemoCatalog, DemoMessage, DiskId, DiskOfflineRequest, QueryBg,
    QueryRebuild, RebuildResult, RebuildSnapshot, ServiceKind, bg_rebuild, disk_offline, rebuild,
};

pub struct DemoBlueprint {
    pub(crate) event: HandlerRegistry<ServiceKind, DemoMessage, EventState>,
    pub(crate) rebuild: HandlerRegistry<ServiceKind, DemoMessage, RebuildState>,
    pub(crate) bg: HandlerRegistry<ServiceKind, DemoMessage, BgState>,
}

impl DemoBlueprint {
    pub fn install() -> Result<Self, RuntimeError> {
        let mut blueprint = Self {
            event: HandlerRegistry::new(),
            rebuild: HandlerRegistry::new(),
            bg: HandlerRegistry::new(),
        };
        disk_offline::install::install(&mut blueprint)?;
        rebuild::install::install(&mut blueprint)?;
        bg_rebuild::install::install(&mut blueprint)?;
        Ok(blueprint)
    }

    pub async fn spawn(
        self,
        catalog: DemoCatalog,
        backend: BgBackend,
        window: usize,
    ) -> Result<DemoSystem, RuntimeError> {
        let mut services = PoolServices::new();
        let event = services
            .spawn(
                ServiceKind::Event,
                EventState {
                    activity_changes: 0,
                },
                self.event,
                64,
            )
            .await?;
        let rebuild = services
            .spawn(
                ServiceKind::Rebuild,
                RebuildState {
                    catalog,
                    window,
                    suspended: false,
                    disks: Default::default(),
                    bgs: Default::default(),
                    queue: Default::default(),
                    running: 0,
                    suspend_reply: None,
                    campaign_stop: None,
                    campaigns_started: 0,
                    bg_delegations_started: 0,
                    activity_changes: 0,
                },
                self.rebuild,
                64,
            )
            .await?;
        let bg = services
            .spawn(
                ServiceKind::Bg,
                BgState {
                    backend,
                    activity_changes: 0,
                },
                self.bg,
                64,
            )
            .await?;
        Ok(DemoSystem {
            services,
            event,
            rebuild,
            bg,
        })
    }
}

pub struct DemoSystem {
    pub services: PoolServices<ServiceKind, DemoMessage>,
    pub event: ServiceRef<ServiceKind, DemoMessage>,
    pub rebuild: ServiceRef<ServiceKind, DemoMessage>,
    pub bg: ServiceRef<ServiceKind, DemoMessage>,
}

impl DemoSystem {
    pub async fn disk_offline(
        &self,
        disk: DiskId,
        operation: u64,
    ) -> Result<RebuildResult, RuntimeError> {
        let (completed, ticket) = crate::request_channel();
        self.services
            .router()
            .send_payload(
                ServiceKind::Event,
                DiskOfflineRequest { disk, completed },
                MessageContext::new(
                    OperationId(operation),
                    TraceContext::root(operation as u128),
                ),
            )
            .await?;
        ticket
            .await
            .map_err(|_| RuntimeError::ChannelClosed("disk rebuild ticket".into()))
    }

    pub async fn rebuild_snapshot(&self, operation: u64) -> Result<RebuildSnapshot, RuntimeError> {
        let (completed, ticket) = crate::request_channel();
        self.services
            .router()
            .send_payload(
                ServiceKind::Rebuild,
                QueryRebuild { completed },
                demo_context(operation),
            )
            .await?;
        ticket
            .await
            .map_err(|_| RuntimeError::ChannelClosed("rebuild snapshot".into()))
    }

    pub async fn bg_snapshot(&self, operation: u64) -> Result<BgSnapshot, RuntimeError> {
        let (completed, ticket) = crate::request_channel();
        self.services
            .router()
            .send_payload(
                ServiceKind::Bg,
                QueryBg { completed },
                demo_context(operation),
            )
            .await?;
        ticket
            .await
            .map_err(|_| RuntimeError::ChannelClosed("bg snapshot".into()))
    }
}

pub fn demo_context(operation: u64) -> MessageContext {
    MessageContext::new(
        OperationId(operation),
        TraceContext::root(operation as u128),
    )
}

pub fn default_demo_catalog() -> DemoCatalog {
    DemoCatalog::new(
        [
            (
                DiskId::new("disk-1"),
                vec![super::BgId::new("bg-a"), super::BgId::new("bg-shared")],
            ),
            (
                DiskId::new("disk-2"),
                vec![super::BgId::new("bg-shared"), super::BgId::new("bg-b")],
            ),
        ],
        Duration::from_millis(5),
    )
}
