use std::{sync::Arc, time::Duration};

use crate::{CallError, RuntimeError, ServiceGroup, ServiceRef, ShutdownMode};

use super::{
    BgBackend, BgService, DemoError, DiskId, DiskRequest, DiskResponse, DiskService,
    RebuildService, ServiceKind,
};

pub struct DemoPool {
    pub services: ServiceGroup<ServiceKind>,
    pub disk: ServiceRef<ServiceKind, DiskService>,
    pub rebuild: ServiceRef<ServiceKind, RebuildService>,
    pub bg: ServiceRef<ServiceKind, BgService>,
    pub backend: BgBackend,
}

impl DemoPool {
    pub async fn start(disks: impl IntoIterator<Item = DiskId>) -> Result<Self, RuntimeError> {
        Self::start_with_backend(
            disks,
            BgBackend::new(Duration::from_millis(20), Duration::from_millis(20)),
        )
        .await
    }

    pub async fn start_with_backend(
        disks: impl IntoIterator<Item = DiskId>,
        backend: BgBackend,
    ) -> Result<Self, RuntimeError> {
        let mut services = ServiceGroup::new();
        let router = services.router();
        let bg = services
            .spawn(
                ServiceKind::Bg,
                Arc::new(BgService::new(backend.clone())),
                64,
            )
            .await?;
        let rebuild = services
            .spawn(
                ServiceKind::Rebuild,
                Arc::new(RebuildService::new(router.clone())),
                64,
            )
            .await?;
        let disk = services
            .spawn(
                ServiceKind::Disk,
                Arc::new(DiskService::new(router, disks)),
                64,
            )
            .await?;
        Ok(Self {
            services,
            disk,
            rebuild,
            bg,
            backend,
        })
    }

    pub async fn query_disk(&self, disk: DiskId) -> Result<Option<super::DiskSnapshot>, DemoError> {
        match self.disk.client.call(DiskRequest::Query(disk)).await? {
            DiskResponse::Snapshot(snapshot) => Ok(snapshot),
            response => Err(DemoError::Runtime(format!(
                "unexpected disk response: {response:?}"
            ))),
        }
    }

    pub async fn offline_disk(&self, disk: DiskId) -> Result<DiskResponse, CallError<DemoError>> {
        self.disk.client.call(DiskRequest::Offline(disk)).await
    }

    pub async fn shutdown(self, mode: ShutdownMode) -> Result<(), RuntimeError> {
        self.services.shutdown_all(mode).await
    }
}
