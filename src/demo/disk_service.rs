use std::sync::Arc;

use crate::{
    CancelReason, ConflictPolicy, RequestContext, Router, Service, ServiceTaskManager, TaskKey,
    TaskMeta,
};

use super::{
    DemoError, DiskId, DiskRequest, DiskResponse, DiskState, RebuildRequest, RebuildService,
    ServiceKind, metadata::DiskMetadata,
};

pub struct DiskService {
    metadata: DiskMetadata,
    router: Router<ServiceKind>,
    tasks: ServiceTaskManager<ServiceKind>,
}

impl DiskService {
    pub fn new(router: Router<ServiceKind>, disks: impl IntoIterator<Item = DiskId>) -> Self {
        Self {
            metadata: DiskMetadata::new(disks),
            router,
            tasks: ServiceTaskManager::new(ServiceKind::Disk),
        }
    }

    fn query(&self, disk: &DiskId) -> DiskResponse {
        DiskResponse::Snapshot(self.metadata.query(disk))
    }

    async fn offline_workflow(
        self: Arc<Self>,
        disk: DiskId,
        context: RequestContext,
    ) -> Result<DiskResponse, DemoError> {
        let task = self
            .create_new_task(
                &context,
                TaskMeta::new(
                    TaskKey::new(format!("disk/{disk}")),
                    format!("offline disk {disk}"),
                )
                .public(),
                ConflictPolicy::Reject,
            )
            .await?;

        self.metadata.set(&disk, DiskState::Offlining)?;
        let result = self
            .router
            .call::<RebuildService>(
                &ServiceKind::Rebuild,
                RebuildRequest::Start(disk.clone()),
                context.with_task(&task),
            )
            .await;

        match result {
            Ok(_) => {
                self.metadata.set(&disk, DiskState::Offline)?;
                Ok(DiskResponse::OfflineCompleted)
            }
            Err(error) => {
                self.metadata.set(&disk, DiskState::Online)?;
                Err(error.into())
            }
        }
    }

    async fn fault_workflow(
        self: Arc<Self>,
        disk: DiskId,
        context: RequestContext,
    ) -> Result<DiskResponse, DemoError> {
        let key = TaskKey::new(format!("disk/{disk}"));
        let _task = self
            .create_new_task(
                &context,
                TaskMeta::new(key.clone(), format!("fault disk {disk}")).public(),
                ConflictPolicy::Replace(CancelReason::Preempted { by: key }),
            )
            .await?;
        self.metadata.set(&disk, DiskState::Faulted)?;
        Ok(DiskResponse::Faulted)
    }
}

impl Service for DiskService {
    type Key = ServiceKind;
    type Request = DiskRequest;
    type Response = DiskResponse;
    type Error = DemoError;

    fn task_manager(&self) -> &ServiceTaskManager<ServiceKind> {
        &self.tasks
    }

    async fn handle(
        self: Arc<Self>,
        request: DiskRequest,
        context: RequestContext,
    ) -> Result<DiskResponse, DemoError> {
        match request {
            DiskRequest::Query(disk) => Ok(self.query(&disk)),
            DiskRequest::Offline(disk) => self.offline_workflow(disk, context).await,
            DiskRequest::Fault(disk) => self.fault_workflow(disk, context).await,
        }
    }
}
