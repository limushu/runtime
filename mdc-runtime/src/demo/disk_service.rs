use std::sync::Arc;

use crate::{
    CancelReason, HandleResult, RequestContext, Router, Service, TaskContext, TaskKey, TaskMeta,
    TaskSpec,
};

use super::{
    DemoError, DiskId, DiskRequest, DiskResponse, DiskState, RebuildRequest, RebuildService,
    ServiceKind, metadata::DiskMetadata,
};

pub struct DiskService {
    metadata: DiskMetadata,
    router: Router<ServiceKind>,
}

impl DiskService {
    pub fn new(router: Router<ServiceKind>, disks: impl IntoIterator<Item = DiskId>) -> Self {
        Self {
            metadata: DiskMetadata::new(disks),
            router,
        }
    }

    async fn offline_workflow(
        self: Arc<Self>,
        disk: DiskId,
        task: TaskContext,
    ) -> Result<DiskResponse, DemoError> {
        self.metadata.set(&disk, DiskState::Offlining)?;
        let rebuild = self
            .router
            .client::<RebuildService>(&ServiceKind::Rebuild)?;
        let result = task
            .call(&rebuild, RebuildRequest::Start(disk.clone()))
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
        _task: TaskContext,
    ) -> Result<DiskResponse, DemoError> {
        self.metadata.set(&disk, DiskState::Faulted)?;
        Ok(DiskResponse::Faulted)
    }
}

impl Service for DiskService {
    type Request = DiskRequest;
    type Response = DiskResponse;
    type Error = DemoError;

    fn handle(
        self: Arc<Self>,
        request: DiskRequest,
        _context: RequestContext,
    ) -> HandleResult<DiskResponse, DemoError> {
        match request {
            DiskRequest::Query(disk) => {
                HandleResult::ok(DiskResponse::Snapshot(self.metadata.query(&disk)))
            }
            DiskRequest::Offline(disk) => {
                let meta = TaskMeta::new(
                    TaskKey::new(format!("disk/{disk}")),
                    format!("offline disk {disk}"),
                )
                .public();
                HandleResult::task(TaskSpec::new(meta, move |task| {
                    self.offline_workflow(disk, task)
                }))
            }
            DiskRequest::Fault(disk) => {
                let key = TaskKey::new(format!("disk/{disk}"));
                let meta = TaskMeta::new(key.clone(), format!("fault disk {disk}")).public();
                HandleResult::task(
                    TaskSpec::new(meta, move |task| self.fault_workflow(disk, task))
                        .replace_running(CancelReason::Preempted { by: key }),
                )
            }
        }
    }
}
