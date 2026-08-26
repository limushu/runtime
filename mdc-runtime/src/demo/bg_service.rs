use std::sync::Arc;

use crate::{ConflictPolicy, RequestContext, Service, ServiceTaskManager, TaskKey, TaskMeta};

use super::{
    BgBackend, BgRequest, BgResponse, DemoError, DiskId, ServiceKind, metadata::ActiveDisks,
};

pub struct BgService {
    metadata: ActiveDisks,
    backend: BgBackend,
    tasks: ServiceTaskManager<ServiceKind>,
}

impl BgService {
    pub fn new(backend: BgBackend) -> Self {
        Self {
            metadata: ActiveDisks::new(),
            backend,
            tasks: ServiceTaskManager::new(ServiceKind::Bg),
        }
    }

    async fn rebuild_workflow(
        self: Arc<Self>,
        disk: DiskId,
        context: RequestContext,
    ) -> Result<BgResponse, DemoError> {
        let task = self
            .create_new_task(
                &context,
                TaskMeta::new(
                    TaskKey::new(format!("bg/{disk}")),
                    format!("rebuild BGs for {disk}"),
                ),
                ConflictPolicy::Reject,
            )
            .await?;

        self.metadata.begin(disk.clone());
        let result = self.backend.rebuild(&task, disk.clone()).await;
        self.metadata.finish(&disk);
        result?;
        Ok(BgResponse::Completed)
    }
}

impl Service for BgService {
    type Key = ServiceKind;
    type Request = BgRequest;
    type Response = BgResponse;
    type Error = DemoError;

    fn task_manager(&self) -> &ServiceTaskManager<ServiceKind> {
        &self.tasks
    }

    async fn handle(
        self: Arc<Self>,
        request: BgRequest,
        context: RequestContext,
    ) -> Result<BgResponse, DemoError> {
        match request {
            BgRequest::Rebuild(disk) => self.rebuild_workflow(disk, context).await,
        }
    }
}
