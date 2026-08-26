use std::sync::Arc;

use crate::{
    ConflictPolicy, RequestContext, Router, Service, ServiceTaskManager, TaskKey, TaskMeta,
};

use super::{
    BgRequest, BgService, DemoError, DiskId, RebuildRequest, RebuildResponse, ServiceKind,
    metadata::ActiveDisks,
};

pub struct RebuildService {
    metadata: ActiveDisks,
    router: Router<ServiceKind>,
    tasks: ServiceTaskManager<ServiceKind>,
}

impl RebuildService {
    pub fn new(router: Router<ServiceKind>) -> Self {
        Self {
            metadata: ActiveDisks::new(),
            router,
            tasks: ServiceTaskManager::new(ServiceKind::Rebuild),
        }
    }

    async fn rebuild_workflow(
        self: Arc<Self>,
        disk: DiskId,
        context: RequestContext,
    ) -> Result<RebuildResponse, DemoError> {
        let task = self
            .create_new_task(
                &context,
                TaskMeta::new(
                    TaskKey::new(format!("rebuild/{disk}")),
                    format!("rebuild disk {disk}"),
                ),
                ConflictPolicy::Reject,
            )
            .await?;

        self.metadata.begin(disk.clone());
        let result = self
            .router
            .call::<BgService>(
                &ServiceKind::Bg,
                BgRequest::Rebuild(disk.clone()),
                context.with_task(&task),
            )
            .await;
        self.metadata.finish(&disk);
        result?;
        Ok(RebuildResponse::Completed)
    }
}

impl Service for RebuildService {
    type Key = ServiceKind;
    type Request = RebuildRequest;
    type Response = RebuildResponse;
    type Error = DemoError;

    fn task_manager(&self) -> &ServiceTaskManager<ServiceKind> {
        &self.tasks
    }

    async fn handle(
        self: Arc<Self>,
        request: RebuildRequest,
        context: RequestContext,
    ) -> Result<RebuildResponse, DemoError> {
        match request {
            RebuildRequest::Start(disk) => self.rebuild_workflow(disk, context).await,
        }
    }
}
