use std::sync::Arc;

use crate::{
    HandleResult, RequestContext, Router, Service, TaskContext, TaskKey, TaskMeta, TaskSpec,
};

use super::{
    BgRequest, BgService, DemoError, DiskId, RebuildRequest, RebuildResponse, ServiceKind,
    metadata::ActiveDisks,
};

pub struct RebuildService {
    metadata: ActiveDisks,
    router: Router<ServiceKind>,
}

impl RebuildService {
    pub fn new(router: Router<ServiceKind>) -> Self {
        Self {
            metadata: ActiveDisks::new(),
            router,
        }
    }

    async fn rebuild_workflow(
        self: Arc<Self>,
        disk: DiskId,
        task: TaskContext,
    ) -> Result<RebuildResponse, DemoError> {
        self.metadata.begin(disk.clone());

        let bg = self.router.client::<BgService>(&ServiceKind::Bg)?;
        let result = task.call(&bg, BgRequest::Rebuild(disk.clone())).await;

        self.metadata.finish(&disk);
        result?;
        Ok(RebuildResponse::Completed)
    }
}

impl Service for RebuildService {
    type Request = RebuildRequest;
    type Response = RebuildResponse;
    type Error = DemoError;

    fn handle(
        self: Arc<Self>,
        request: RebuildRequest,
        _context: RequestContext,
    ) -> HandleResult<RebuildResponse, DemoError> {
        match request {
            RebuildRequest::Start(disk) => {
                let meta = TaskMeta::new(
                    TaskKey::new(format!("rebuild/{disk}")),
                    format!("rebuild disk {disk}"),
                );
                HandleResult::task(TaskSpec::new(meta, move |task| {
                    self.rebuild_workflow(disk, task)
                }))
            }
        }
    }
}
