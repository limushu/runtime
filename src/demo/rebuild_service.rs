use std::sync::Arc;

use crate::{HandleResult, RequestContext, Router, Service, TaskKey, TaskMeta, TaskSpec};

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

    fn rebuild_workflow(self: Arc<Self>, disk: DiskId) -> HandleResult<RebuildResponse, DemoError> {
        let meta = TaskMeta::new(
            TaskKey::new(format!("rebuild/{disk}")),
            format!("rebuild disk {disk}"),
        );

        HandleResult::task(TaskSpec::new(meta, move |task| async move {
            self.metadata.begin(disk.clone());

            let bg = self.router.client::<BgService>(&ServiceKind::Bg)?;
            let result = task.call(&bg, BgRequest::Rebuild(disk.clone())).await;

            self.metadata.finish(&disk);
            result?;
            Ok(RebuildResponse::Completed)
        }))
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
            RebuildRequest::Start(disk) => self.rebuild_workflow(disk),
        }
    }
}
