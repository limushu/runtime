use std::sync::Arc;

use crate::{HandleResult, RequestContext, Service, TaskContext, TaskKey, TaskMeta, TaskSpec};

use super::{BgBackend, BgRequest, BgResponse, DemoError, DiskId, metadata::ActiveDisks};

pub struct BgService {
    metadata: ActiveDisks,
    backend: BgBackend,
}

impl BgService {
    pub fn new(backend: BgBackend) -> Self {
        Self {
            metadata: ActiveDisks::new(),
            backend,
        }
    }

    async fn rebuild_workflow(
        self: Arc<Self>,
        disk: DiskId,
        task: TaskContext,
    ) -> Result<BgResponse, DemoError> {
        self.metadata.begin(disk.clone());
        let result = self.backend.rebuild(&task, disk.clone()).await;
        self.metadata.finish(&disk);
        result?;
        Ok(BgResponse::Completed)
    }
}

impl Service for BgService {
    type Request = BgRequest;
    type Response = BgResponse;
    type Error = DemoError;

    fn handle(
        self: Arc<Self>,
        request: BgRequest,
        _context: RequestContext,
    ) -> HandleResult<BgResponse, DemoError> {
        match request {
            BgRequest::Rebuild(disk) => {
                let meta = TaskMeta::new(
                    TaskKey::new(format!("bg/{disk}")),
                    format!("rebuild BGs for {disk}"),
                );
                HandleResult::task(TaskSpec::new(meta, move |task| {
                    self.rebuild_workflow(disk, task)
                }))
            }
        }
    }
}
