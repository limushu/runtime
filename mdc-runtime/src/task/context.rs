use crate::{
    CancelReason, OperationId, RequestId, TaskId, TaskKey, TraceContext,
    cancellation::CancellationScope,
};

#[derive(Clone)]
pub struct TaskRef {
    task_id: TaskId,
    key: TaskKey,
    pub(super) cancellation: CancellationScope,
}

impl TaskRef {
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn key(&self) -> &TaskKey {
        &self.key
    }
}

#[derive(Clone)]
pub struct RequestContext {
    pub request_id: RequestId,
    pub operation_id: OperationId,
    pub trace: TraceContext,
    pub task: Option<TaskRef>,
}

impl RequestContext {
    pub fn root(request_id: RequestId, operation_id: OperationId, trace: TraceContext) -> Self {
        Self {
            request_id,
            operation_id,
            trace,
            task: None,
        }
    }

    pub fn with_task(&self, task: &TaskContext) -> Self {
        Self {
            request_id: self.request_id,
            operation_id: self.operation_id,
            trace: self.trace.child(task.task_id),
            task: Some(task.reference()),
        }
    }

    pub fn cancellation_reason(&self) -> Option<CancelReason> {
        self.task
            .as_ref()
            .and_then(|task| task.cancellation.reason())
    }

    pub async fn cancelled(&self) -> CancelReason {
        match &self.task {
            Some(task) => task.cancellation.cancelled().await,
            None => std::future::pending().await,
        }
    }

    pub(crate) fn for_request(mut self, request_id: RequestId) -> Self {
        self.request_id = request_id;
        self
    }
}

#[derive(Clone)]
pub struct TaskContext {
    task_id: TaskId,
    key: TaskKey,
    pub(super) cancellation: CancellationScope,
}

impl TaskContext {
    pub(super) fn new(task_id: TaskId, key: TaskKey, cancellation: CancellationScope) -> Self {
        Self {
            task_id,
            key,
            cancellation,
        }
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn key(&self) -> &TaskKey {
        &self.key
    }

    pub fn cancellation_reason(&self) -> Option<CancelReason> {
        self.cancellation.reason()
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancellation.is_cancelled()
    }

    pub async fn cancelled(&self) -> CancelReason {
        self.cancellation.cancelled().await
    }

    fn reference(&self) -> TaskRef {
        TaskRef {
            task_id: self.task_id,
            key: self.key.clone(),
            cancellation: self.cancellation.clone(),
        }
    }
}
