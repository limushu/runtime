use std::{future::Future, pin::Pin, sync::Arc};

use crate::{
    CancelReason, OperationId, TaskId, TaskKey, TraceContext, cancellation::CancellationScope,
};

pub type TaskFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

pub(crate) type TaskFactory<T, E> =
    Box<dyn FnOnce(TaskContext) -> TaskFuture<T, E> + Send + 'static>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskVisibility {
    Public,
    Internal,
}

#[derive(Clone, Debug)]
pub struct TaskMeta {
    pub key: TaskKey,
    pub label: Arc<str>,
    pub visibility: TaskVisibility,
}

impl TaskMeta {
    pub fn new(key: TaskKey, label: impl Into<Arc<str>>) -> Self {
        Self {
            key,
            label: label.into(),
            visibility: TaskVisibility::Internal,
        }
    }

    pub fn public(mut self) -> Self {
        self.visibility = TaskVisibility::Public;
        self
    }
}

#[derive(Clone, Debug)]
pub enum ConflictPolicy {
    Reject,
    Replace(CancelReason),
}

pub struct TaskSpec<T, E> {
    pub meta: TaskMeta,
    pub conflict: ConflictPolicy,
    factory: TaskFactory<T, E>,
}

impl<T, E> TaskSpec<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    pub fn new<F, Fut>(meta: TaskMeta, factory: F) -> Self
    where
        F: FnOnce(TaskContext) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
    {
        Self {
            meta,
            conflict: ConflictPolicy::Reject,
            factory: Box::new(move |context| Box::pin(factory(context))),
        }
    }

    pub fn replace_running(mut self, reason: CancelReason) -> Self {
        self.conflict = ConflictPolicy::Replace(reason);
        self
    }

    pub(crate) fn into_parts(self) -> (TaskMeta, ConflictPolicy, TaskFactory<T, E>) {
        (self.meta, self.conflict, self.factory)
    }
}

#[derive(Clone)]
pub struct RequestContext {
    pub operation_id: OperationId,
    pub trace: TraceContext,
    pub(crate) cancellation: CancellationScope,
}

impl RequestContext {
    pub fn root(operation_id: OperationId, trace: TraceContext) -> Self {
        Self {
            operation_id,
            trace,
            cancellation: CancellationScope::root(),
        }
    }

    pub(crate) fn child(&self, parent_task: TaskId) -> Self {
        Self {
            operation_id: self.operation_id,
            trace: self.trace.child(parent_task),
            cancellation: self.cancellation.child(),
        }
    }
}

#[derive(Clone)]
pub struct TaskContext {
    task_id: TaskId,
    key: TaskKey,
    request: RequestContext,
}

impl TaskContext {
    pub(crate) fn new(task_id: TaskId, key: TaskKey, request: RequestContext) -> Self {
        Self {
            task_id,
            key,
            request,
        }
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn key(&self) -> &TaskKey {
        &self.key
    }

    pub fn operation_id(&self) -> OperationId {
        self.request.operation_id
    }

    pub fn trace(&self) -> TraceContext {
        self.request.trace.child(self.task_id)
    }

    pub fn cancellation_reason(&self) -> Option<CancelReason> {
        self.request.cancellation.reason()
    }

    pub fn is_cancelled(&self) -> bool {
        self.request.cancellation.is_cancelled()
    }

    pub async fn cancelled(&self) -> CancelReason {
        self.request.cancellation.cancelled().await
    }

    pub(crate) fn child_request(&self) -> RequestContext {
        self.request.child(self.task_id)
    }
}
