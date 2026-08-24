use std::{fmt, sync::Arc};

use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OperationId(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TaskId(pub u64);

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TaskKey(Arc<str>);

impl TaskKey {
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TaskKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TraceContext {
    pub trace_id: u128,
    pub parent_task: Option<TaskId>,
}

impl TraceContext {
    pub const fn root(trace_id: u128) -> Self {
        Self {
            trace_id,
            parent_task: None,
        }
    }

    pub const fn child(self, parent_task: TaskId) -> Self {
        Self {
            trace_id: self.trace_id,
            parent_task: Some(parent_task),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MessageContext {
    pub operation_id: OperationId,
    pub trace: TraceContext,
}

impl MessageContext {
    pub const fn new(operation_id: OperationId, trace: TraceContext) -> Self {
        Self {
            operation_id,
            trace,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    ServiceNotFound(String),
    ServiceUnavailable(String),
    ChannelClosed(String),
    HandlerAlreadyInstalled(String),
    HandlerNotInstalled(String),
    WrongMessagePayload,
    TaskAlreadyRunning(TaskKey),
    TaskNotFound(TaskKey),
    TaskFailed(String),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for RuntimeError {}

pub type Reply<T> = oneshot::Sender<T>;
pub type Ticket<T> = oneshot::Receiver<T>;

pub fn request_channel<T>() -> (Reply<T>, Ticket<T>) {
    oneshot::channel()
}
