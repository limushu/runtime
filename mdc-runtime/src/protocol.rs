use std::{fmt, sync::Arc};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OperationId(pub u64);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct RequestId(pub u64);

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CancelReason {
    Requested(Arc<str>),
    Preempted { by: TaskKey },
    ParentCancelled,
    ServiceStopping,
}

impl CancelReason {
    pub fn requested(reason: impl Into<Arc<str>>) -> Self {
        Self::Requested(reason.into())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    ServiceNotFound(String),
    WrongServiceType(String),
    ServiceUnavailable(String),
    ChannelClosed(String),
    ResponseDropped,
    TaskAlreadyRunning(TaskKey),
    TaskCancelled(CancelReason),
    TaskNotFound(TaskId),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for RuntimeError {}

#[derive(Debug)]
pub enum TaskExit<T, E> {
    Completed(T),
    Failed(E),
    Cancelled(CancelReason),
    Aborted,
}

#[derive(Debug)]
pub enum CallError<E> {
    Service(E),
    Cancelled(CancelReason),
    Aborted,
    Runtime(RuntimeError),
}

impl<E: fmt::Debug> fmt::Display for CallError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl<E: fmt::Debug> std::error::Error for CallError<E> {}
