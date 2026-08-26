use std::{fmt, sync::Arc};

use crate::{CallError, RuntimeError};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ServiceKind {
    Disk,
    Rebuild,
    Bg,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DiskId(pub Arc<str>);

impl DiskId {
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }
}

impl fmt::Display for DiskId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiskState {
    Online,
    Offlining,
    Offline,
    Faulted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DiskSnapshot {
    pub disk: DiskId,
    pub state: DiskState,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DemoError {
    DiskNotFound(DiskId),
    Cancelled,
    Runtime(String),
}

impl fmt::Display for DemoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl std::error::Error for DemoError {}

impl From<RuntimeError> for DemoError {
    fn from(error: RuntimeError) -> Self {
        match error {
            RuntimeError::TaskCancelled(_) => Self::Cancelled,
            error => Self::Runtime(error.to_string()),
        }
    }
}

impl From<CallError<DemoError>> for DemoError {
    fn from(error: CallError<DemoError>) -> Self {
        match error {
            CallError::Service(error) => error,
            CallError::Cancelled(_) => Self::Cancelled,
            CallError::Aborted => Self::Runtime("child task aborted".into()),
            CallError::Runtime(error) => error.into(),
        }
    }
}

#[derive(Clone, Debug)]
pub enum DiskRequest {
    Offline(DiskId),
    Fault(DiskId),
    Query(DiskId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DiskResponse {
    OfflineCompleted,
    Faulted,
    Snapshot(Option<DiskSnapshot>),
}

#[derive(Clone, Debug)]
pub enum RebuildRequest {
    Start(DiskId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RebuildResponse {
    Completed,
}

#[derive(Clone, Debug)]
pub enum BgRequest {
    Rebuild(DiskId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BgResponse {
    Completed,
}
