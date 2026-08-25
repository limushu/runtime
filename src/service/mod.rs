mod client;
mod control;
mod group;

use std::{fmt::Debug, hash::Hash, sync::Arc};

use crate::{RequestContext, ServiceActivity, TaskSpec};

pub use client::{ServiceClient, Submission, TaskTicket};
pub use control::{ControlHandle, ShutdownMode};
pub use group::{ServiceGroup, ServiceRef};

pub(crate) use client::{RequestEnvelope, client};
pub(crate) use control::{RuntimeControl, control};
pub(crate) use group::{ServiceTaskGuard, SpawnedService, lifecycle_snapshot};

pub trait ServiceKey: Clone + Debug + Eq + Hash + Send + Sync + 'static {}

impl<T> ServiceKey for T where T: Clone + Debug + Eq + Hash + Send + Sync + 'static {}

pub enum HandleResult<T, E> {
    Reply(Result<T, E>),
    Task(TaskSpec<T, E>),
}

impl<T, E> HandleResult<T, E> {
    pub fn ok(value: T) -> Self {
        Self::Reply(Ok(value))
    }

    pub fn error(error: E) -> Self {
        Self::Reply(Err(error))
    }

    pub fn task(task: TaskSpec<T, E>) -> Self {
        Self::Task(task)
    }
}

pub trait Service: Send + Sync + 'static {
    type Request: Send + 'static;
    type Response: Send + 'static;
    type Error: Send + 'static;

    /// Routes a protocol request to the corresponding service member method.
    /// The member method owns the Reply/Task decision and task policy.
    fn handle(
        self: Arc<Self>,
        request: Self::Request,
        context: RequestContext,
    ) -> HandleResult<Self::Response, Self::Error>;

    fn on_activity(&self, _activity: ServiceActivity) {}

    fn on_shutdown(&self) {}
}
