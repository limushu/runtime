mod client;
mod control;
mod group;

use std::{fmt::Debug, future::Future, hash::Hash, sync::Arc};

use crate::{
    ConflictPolicy, RequestContext, RuntimeError, ServiceActivity, ServiceTaskManager, TaskContext,
    TaskMeta,
};

pub use client::{ServiceClient, Submission, TaskTicket};
pub use control::{ControlHandle, ShutdownMode};
pub use group::{ServiceGroup, ServiceRef};

pub(crate) use client::{Accepted, RequestEnvelope, client};
pub(crate) use control::{RuntimeControl, control};
pub(crate) use group::{ServiceTaskGuard, SpawnedService, lifecycle_snapshot};

pub trait ServiceKey: Clone + Debug + Eq + Hash + Send + Sync + 'static {}

impl<T> ServiceKey for T where T: Clone + Debug + Eq + Hash + Send + Sync + 'static {}

pub trait Service: Send + Sync + 'static {
    type Key: ServiceKey;
    type Request: Send + 'static;
    type Response: Send + 'static;
    type Error: Send + 'static;

    fn task_manager(&self) -> &ServiceTaskManager<Self::Key>;

    fn handle(
        self: Arc<Self>,
        request: Self::Request,
        context: RequestContext,
    ) -> impl Future<Output = Result<Self::Response, Self::Error>> + Send;

    fn create_new_task(
        &self,
        context: &RequestContext,
        meta: TaskMeta,
        conflict: ConflictPolicy,
    ) -> impl Future<Output = Result<TaskContext, RuntimeError>> + Send {
        self.task_manager().create_new_task(context, meta, conflict)
    }

    fn on_activity(&self, _activity: ServiceActivity) {}

    fn on_shutdown(&self) {}
}
