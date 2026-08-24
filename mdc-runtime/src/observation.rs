use tokio::sync::{broadcast, watch};

use crate::{OperationId, ServiceKey, TaskId, TaskKey, TraceContext};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceLifecycle {
    Initializing,
    Running,
    Paused,
    Draining,
    Stopping,
    Stopped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceActivity {
    Idle,
    Busy,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceSnapshot<K>
where
    K: ServiceKey,
{
    pub service: K,
    pub lifecycle: ServiceLifecycle,
    pub activity: ServiceActivity,
    pub queued_messages: usize,
    pub running_tasks: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskVisibility {
    Public,
    Internal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskState {
    Started,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskEvent<K>
where
    K: ServiceKey,
{
    pub service: K,
    pub task_id: TaskId,
    pub key: TaskKey,
    pub label: String,
    pub state: TaskState,
    pub visibility: TaskVisibility,
    pub operation_id: OperationId,
    pub trace: TraceContext,
}

pub struct ServiceObserver<K>
where
    K: ServiceKey,
{
    status: watch::Receiver<ServiceSnapshot<K>>,
    tasks: broadcast::Sender<TaskEvent<K>>,
}

impl<K> ServiceObserver<K>
where
    K: ServiceKey,
{
    pub(crate) fn new(
        status: watch::Receiver<ServiceSnapshot<K>>,
        tasks: broadcast::Sender<TaskEvent<K>>,
    ) -> Self {
        Self { status, tasks }
    }

    pub fn snapshot(&self) -> ServiceSnapshot<K> {
        self.status.borrow().clone()
    }

    pub fn watch_status(&self) -> watch::Receiver<ServiceSnapshot<K>> {
        self.status.clone()
    }

    pub fn watch_tasks(&self) -> broadcast::Receiver<TaskEvent<K>> {
        self.tasks.subscribe()
    }
}

impl<K> Clone for ServiceObserver<K>
where
    K: ServiceKey,
{
    fn clone(&self) -> Self {
        Self {
            status: self.status.clone(),
            tasks: self.tasks.clone(),
        }
    }
}
