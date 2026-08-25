use std::sync::Arc;

use tokio::sync::{broadcast, watch};

use crate::{OperationId, TaskId, TaskKey, TaskVisibility, TraceContext};

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
pub struct ServiceSnapshot<K> {
    pub service: K,
    pub lifecycle: ServiceLifecycle,
    pub activity: ServiceActivity,
    pub queued_requests: usize,
    pub managed_tasks: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskState {
    Queued,
    Running,
    Cancelling,
    Completed,
    Failed,
    Cancelled,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskSnapshot<K> {
    pub service: K,
    pub task_id: TaskId,
    pub key: TaskKey,
    pub label: Arc<str>,
    pub state: TaskState,
    pub visibility: TaskVisibility,
    pub operation_id: OperationId,
    pub trace: TraceContext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskEvent<K> {
    pub task: TaskSnapshot<K>,
}

pub struct ServiceObserver<K> {
    status: watch::Receiver<ServiceSnapshot<K>>,
    tasks: watch::Receiver<Vec<TaskSnapshot<K>>>,
    events: broadcast::Sender<TaskEvent<K>>,
}

impl<K: Clone> ServiceObserver<K> {
    pub(crate) fn new(
        status: watch::Receiver<ServiceSnapshot<K>>,
        tasks: watch::Receiver<Vec<TaskSnapshot<K>>>,
        events: broadcast::Sender<TaskEvent<K>>,
    ) -> Self {
        Self {
            status,
            tasks,
            events,
        }
    }

    pub fn snapshot(&self) -> ServiceSnapshot<K> {
        self.status.borrow().clone()
    }

    pub fn task_snapshots(&self) -> Vec<TaskSnapshot<K>> {
        self.tasks.borrow().clone()
    }

    pub fn watch_status(&self) -> watch::Receiver<ServiceSnapshot<K>> {
        self.status.clone()
    }

    pub fn watch_tasks(&self) -> watch::Receiver<Vec<TaskSnapshot<K>>> {
        self.tasks.clone()
    }

    pub fn task_events(&self) -> broadcast::Receiver<TaskEvent<K>> {
        self.events.subscribe()
    }
}

impl<K: Clone> Clone for ServiceObserver<K> {
    fn clone(&self) -> Self {
        Self {
            status: self.status.clone(),
            tasks: self.tasks.clone(),
            events: self.events.clone(),
        }
    }
}
