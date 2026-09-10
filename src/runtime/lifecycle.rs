use super::{ServiceObserver, TaskId};
use std::fmt;
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceLifecycle {
    Initializing,
    Running,
    Paused,
    Draining,
    Stopping,
    Stopped,
    Failed,
}

impl ServiceLifecycle {
    pub const fn accepts_requests(self) -> bool {
        matches!(self, Self::Running)
    }

    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceActivity {
    Idle,
    Busy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceUnavailable {
    Initializing,
    Paused,
    Draining,
    Stopping,
    Stopped,
    Failed,
}

impl From<ServiceLifecycle> for ServiceUnavailable {
    fn from(value: ServiceLifecycle) -> Self {
        match value {
            ServiceLifecycle::Initializing => Self::Initializing,
            ServiceLifecycle::Paused => Self::Paused,
            ServiceLifecycle::Draining => Self::Draining,
            ServiceLifecycle::Stopping => Self::Stopping,
            ServiceLifecycle::Stopped => Self::Stopped,
            ServiceLifecycle::Failed => Self::Failed,
            ServiceLifecycle::Running => {
                unreachable!("a running service is available")
            }
        }
    }
}

impl fmt::Display for ServiceUnavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "service is {self:?}")
    }
}

impl std::error::Error for ServiceUnavailable {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlError {
    ServiceStopped,
    InvalidTransition {
        from: ServiceLifecycle,
        command: &'static str,
    },
    TaskNotFound(TaskId),
    TaskNotCancellable(TaskId),
}

impl fmt::Display for ControlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServiceStopped => f.write_str("service control channel is closed"),
            Self::InvalidTransition { from, command } => {
                write!(f, "cannot {command} a service in {from:?}")
            }
            Self::TaskNotFound(task) => write!(f, "task {task} is not active"),
            Self::TaskNotCancellable(task) => write!(f, "task {task} is not cancellable"),
        }
    }
}

impl std::error::Error for ControlError {}

pub(crate) enum ControlCommand {
    Pause(oneshot::Sender<Result<(), ControlError>>),
    Resume(oneshot::Sender<Result<(), ControlError>>),
    Drain(oneshot::Sender<Result<(), ControlError>>),
    Stop(oneshot::Sender<Result<(), ControlError>>),
    CancelTask {
        task: TaskId,
        cause: String,
        reply: oneshot::Sender<Result<(), ControlError>>,
    },
}

/// Out-of-band control path. Control messages never share capacity with
/// business traffic and are polled first by the service root task.
#[derive(Clone)]
pub struct ServiceControl {
    pub(crate) sender: mpsc::Sender<ControlCommand>,
    observer: ServiceObserver,
}

impl ServiceControl {
    pub(crate) fn new(sender: mpsc::Sender<ControlCommand>, observer: ServiceObserver) -> Self {
        Self { sender, observer }
    }

    pub fn observer(&self) -> ServiceObserver {
        self.observer.clone()
    }

    pub async fn pause(&self) -> Result<(), ControlError> {
        self.request(ControlCommand::Pause).await
    }

    pub async fn resume(&self) -> Result<(), ControlError> {
        self.request(ControlCommand::Resume).await
    }

    /// Rejects new work and returns only after all accepted work and the
    /// service shutdown hook have settled.
    pub async fn drain(&self) -> Result<(), ControlError> {
        self.request(ControlCommand::Drain).await
    }

    /// Requests cooperative cancellation of active work, then waits for every
    /// Future to settle and for the shutdown hook to finish.
    pub async fn stop(&self) -> Result<(), ControlError> {
        self.request(ControlCommand::Stop).await
    }

    pub async fn cancel_task(
        &self,
        task: TaskId,
        cause: impl Into<String>,
    ) -> Result<(), ControlError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(ControlCommand::CancelTask {
                task,
                cause: cause.into(),
                reply,
            })
            .await
            .map_err(|_| ControlError::ServiceStopped)?;
        response.await.map_err(|_| ControlError::ServiceStopped)?
    }

    async fn request<F>(&self, make: F) -> Result<(), ControlError>
    where
        F: FnOnce(oneshot::Sender<Result<(), ControlError>>) -> ControlCommand,
    {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(make(reply))
            .await
            .map_err(|_| ControlError::ServiceStopped)?;
        response.await.map_err(|_| ControlError::ServiceStopped)?
    }
}
