use crate::ServiceId;
use thiserror::Error;

pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum RuntimeError {
    #[error("service {0:?} is not registered")]
    ServiceNotFound(ServiceId),
    #[error("service {0:?} is registered with a different request protocol")]
    WrongProtocol(ServiceId),
    #[error("service {0:?} is paused")]
    ServicePaused(ServiceId),
    #[error("service {0:?} is draining")]
    ServiceDraining(ServiceId),
    #[error("service {0:?} is stopping")]
    ServiceStopping(ServiceId),
    #[error("service channel for {0:?} is closed")]
    ChannelClosed(ServiceId),
    #[error("workflow was cancelled")]
    Cancelled,
    #[error("request was rejected: {0}")]
    Rejected(String),
    #[error("request was superseded: {0}")]
    Superseded(String),
    #[error("invalid domain state: {0}")]
    InvalidState(String),
    #[error("operation timed out: {0}")]
    Timeout(String),
    #[error("workflow panicked: {0}")]
    WorkflowPanicked(String),
    #[error("internal runtime error: {0}")]
    Internal(String),
}
