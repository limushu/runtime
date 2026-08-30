//! Business-agnostic in-process control-plane runtime.

pub mod context;
pub mod error;
pub mod observation;
pub mod protocol;
pub mod router;
pub mod service;
pub mod state_cell;

pub use context::{CancelCause, CancellationScope, WorkflowContext};
pub use error::{RuntimeError, RuntimeResult};
pub use observation::{
    Activity, CallOutcome, ObservationEvent, ServiceLifecycle, ServiceObserver, ServiceSnapshot,
    TaskOutcome,
};
pub use protocol::{CallId, ObjectKey, OperationId, ServiceId, ServiceRequest, TaskAttemptId};
pub use router::Router;
pub use service::{
    spawn_service, ControlHandle, ExecutionClass, Footprint, ManagedService, ObjectActivity,
    ObjectDecision, RuntimeConfig, Service, WorkflowMeta,
};
pub use state_cell::StateCell;
