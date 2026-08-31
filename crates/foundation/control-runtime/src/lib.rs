//! Business-agnostic in-process control-plane runtime.

pub mod client;
pub mod context;
pub mod error;
pub mod observation;
pub mod protocol;
pub mod service;
pub mod state_cell;
pub mod state_machine;

pub use client::ServiceClient;
pub use context::{CancelCause, CancellationScope, WorkflowContext};
pub use error::{RuntimeError, RuntimeResult};
pub use observation::{
    Activity, CallOutcome, ObservationEvent, ServiceLifecycle, ServiceObserver, ServiceSnapshot,
    TaskOutcome,
};
pub use protocol::{CallId, ObjectKey, OperationId, ServiceId, ServiceRequest, TaskAttemptId};
pub use service::{
    spawn_service, ControlHandle, ManagedService, OrphanPolicy, RequestPlan, RuntimeConfig,
    Service, WorkflowMeta,
};
pub use state_cell::StateCell;
pub use state_machine::{Transition, TransitionEffect};
