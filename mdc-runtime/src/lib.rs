//! One Tokio task per service, polling many request handler futures.
//!
//! Services own their task managers. A handler may create a managed task scope,
//! while the executor remains responsible only for polling handler futures.

mod cancellation;
mod executor;
mod observation;
mod protocol;
mod router;
mod service;
mod task;

pub mod demo;

pub use observation::{
    ServiceActivity, ServiceLifecycle, ServiceObserver, ServiceSnapshot, TaskEvent, TaskSnapshot,
    TaskState,
};
pub use protocol::{
    CallError, CancelReason, OperationId, RequestId, RuntimeError, TaskExit, TaskId, TaskKey,
    TraceContext,
};
pub use router::Router;
pub use service::{
    ControlHandle, Service, ServiceClient, ServiceGroup, ServiceKey, ServiceRef, ShutdownMode,
    Submission, TaskTicket,
};
pub use task::{
    ConflictPolicy, RequestContext, ServiceTaskManager, TaskContext, TaskMeta, TaskRef,
    TaskVisibility,
};
