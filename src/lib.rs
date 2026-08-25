//! One Tokio task per service, many explicitly managed workflow futures.
//!
//! A service handles a request in one of two ways: reply immediately, or return
//! a [`TaskSpec`] whose future is polled by the service executor.

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
    CallError, CancelReason, OperationId, RuntimeError, TaskExit, TaskId, TaskKey, TraceContext,
};
pub use router::Router;
pub use service::{
    ControlHandle, HandleResult, Service, ServiceClient, ServiceGroup, ServiceKey, ServiceRef,
    ShutdownMode, Submission, TaskTicket,
};
pub use task::{
    ConflictPolicy, RequestContext, TaskContext, TaskFuture, TaskMeta, TaskSpec, TaskVisibility,
};
