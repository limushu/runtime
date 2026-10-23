//! Business-free execution mechanics; applications can drive workflows in their own event loop.
mod audit;
mod control;
mod driver;
mod locks;
mod model;
mod service;
mod standalone;
mod tasks;
mod workflow;

pub use audit::{AuditEvent, AuditRecord, AuditSink, CheckpointWriter, ExecutionOptions};
pub use control::{
    ActivityPause, CancellationReason, ContinuationGuard, ContinueDecision, ControlRequest,
    ControlToken,
};
pub use driver::{ExecutionView, StartPermit, WorkflowDriver};
pub use model::*;
pub use service::{ActivityClient, ActivityContext, ServiceClient, ServiceHandler};
pub use standalone::{RuntimeHandle, WorkflowRuntime};
pub use tasks::{
    ActivityHistory, ActivitySnapshot, ActivityState, TaskObserver, TaskProgress, TaskSnapshot,
};
pub use workflow::{Run, RunControl, Workflow, WorkflowContext};
