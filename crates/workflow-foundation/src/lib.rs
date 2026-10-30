//! Business-free execution mechanics; applications can drive workflows in their own event loop.
mod activity;
mod driver;
mod identity;
pub use identity::{TaskId, TaskIds};
mod locks;
mod model;
mod service;
mod standalone;
mod tasks;
mod workflow;

pub use activity::{Activity, ActivityAction, ActivityScope, ActivityWait, Backoff, RetryPolicy};
pub use driver::execution::Run;
pub use driver::recovery::Restored;
pub use driver::{ExecutionView, StartPermit, WorkflowDriver};
pub use model::*;
pub use service::{ActivityClient, ActivityContext, ActivityExit, ServiceClient, ServiceHandler};
pub use standalone::{RuntimeHandle, WorkflowRuntime};
pub(crate) use tasks::TaskInfo;
pub use tasks::{
    ActivityHistory, ActivityKind, ActivitySnapshot, ActivityState, TaskObserver, TaskProgress,
    TaskSnapshot,
};
pub use workflow::{
    ActivityCall, Dependencies, Readiness, RunCondition, Workflow, WorkflowContext,
};

mod control;
pub use control::{CancellationReason, ControlRequest, ControlToken, RunControl};

pub mod persistence;
pub use persistence::Persistence;

pub mod scheduling;
pub use scheduling::{Conflict, ConflictDecision, ConflictMatch};
