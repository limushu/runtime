//! Business-free execution mechanics; applications can drive workflows in their own event loop.
mod driver;
mod identity;
pub use identity::{TaskId, TaskIds};
mod locks;
mod model;
mod service;
mod standalone;
mod tasks;
mod workflow;

pub use driver::{ExecutionView, StartPermit, WorkflowDriver};
pub use model::*;
pub use service::{ActivityClient, ActivityContext, ServiceClient, ServiceHandler};
pub use standalone::{RuntimeHandle, WorkflowRuntime};
pub(crate) use tasks::TaskInfo;
pub use tasks::{
    ActivityHistory, ActivitySnapshot, ActivityState, Conflict, ConflictDecision, ConflictMatch,
    TaskObserver, TaskProgress, TaskSnapshot,
};
pub use workflow::{Run, RunControl, Workflow, WorkflowContext};

mod control;
pub use control::{
    ActivityControl, ActivityExit, CancellationReason, ControlRequest, ControlToken, Readiness,
    RunCondition,
};

pub mod persistence;
pub use persistence::{Dependencies, Persistence};

pub mod scheduling;
