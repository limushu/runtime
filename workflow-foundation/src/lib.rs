//! Pool-scoped workflow coordination, without storage-domain dependencies.
mod coordinator;
mod locks;
mod model;
mod service;
mod workflow;

pub use coordinator::{Coordinator, CoordinatorClient};
pub use model::*;
pub use service::{ActivityContext, DomainService, ServiceClient};
pub use workflow::{Run, RunControl, Workflow, WorkflowContext};
