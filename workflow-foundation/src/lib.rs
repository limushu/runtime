//! Pool-scoped workflow coordination, without storage-domain dependencies.
mod admission;
mod coordinator;
mod locks;
mod model;
mod service;
mod workflow;

pub use admission::{AdmissionDecision, AdmissionPolicy};
pub use coordinator::{Coordinator, CoordinatorClient};
pub use model::*;
pub use service::{ActivityContext, DomainService, ServiceClient};
pub use workflow::{Run, RunControl, Workflow, WorkflowContext};
