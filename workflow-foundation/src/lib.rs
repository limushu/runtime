//! Pool-scoped workflow coordination, without storage-domain dependencies.
mod admission;
mod coordinator;
mod locks;
mod model;
mod service;
mod tasks;
mod workflow;

pub use admission::{AdmissionDecision, AdmissionEvaluation, AdmissionPolicy};
pub use coordinator::{Coordinator, CoordinatorClient};
pub use model::*;
pub use service::{ActivityContext, DomainService, ServiceClient};
pub use tasks::{AdmissionMatch, TaskObserver, TaskProgress, TaskSnapshot};
pub use workflow::{Run, RunControl, Workflow, WorkflowContext};
