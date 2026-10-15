//! Storage application built on the business-free workflow foundation.
pub mod app;
pub mod domain;
pub mod infrastructure;

pub use app::{CreateVolume, NodeTransitionRequest, Pool, PoolHandle, PoolManager};
pub use domain::{PoolId, TierId, disk::*, node::*, vd::*};
pub use workflow_foundation::{
    Config, Failure, Outcome, Run, TaskObserver, TaskSnapshot, WorkflowId,
};

// Preserve the existing public module paths for callers.
pub use app::{coordination, workflows};
pub use domain::ports;
pub use infrastructure::adapters;
