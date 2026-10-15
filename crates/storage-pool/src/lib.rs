//! Storage-pool application, organized by business capability.
pub mod adapters;
pub mod domains;
mod pool;
pub mod ports;
mod requests;
mod support;
pub mod task_center;
pub mod workflows;

pub use domains::{PoolId, TierId, disk::*, node::*, vd::*};
pub use pool::{Pool, PoolHandle, PoolManager};
pub use requests::{CreateVolume, NodeTransitionRequest};
pub use task_center::{TaskObserver, TaskSnapshot};
pub use workflow_foundation::{Config, Failure, Outcome, Run, WorkflowId};
