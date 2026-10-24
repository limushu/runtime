//! Storage-pool application, organized by business capability.
pub mod adapters;
pub mod domains;
pub mod manager;
mod pool;
pub mod ports;
mod requests;
mod support;
pub mod task_center;
pub mod workflows;

pub use domains::{PoolId, TierId, disk::*, node::*, tier::Tier, vd::*};
pub use manager::*;
pub use pool::{Pool, PoolHandle};
pub use requests::{CreateVolume, NodeTransitionRequest};
pub use task_center::{TaskObserver, TaskSnapshot};
pub use workflow_foundation::{Config, Failure, Outcome, Run, TaskId, TaskIds};

pub use workflows::{ShrinkProgress, ShrinkRequest, ShrinkWorkflow};

mod shrink;
pub use shrink::ShrinkServices;
