//! Storage control-plane model built on the business-free workflow foundation.
pub mod adapters;
pub mod coordination;
mod disk;
mod node;
mod pool;
pub mod ports;
mod types;
mod vd;
pub mod workflows;

pub use pool::{Pool, PoolHandle, PoolManager};
pub use types::*;
pub use workflow_foundation::{Config, Failure, Outcome, Run};
