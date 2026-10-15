//! Application entry points, workflow orchestration, and business admission policy.
pub mod coordination;
mod pool;
mod requests;
pub mod workflows;

pub use pool::{Pool, PoolHandle, PoolManager};
pub use requests::CreateVolume;
