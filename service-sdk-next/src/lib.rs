//! A small runtime for stateful, in-process services.
//!
//! PoolManager routes isolated Pools. ProcessManager advances generic workflows
//! and owns object exclusion. Each domain has its own Runtime and typed activities.

pub mod member_disk;
pub mod pool;
pub mod process_manager;
pub mod runtime;
pub mod vd;
pub mod workflows;
