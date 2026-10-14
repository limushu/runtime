//! A small runtime for stateful, in-process services.
//!
//! PoolManager routes isolated Pool roots. ProcessManager owns workflows;
//! domains own state and resource protection. Runtime supplies execution.

pub mod member_disk;
pub mod pool;
pub mod process_manager;
pub mod runtime;
pub mod vd;
