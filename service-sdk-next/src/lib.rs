//! A small runtime for stateful, in-process services.
//!
//! The runtime owns lifecycle and future polling. A service owns its data,
//! request queues and business policy.

pub mod member_disk;
pub mod runtime;
