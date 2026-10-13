//! A small runtime for stateful, in-process services.
//!
//! Runtime owns execution, cancellation and resource admission. Workflows are
//! ordinary async functions; activities protect domain state and provider effects.

pub mod member_disk;
pub mod runtime;
