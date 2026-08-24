//! End-to-end example for the disk -> BG rebuild business.
//!
//! The module is intentionally production-shaped: each feature owns its messages,
//! handlers, workflow functions and one `install.rs` mapping.

mod blueprint;
mod protocol;
mod state;

pub mod bg_rebuild;
pub mod disk_offline;
pub mod rebuild;

pub use blueprint::{DemoBlueprint, DemoSystem, default_demo_catalog, demo_context};
pub use protocol::*;
pub use state::{BgBackend, DemoCatalog};
