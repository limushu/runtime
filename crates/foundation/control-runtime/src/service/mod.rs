mod container;
mod contract;
mod object_slot;
mod service_loop;

pub use container::{spawn_service, ControlHandle, ManagedService, RuntimeConfig};
pub use contract::{Admission, ObjectActivity, OrphanPolicy, RequestRoute, Service, WorkflowMeta};
