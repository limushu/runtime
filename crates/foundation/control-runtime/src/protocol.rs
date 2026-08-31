use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_CALL_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_TASK_ATTEMPT_ID: AtomicU64 = AtomicU64::new(1);

macro_rules! string_id {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(Arc<str>);

        impl $name {
            pub fn new(value: impl Into<Arc<str>>) -> Self {
                Self(value.into())
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter
                    .debug_tuple(stringify!($name))
                    .field(&self.0)
                    .finish()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

string_id!(ServiceId, "Opaque service-routing identity.");
string_id!(ObjectKey, "Opaque conflict and admission identity.");

macro_rules! sequence_id {
    ($name:ident, $counter:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(u64);

        impl $name {
            pub fn next() -> Self {
                Self($counter.fetch_add(1, Ordering::Relaxed))
            }

            pub fn get(self) -> u64 {
                self.0
            }
        }
    };
}

sequence_id!(OperationId, NEXT_OPERATION_ID);
sequence_id!(CallId, NEXT_CALL_ID);
sequence_id!(TaskAttemptId, NEXT_TASK_ATTEMPT_ID);

/// One typed request accepted by a service endpoint.
///
/// A request deliberately carries no routing identity. Callers choose an
/// explicit `ServiceClient`; domain facades normally hide the command enum
/// altogether and expose named methods instead.
pub trait ServiceRequest: Send + 'static {
    type Response: Clone + Send + Sync + 'static;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opaque_ids_are_value_objects() {
        assert_eq!(ServiceId::new("service-a"), ServiceId::new("service-a"));
        assert_eq!(ObjectKey::new("object/a").as_str(), "object/a");
        assert!(OperationId::next().get() < OperationId::next().get());
    }
}
