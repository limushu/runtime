//! Domain entities, rules, services, and external capability contracts.
use workflow_foundation::{Failure, ObjectKey, Outcome};

macro_rules! identity {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(String);
        impl $name {
            pub fn new(value: impl Into<String>) -> Outcome<Self> {
                let value = value.into();
                if value.trim().is_empty() || value.len() > 128 || value.trim() != value {
                    return Err(Failure::Rejected(
                        "identity must be 1..128 bytes without edge whitespace".into(),
                    ));
                }
                Ok(Self(value))
            }
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}
identity!(PoolId);
identity!(TierId);

pub(crate) fn object(kind: &str, id: &str) -> ObjectKey {
    ObjectKey::new(kind, id)
}
pub(crate) fn reject(message: &str) -> Failure {
    Failure::Rejected(message.into())
}

pub mod disk;
pub mod node;
pub mod ports;
pub mod vd;
