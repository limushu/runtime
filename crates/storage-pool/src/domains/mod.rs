//! Domain entities, rules, services, and external capability contracts.
/// A business validation error, independent of execution and transport failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainError(pub String);
impl std::fmt::Display for DomainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for DomainError {}
pub type DomainResult<T> = Result<T, DomainError>;

macro_rules! identity {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
        #[serde(try_from = "String", into = "String")]
        pub struct $name(String);
        impl TryFrom<String> for $name { type Error = $crate::domains::DomainError; fn try_from(v: String) -> Result<Self, Self::Error> { Self::new(v) } }
        impl From<$name> for String { fn from(v: $name) -> Self { v.0 } }
        impl $name {
            pub fn new(value: impl Into<String>) -> $crate::domains::DomainResult<Self> {
                let value = value.into();
                if value.trim().is_empty() || value.len() > 128 || value.trim() != value {
                    return Err($crate::domains::DomainError(
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

pub mod disk;
pub mod node;
pub mod tier;
pub mod vd;

pub mod balance;
