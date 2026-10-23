use crate::{Outcome, PoolId, domains::DomainError, support::reject};
use std::{fmt, str::FromStr};

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct PoolUuid(uuid::Uuid);
impl PoolUuid {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}
impl Default for PoolUuid {
    fn default() -> Self {
        Self::new()
    }
}
impl FromStr for PoolUuid {
    type Err = DomainError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let uuid =
            uuid::Uuid::parse_str(value).map_err(|_| DomainError("invalid pool UUID".into()))?;
        if uuid.is_nil() {
            return Err(DomainError("nil pool UUID".into()));
        }
        Ok(Self(uuid))
    }
}
impl fmt::Display for PoolUuid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Identifies one in-memory Pool host. Never substitutes for stable PoolUuid or an HA term.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct PoolInstanceId(uuid::Uuid);
impl PoolInstanceId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}
impl Default for PoolInstanceId {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Display for PoolInstanceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PoolName(String);
impl PoolName {
    pub fn new(value: impl Into<String>) -> Result<Self, DomainError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > 128
            || value.trim() != value
            || value.chars().any(char::is_control)
        {
            return Err(DomainError(
                "pool name must be 1..128 bytes without edge whitespace or control characters"
                    .into(),
            ));
        }
        Ok(Self(value))
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl fmt::Display for PoolName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoolSelector {
    Id(PoolId),
    Uuid(PoolUuid),
    Name(PoolName),
}
impl From<PoolId> for PoolSelector {
    fn from(id: PoolId) -> Self {
        Self::Id(id)
    }
}
impl From<&PoolId> for PoolSelector {
    fn from(id: &PoolId) -> Self {
        Self::Id(id.clone())
    }
}
impl From<PoolUuid> for PoolSelector {
    fn from(uuid: PoolUuid) -> Self {
        Self::Uuid(uuid)
    }
}
impl From<PoolName> for PoolSelector {
    fn from(name: PoolName) -> Self {
        Self::Name(name)
    }
}
impl From<&PoolName> for PoolSelector {
    fn from(name: &PoolName) -> Self {
        Self::Name(name.clone())
    }
}
impl From<&PoolUuid> for PoolSelector {
    fn from(uuid: &PoolUuid) -> Self {
        Self::Uuid(*uuid)
    }
}
impl From<&PoolSelector> for PoolSelector {
    fn from(selector: &PoolSelector) -> Self {
        selector.clone()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolLifecycle {
    Active,
    Unloading,
    Unloaded,
    Loading,
    Deleting,
    Deleted,
    ReconcileRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolDescriptor {
    pub id: PoolId,
    pub uuid: PoolUuid,
    pub name: PoolName,
    pub revision: u64,
    pub lifecycle: PoolLifecycle,
}

#[derive(Debug, Clone)]
pub struct CreatePool {
    pub id: PoolId,
    pub uuid: PoolUuid,
    pub name: PoolName,
}
impl CreatePool {
    pub fn new(id: PoolId, name: PoolName) -> Self {
        Self {
            id,
            uuid: PoolUuid::new(),
            name,
        }
    }
    pub(crate) fn descriptor(self) -> PoolDescriptor {
        PoolDescriptor {
            id: self.id,
            uuid: self.uuid,
            name: self.name,
            revision: 0,
            lifecycle: PoolLifecycle::Active,
        }
    }
}
impl PoolDescriptor {
    pub(crate) fn active(&self) -> Outcome<()> {
        if self.lifecycle == PoolLifecycle::Active {
            Ok(())
        } else {
            Err(reject(format!("pool is {:?}", self.lifecycle)))
        }
    }
}
