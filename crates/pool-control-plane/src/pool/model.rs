use crate::kernel::PoolId;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolSpec {
    pub id: PoolId,
    pub name: Arc<str>,
}

impl PoolSpec {
    pub fn new(id: PoolId, name: impl Into<Arc<str>>) -> Self {
        Self {
            id,
            name: name.into(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoolPatch {
    pub name: Option<Arc<str>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolLifecycle {
    Creating,
    Active,
    Draining,
    Removed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolMetadata {
    pub spec: PoolSpec,
    pub lifecycle: PoolLifecycle,
    pub revision: u64,
}

impl PoolMetadata {
    pub(crate) fn creating(spec: PoolSpec) -> Self {
        Self {
            spec,
            lifecycle: PoolLifecycle::Creating,
            revision: 1,
        }
    }

    pub(crate) fn apply(&mut self, patch: PoolPatch) {
        if let Some(name) = patch.name {
            self.spec.name = name;
        }
        self.revision += 1;
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PoolSnapshot {
    pub metadata: PoolMetadata,
    pub member_disk_count: usize,
}
