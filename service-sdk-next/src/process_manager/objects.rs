use std::sync::Arc;

/// Identity shared by one workflow manager and its domain runtimes.
#[derive(Clone, Default)]
pub struct ExecutionScope(Arc<()>);
impl ExecutionScope {
    pub(crate) fn same(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectKey {
    pub namespace: String,
    pub id: Option<String>,
}
impl ObjectKey {
    pub fn new(namespace: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            id: Some(id.into()),
        }
    }
    pub fn collection(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            id: None,
        }
    }
    fn overlaps(&self, other: &Self) -> bool {
        self.namespace == other.namespace
            && (self.id.is_none() || other.id.is_none() || self.id == other.id)
    }
    fn covers(&self, other: &Self) -> bool {
        self.namespace == other.namespace && (self.id.is_none() || self.id == other.id)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    Shared,
    Exclusive,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectClaim {
    pub object: ObjectKey,
    pub access: Access,
}
impl ObjectClaim {
    pub fn exclusive(object: ObjectKey) -> Self {
        Self {
            object,
            access: Access::Exclusive,
        }
    }
    pub fn shared(object: ObjectKey) -> Self {
        Self {
            object,
            access: Access::Shared,
        }
    }
    pub(crate) fn conflicts(&self, other: &Self) -> bool {
        self.object.overlaps(&other.object)
            && (self.access == Access::Exclusive || other.access == Access::Exclusive)
    }
    pub(crate) fn covers(&self, other: &Self) -> bool {
        self.object.covers(&other.object)
            && (self.access == Access::Exclusive || other.access == Access::Shared)
    }
}
