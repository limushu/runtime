use std::sync::Arc;

use crate::{CancelReason, TaskKey};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TaskVisibility {
    Public,
    Internal,
}

#[derive(Clone, Debug)]
pub struct TaskMeta {
    pub key: TaskKey,
    pub label: Arc<str>,
    pub visibility: TaskVisibility,
}

impl TaskMeta {
    pub fn new(key: TaskKey, label: impl Into<Arc<str>>) -> Self {
        Self {
            key,
            label: label.into(),
            visibility: TaskVisibility::Internal,
        }
    }

    pub fn public(mut self) -> Self {
        self.visibility = TaskVisibility::Public;
        self
    }
}

#[derive(Clone, Debug)]
pub enum ConflictPolicy {
    Reject,
    Replace(CancelReason),
}
