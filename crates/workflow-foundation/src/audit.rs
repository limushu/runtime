//! Optional application-owned observation and durable checkpoints. No database dependency.
use crate::{ActivitySnapshot, Outcome, RunState, TaskProgress, WorkflowId};
use async_trait::async_trait;
use std::{panic::AssertUnwindSafe, sync::Arc, time::SystemTime};

#[derive(Debug, Clone)]
pub enum AuditEvent {
    Registered { name: &'static str },
    State(RunState),
    Progress(TaskProgress),
    Activity(ActivitySnapshot),
    Control { pause: Option<bool>, cancel: bool },
    SystemPause(bool),
}

#[derive(Debug, Clone)]
pub struct AuditRecord {
    pub workflow: WorkflowId,
    pub at: SystemTime,
    pub event: AuditEvent,
}

/// Called at the source of lifecycle changes, including after an observer is dropped.
/// Implementations must return promptly; queue IO elsewhere and expose recording failures.
pub trait AuditSink: Send + Sync + 'static {
    fn record(&self, record: AuditRecord);
}

#[async_trait]
pub trait CheckpointWriter: Send + Sync + 'static {
    /// Return only after the checkpoint is durably committed. Encoding belongs to the caller.
    async fn save(&self, checkpoint: Vec<u8>) -> Outcome<()>;
}

#[derive(Clone, Default)]
pub struct ExecutionOptions {
    pub audit: Option<Arc<dyn AuditSink>>,
    pub checkpoints: Option<Arc<dyn CheckpointWriter>>,
}

#[derive(Clone)]
pub(crate) struct AuditEmitter {
    pub workflow: WorkflowId,
    pub sink: Option<Arc<dyn AuditSink>>,
}
impl AuditEmitter {
    pub fn emit(&self, event: AuditEvent) {
        if let Some(sink) = &self.sink {
            // Observation must never turn an already applied effect into a business failure.
            let _ = std::panic::catch_unwind(AssertUnwindSafe(|| {
                sink.record(AuditRecord {
                    workflow: self.workflow,
                    at: SystemTime::now(),
                    event,
                });
            }));
        }
    }
}
