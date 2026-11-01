//! Execution evidence, separate from the workflow recovery checkpoint.
//! Events are captured at mutation sites, never reconstructed from watch subscriptions.
use crate::{Failure, Outcome, TaskId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    sync::{Arc, Mutex, mpsc},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::oneshot;

mod file;
pub use file::{FileAuditStore, read_file};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditEvent {
    pub schema: u32,
    pub task: Option<TaskId>,
    pub pool_uuid: String,
    pub pool_name: String,
    /// Distinguishes execution attempts after restoration; Activity sequence is local to this attempt.
    pub execution: String,
    pub owner_epoch: u64,
    pub at_ms: u64,
    pub kind: String,
    pub activity: Option<u64>,
    pub data: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    pub cursor: u64,
    pub event: AuditEvent,
}
/// A backend must preserve append order and acknowledged records across its advertised failures.
/// A shared backend must also fence stale epochs; the file adapter is single-host only.
pub trait AuditStore: Send + Sync + 'static {
    fn append(&self, event: AuditEvent) -> Outcome<()>;
    fn flush(&self) -> Outcome<()>;
    fn records(&self, after: u64, limit: usize) -> Outcome<Vec<AuditRecord>>;
}
enum Message {
    Event(Box<AuditEvent>),
    Flush(oneshot::Sender<Outcome<()>>),
}
#[derive(Clone)]
pub struct AuditLog {
    tx: mpsc::SyncSender<Message>,
    failure: Arc<Mutex<Option<Failure>>>,
    store: Arc<dyn AuditStore>,
}
impl AuditLog {
    pub fn new(store: Arc<dyn AuditStore>, capacity: usize) -> Outcome<Self> {
        if capacity == 0 {
            return Err(Failure::Rejected("audit capacity must be nonzero".into()));
        }
        let (tx, rx) = mpsc::sync_channel(capacity);
        let failure = Arc::new(Mutex::new(None));
        let failed = failure.clone();
        let backend = store.clone();
        std::thread::Builder::new()
            .name("workflow-audit-writer".into())
            .spawn(move || {
                let mut last_event: Option<AuditEvent> = None;
                for message in rx {
                    match message {
                        Message::Event(event) => {
                            let event = *event;
                            last_event = Some(event.clone());
                            // Drain already queued evidence even after a sticky error.
                            if let Err(error) = backend.append(event) {
                                *failed.lock().unwrap() = Some(error);
                            }
                        }
                        Message::Flush(reply) => {
                            if failed.lock().unwrap().is_some()
                                && let Some(mut event) = last_event.clone()
                            {
                                event.kind = "EvidenceIncomplete".into();
                                event.activity = None;
                                event.data = serde_json::json!({
                                    "reason": "audit queue or writer failed; this stream is incomplete"
                                });
                                let _ = backend.append(event);
                            }
                            let result = backend.flush().and_then(|()| {
                                failed.lock().unwrap().clone().map_or(Ok(()), Err)
                            });
                            let _ = reply.send(result);
                        }
                    }
                }
                let _ = backend.flush();
            })
            .map_err(|e| Failure::Uncertain(format!("audit writer: {e}")))?;
        Ok(Self { tx, failure, store })
    }
    pub fn health(&self) -> Outcome<()> {
        self.failure.lock().unwrap().clone().map_or(Ok(()), Err)
    }
    pub fn records(&self, after: u64, limit: usize) -> Outcome<Vec<AuditRecord>> {
        self.store.records(after, limit.min(1000))
    }
    pub(crate) fn register_pool(&self, pool: u128, name: String, epoch: u64) {
        self.emit(AuditEvent {
            schema: 1,
            task: None,
            pool_uuid: format!("{pool:032x}"),
            pool_name: name,
            execution: uuid::Uuid::new_v4().to_string(),
            owner_epoch: epoch,
            at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            kind: "PoolRegistered".into(),
            activity: None,
            data: Value::Null,
        });
    }
    fn emit(&self, event: AuditEvent) {
        if self.tx.try_send(Message::Event(Box::new(event))).is_err() {
            *self.failure.lock().unwrap() = Some(Failure::Uncertain(
                "audit queue full or writer unavailable; evidence incomplete".into(),
            ));
        }
    }
    /// Barrier: no success acknowledgement before preceding evidence has reached durable storage.
    pub async fn flush(&self) -> Outcome<()> {
        let (tx, rx) = oneshot::channel();
        let sender = self.tx.clone();
        tokio::task::spawn_blocking(move || sender.send(Message::Flush(tx)))
            .await
            .map_err(|_| Failure::Unavailable)?
            .map_err(|_| Failure::Unavailable)?;
        rx.await.map_err(|_| Failure::Unavailable)?
    }
    pub(crate) fn task(&self, task: TaskId, pool_name: String, owner_epoch: u64) -> TaskAudit {
        TaskAudit(Some(Arc::new(TaskAuditInner {
            log: self.clone(),
            task,
            pool_name,
            owner_epoch,
            execution: uuid::Uuid::new_v4().to_string(),
        })))
    }
}
#[derive(Clone, Default)]
pub(crate) struct TaskAudit(Option<Arc<TaskAuditInner>>);
struct TaskAuditInner {
    log: AuditLog,
    task: TaskId,
    pool_name: String,
    owner_epoch: u64,
    execution: String,
}
impl TaskAudit {
    pub fn emit(&self, kind: &str, activity: Option<u64>, data: Value) {
        if let Some(a) = &self.0 {
            a.log.emit(AuditEvent {
                schema: 1,
                task: Some(a.task),
                pool_uuid: format!("{:032x}", a.task.pool),
                pool_name: a.pool_name.clone(),
                execution: a.execution.clone(),
                owner_epoch: a.owner_epoch,
                at_ms: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis()
                    .min(u64::MAX as u128) as u64,
                kind: kind.into(),
                activity,
                data,
            });
        }
    }
    pub fn health(&self) -> Outcome<()> {
        self.0.as_ref().map_or(Ok(()), |a| a.log.health())
    }
    pub async fn flush(&self) -> Outcome<()> {
        if let Some(a) = &self.0 {
            a.log.flush().await
        } else {
            Ok(())
        }
    }
}
/// Capture every Task state publication, including transitions coalesced by watch.
#[derive(Clone)]
pub(crate) struct TaskState {
    tx: tokio::sync::watch::Sender<crate::RunState>,
    audit: TaskAudit,
}
impl TaskState {
    pub fn new(tx: tokio::sync::watch::Sender<crate::RunState>, audit: TaskAudit) -> Self {
        Self { tx, audit }
    }
    pub fn send_replace(&self, state: crate::RunState) -> crate::RunState {
        if *self.tx.borrow() != state {
            self.audit.emit(
                "TaskState",
                None,
                serde_json::to_value(&state).expect("serializable state"),
            );
        }
        self.tx.send_replace(state)
    }
}
