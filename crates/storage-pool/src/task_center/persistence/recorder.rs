use super::*;
use crate::{Failure, Outcome, PoolInstanceId, PoolUuid};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::SystemTime,
};
use tokio::sync::{mpsc, oneshot};
use workflow_foundation::{
    AuditEvent, AuditRecord, AuditSink, CheckpointWriter, ExecutionOptions, ObjectKey, RunState,
};

type Job = Box<dyn FnOnce(&dyn TaskStore) + Send>;
enum Message {
    Call(Job),
    Close(oneshot::Sender<()>),
}

#[derive(Clone)]
pub(crate) struct TaskPersistence {
    pub pool: PoolUuid,
    pub instance: PoolInstanceId,
    tx: mpsc::Sender<Message>,
    config: PersistenceConfig,
    health: Arc<Mutex<PersistenceHealth>>,
}
#[derive(Clone)]
pub(crate) struct TaskBinding {
    pub id: TaskId,
    pub attempt: u64,
    pub is_restored: bool,
    pub initial_pause: bool,
    persistence: TaskPersistence,
    checkpoint_failed: Arc<AtomicBool>,
}
fn failed(health: &Mutex<PersistenceHealth>, error: &Failure) {
    let mut health = health.lock().unwrap();
    health.failed_records += 1;
    health.last_error = Some(error.to_string());
}
fn edit<T>(
    store: &dyn TaskStore,
    pool: PoolUuid,
    id: TaskId,
    mut f: impl FnMut(&mut StoredTask) -> Outcome<T>,
) -> Outcome<T> {
    for _ in 0..16 {
        let mut task = store
            .get(pool, id)?
            .ok_or_else(|| Failure::Rejected("unknown persistent task".into()))?;
        let revision = task.revision;
        let result = f(&mut task)?;
        task.revision = revision
            .checked_add(1)
            .ok_or_else(|| Failure::Rejected("task revision exhausted".into()))?;
        task.updated_at = SystemTime::now();
        if store.compare_and_set(Some(revision), &task)? {
            return Ok(result);
        }
    }
    Err(Failure::Busy)
}

impl TaskPersistence {
    pub fn new(
        pool: PoolUuid,
        instance: PoolInstanceId,
        store: Arc<dyn TaskStore>,
        config: PersistenceConfig,
    ) -> Outcome<Self> {
        if config.queue_capacity == 0 || config.max_resume_bytes == 0 {
            return Err(Failure::Rejected(
                "persistence limits must be nonzero".into(),
            ));
        }
        let (tx, mut rx) = mpsc::channel::<Message>(config.queue_capacity);
        let health = Arc::new(Mutex::new(PersistenceHealth::default()));
        let worker_health = health.clone();
        // Database locks, serialization and fsync never block the TaskCenter driver loop.
        tokio::task::spawn_blocking(move || {
            let mut closed = None;
            while let Some(message) = rx.blocking_recv() {
                match message {
                    Message::Call(job) => {
                        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            job(store.as_ref())
                        }))
                        .is_err()
                        {
                            failed(
                                &worker_health,
                                &Failure::Failed("task store adapter panicked".into()),
                            );
                        }
                    }
                    Message::Close(reply) => {
                        rx.close();
                        closed = Some(reply);
                        break;
                    }
                }
            }
            drop(store);
            if let Some(reply) = closed {
                let _ = reply.send(());
            }
        });
        Ok(Self {
            pool,
            instance,
            tx,
            config,
            health,
        })
    }
    async fn request<T: Send + 'static>(
        &self,
        f: impl FnOnce(&dyn TaskStore) -> Outcome<T> + Send + 'static,
    ) -> Outcome<T> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Message::Call(Box::new(move |store| {
                let _ = tx.send(f(store));
            })))
            .await
            .map_err(|_| Failure::Unavailable)?;
        rx.await.map_err(|_| Failure::Unavailable)?
    }
    fn observe(&self, f: impl FnOnce(&dyn TaskStore) -> Outcome<()> + Send + 'static) {
        let health = self.health.clone();
        if self
            .tx
            .try_send(Message::Call(Box::new(move |store| {
                if let Err(e) = f(store) {
                    failed(&health, &e);
                }
            })))
            .is_err()
        {
            failed(&self.health, &Failure::Busy);
        }
    }
    pub fn health(&self) -> PersistenceHealth {
        self.health.lock().unwrap().clone()
    }
    pub async fn close(&self) -> Outcome<()> {
        let (reply, received) = oneshot::channel();
        self.tx
            .send(Message::Close(reply))
            .await
            .map_err(|_| Failure::Unavailable)?;
        received.await.map_err(|_| Failure::Unavailable)
    }
    pub async fn flush(&self) -> Outcome<()> {
        self.request(|_| Ok(())).await?;
        let health = self.health();
        if health.failed_records != 0 {
            return Err(Failure::Failed(format!(
                "{} history updates failed: {:?}",
                health.failed_records, health.last_error
            )));
        }
        Ok(())
    }
    fn initial(
        &self,
        id: TaskId,
        name: String,
        objects: Vec<ObjectKey>,
        resume: Option<ResumeSpec>,
    ) -> StoredTask {
        let now = SystemTime::now();
        StoredTask {
            format_version: 1,
            id,
            pool: self.pool,
            name,
            class: TaskClass::General,
            objects,
            source_event: None,
            status: TaskStatus::Queued,
            revision: 0,
            created_at: now,
            updated_at: now,
            resume,
            pause_requested: false,
            system_pause_requested: false,
            cancellation_requested: false,
            last_error: None,
            executions: vec![StoredExecution::new(1, self.instance)],
        }
    }
    pub fn history_binding(
        &self,
        id: TaskId,
        name: String,
        objects: Vec<ObjectKey>,
        source_event: Option<uuid::Uuid>,
        class: TaskClass,
    ) -> TaskBinding {
        let mut task = self.initial(id, name, objects, None);
        task.source_event = source_event;
        task.class = class;
        let id = task.id;
        self.observe(move |store| {
            if !store.compare_and_set(None, &task)? {
                return Err(Failure::Rejected("task UUID already exists".into()));
            }
            Ok(())
        });
        TaskBinding {
            id,
            attempt: 1,
            is_restored: false,
            initial_pause: false,
            persistence: self.clone(),
            checkpoint_failed: Arc::default(),
        }
    }
    pub async fn resumable_binding(
        &self,
        id: TaskId,
        name: String,
        objects: Vec<ObjectKey>,
        resume: ResumeSpec,
        class: TaskClass,
    ) -> Outcome<TaskBinding> {
        if resume.kind.trim().is_empty()
            || resume.kind.len() > 128
            || resume.version == 0
            || resume.input.len().saturating_add(resume.checkpoint.len())
                > self.config.max_resume_bytes
        {
            return Err(Failure::Rejected(
                "invalid recovery kind/version or oversized resume data".into(),
            ));
        }
        let mut task = self.initial(id, name, objects, Some(resume));
        task.class = class;
        let id = task.id;
        self.request(move |store| {
            if !store.compare_and_set(None, &task)? {
                return Err(Failure::Rejected("task UUID already exists".into()));
            }
            Ok(())
        })
        .await?;
        Ok(TaskBinding {
            id,
            attempt: 1,
            is_restored: false,
            initial_pause: false,
            persistence: self.clone(),
            checkpoint_failed: Arc::default(),
        })
    }
    pub async fn claim(&self, previous: StoredTask) -> Outcome<TaskBinding> {
        let pool = self.pool;
        let instance = self.instance;
        let id = previous.id;
        let initial_pause = previous.pause_requested;
        let attempt = self
            .request(move |store| {
                let mut task = store
                    .get(pool, id)?
                    .ok_or_else(|| Failure::Rejected("unknown persistent task".into()))?;
                if task.revision != previous.revision {
                    return Err(Failure::Busy);
                }
                if task.resume.is_none()
                    || !matches!(
                        task.status,
                        TaskStatus::Ready | TaskStatus::NeedsCheck | TaskStatus::Paused
                    )
                    || task.cancellation_requested
                {
                    return Err(Failure::Rejected(
                        "task is not ready for continuation".into(),
                    ));
                }
                let attempt = task
                    .executions
                    .last()
                    .map_or(Some(1), |e| e.attempt.checked_add(1))
                    .ok_or_else(|| Failure::Rejected("execution counter exhausted".into()))?;
                task.executions
                    .push(StoredExecution::new(attempt, instance));
                task.status = TaskStatus::Queued;
                // System blockers belong to the new host's current topology tasks.
                task.system_pause_requested = false;
                task.last_error = None;
                task.revision += 1;
                task.updated_at = SystemTime::now();
                if !store.compare_and_set(Some(previous.revision), &task)? {
                    return Err(Failure::Busy);
                }
                Ok(attempt)
            })
            .await?;
        Ok(TaskBinding {
            id,
            attempt,
            is_restored: true,
            initial_pause,
            persistence: self.clone(),
            checkpoint_failed: Arc::default(),
        })
    }
    pub async fn get(&self, id: TaskId) -> Outcome<StoredTask> {
        let pool = self.pool;
        self.request(move |s| {
            s.get(pool, id)?
                .ok_or_else(|| Failure::Rejected("unknown persistent task".into()))
        })
        .await
    }
    pub async fn list(&self, query: TaskQuery) -> Outcome<TaskPage> {
        let pool = self.pool;
        self.request(move |s| s.list(pool, &query)).await
    }
    /// Caller must already own this Pool; this is discovery, not a distributed ownership protocol.
    pub async fn recover(&self) -> Outcome<Vec<StoredTask>> {
        let pool = self.pool;
        let instance = self.instance;
        self.request(move |store| {
            let mut query = TaskQuery {
                limit: 200,
                ..TaskQuery::default()
            };
            let mut recovered = Vec::new();
            loop {
                let page = store.list(pool, &query)?;
                for old in page.tasks {
                    if old.status.terminal() {
                        continue;
                    }
                    if old.executions.last().is_none_or(|e| e.instance == instance) {
                        continue;
                    }
                    edit(store, pool, old.id, |task| {
                        if task
                            .executions
                            .last()
                            .is_some_and(|e| e.instance == instance)
                        {
                            return Err(Failure::Busy);
                        }
                        if let Some(e) = task.executions.last_mut() {
                            e.interrupted = true;
                            e.finished_at.get_or_insert(SystemTime::now());
                        }
                        task.status = if task.status == TaskStatus::NeedsCheck {
                            TaskStatus::NeedsCheck
                        } else if task.resume.is_none() {
                            TaskStatus::Interrupted
                        } else if task.cancellation_requested {
                            TaskStatus::Cancelled
                        } else if task.pause_requested {
                            TaskStatus::Paused
                        } else {
                            TaskStatus::Ready
                        };
                        Ok(())
                    })?;
                    recovered.push(store.get(pool, old.id)?.unwrap());
                }
                match page.next {
                    Some(next) => query.before = Some(next),
                    None => break,
                }
            }
            Ok(recovered)
        })
        .await
    }
    pub async fn cancel_inactive(&self, id: TaskId) -> Outcome<()> {
        let pool = self.pool;
        self.request(move |s| {
            edit(s, pool, id, |t| {
                if !matches!(t.status, TaskStatus::Ready | TaskStatus::Paused) {
                    return Err(Failure::Rejected("task is not inactive".into()));
                }
                t.cancellation_requested = true;
                t.status = TaskStatus::Cancelled;
                Ok(())
            })
        })
        .await
    }
}

impl TaskBinding {
    pub fn options(&self) -> ExecutionOptions {
        ExecutionOptions {
            audit: Some(Arc::new(self.clone())),
            checkpoints: Some(Arc::new(self.clone())),
        }
    }
    fn apply_audit(&self, store: &dyn TaskStore, record: AuditRecord) -> Outcome<()> {
        edit(store, self.persistence.pool, self.id, |task| {
            let current = task
                .executions
                .last_mut()
                .ok_or_else(|| Failure::Rejected("missing execution".into()))?;
            // Delayed messages from an old host/attempt cannot overwrite the recovered task.
            if current.attempt != self.attempt
                || current.instance != self.persistence.instance
                || current.interrupted
            {
                return Ok(());
            }
            if current.finished_at.is_some() {
                return Ok(());
            }
            match &record.event {
                AuditEvent::SystemPause(value) => task.system_pause_requested = *value,
                AuditEvent::Registered { .. } => current.workflow = Some(record.workflow),
                AuditEvent::Activity(a) => {
                    current.activities.insert(a.id.sequence, a.clone().into());
                }
                AuditEvent::Progress(p) => current.progress = p.clone(),
                AuditEvent::Control { pause, cancel } => {
                    if let Some(p) = pause {
                        task.pause_requested = *p;
                    }
                    task.cancellation_requested |= cancel;
                }
                AuditEvent::State(state) => {
                    current.state = state.clone();
                    match state {
                        RunState::Completed => {
                            task.status = TaskStatus::Completed;
                            current.finished_at = Some(record.at);
                        }
                        RunState::Failed(error) => {
                            current.finished_at = Some(record.at);
                            task.last_error = Some(error.clone());
                            task.status = match error {
                                _ if task.resume.is_some()
                                    && self.checkpoint_failed.load(Ordering::SeqCst) =>
                                {
                                    TaskStatus::NeedsCheck
                                }
                                Failure::Cancelled
                                    if task.resume.is_some() && !task.cancellation_requested =>
                                {
                                    if task.pause_requested {
                                        TaskStatus::Paused
                                    } else {
                                        TaskStatus::Ready
                                    }
                                }
                                Failure::Cancelled => TaskStatus::Cancelled,
                                Failure::Uncertain(_) => TaskStatus::NeedsCheck,
                                _ => TaskStatus::Failed,
                            };
                        }
                        RunState::Paused => task.status = TaskStatus::Paused,
                        RunState::WaitingConditions(_) => {
                            task.status = TaskStatus::WaitingConditions
                        }
                        RunState::WaitingObjects => task.status = TaskStatus::Queued,
                        _ => {
                            current.started_at.get_or_insert(record.at);
                            task.status = TaskStatus::Running;
                        }
                    }
                }
            }
            Ok(())
        })
    }
    pub fn rejected(&self, error: Failure) {
        let binding = self.clone();
        self.persistence.observe(move |store| {
            edit(store, binding.persistence.pool, binding.id, |task| {
                if let Some(e) = task.executions.last_mut().filter(|e| {
                    e.attempt == binding.attempt
                        && e.instance == binding.persistence.instance
                        && !e.interrupted
                }) {
                    e.state = RunState::Failed(error.clone());
                    e.finished_at = Some(SystemTime::now());
                    task.last_error = Some(error.clone());
                    task.status = if binding.is_restored && task.pause_requested {
                        TaskStatus::Paused
                    } else if binding.is_restored {
                        TaskStatus::Ready
                    } else {
                        TaskStatus::Failed
                    };
                }
                Ok(())
            })
        });
    }
}
impl AuditSink for TaskBinding {
    fn record(&self, record: AuditRecord) {
        let binding = self.clone();
        self.persistence
            .observe(move |store| binding.apply_audit(store, record));
    }
}
#[async_trait::async_trait]
impl CheckpointWriter for TaskBinding {
    async fn save(&self, checkpoint: Vec<u8>) -> Outcome<()> {
        let binding = self.clone();
        if checkpoint.len() > self.persistence.config.max_resume_bytes {
            self.checkpoint_failed.store(true, Ordering::SeqCst);
            return Err(Failure::Uncertain(
                "checkpoint too large; business progress needs verification".into(),
            ));
        }
        let result = self
            .persistence
            .request(move |s| {
                edit(s, binding.persistence.pool, binding.id, |t| {
                    let execution = t
                        .executions
                        .last()
                        .ok_or_else(|| Failure::Rejected("missing execution".into()))?;
                    if execution.attempt != binding.attempt
                        || execution.instance != binding.persistence.instance
                        || execution.interrupted
                        || execution.finished_at.is_some()
                    {
                        return Err(Failure::Rejected(
                            "checkpoint belongs to a stale execution".into(),
                        ));
                    }
                    let resume = t
                        .resume
                        .as_mut()
                        .ok_or_else(|| Failure::Rejected("task is history-only".into()))?;
                    if resume.input.len().saturating_add(checkpoint.len())
                        > binding.persistence.config.max_resume_bytes
                    {
                        return Err(Failure::Rejected("resume data too large".into()));
                    }
                    resume.checkpoint = checkpoint.clone();
                    Ok(())
                })
            })
            .await;
        if result.is_err() {
            self.checkpoint_failed.store(true, Ordering::SeqCst);
        }
        result.map_err(|e| Failure::Uncertain(format!("checkpoint was not durably confirmed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WorkflowId;

    #[tokio::test]
    async fn old_checkpoint_writer_and_terminal_audit_cannot_overwrite_a_new_attempt() {
        let store = Arc::new(MemoryTaskStore::default());
        let pool = PoolUuid::new();
        let old = TaskPersistence::new(
            pool,
            PoolInstanceId::new(),
            store.clone(),
            PersistenceConfig::default(),
        )
        .unwrap();
        let binding = old
            .resumable_binding(
                TaskId::new(),
                "test".into(),
                vec![ObjectKey::new("disk", "d1")],
                ResumeSpec {
                    kind: "test".into(),
                    version: 1,
                    input: vec![],
                    checkpoint: vec![0],
                },
                TaskClass::General,
            )
            .await
            .unwrap();
        binding.save(vec![1]).await.unwrap();
        let new = TaskPersistence::new(
            pool,
            PoolInstanceId::new(),
            store,
            PersistenceConfig::default(),
        )
        .unwrap();
        new.recover().await.unwrap();
        let next = new.claim(new.get(binding.id).await.unwrap()).await.unwrap();
        next.save(vec![2]).await.unwrap();
        assert!(binding.save(vec![9]).await.is_err());
        binding.record(AuditRecord {
            workflow: WorkflowId(1),
            at: SystemTime::now(),
            event: AuditEvent::State(RunState::Completed),
        });
        old.flush().await.unwrap();
        let task = new.get(binding.id).await.unwrap();
        assert_eq!(task.resume.unwrap().checkpoint, vec![2]);
        assert_eq!(task.status, TaskStatus::Queued);
        assert_eq!(task.executions.len(), 2);
        old.close().await.unwrap();
        new.close().await.unwrap();
    }
}
