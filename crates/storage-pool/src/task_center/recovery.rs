use super::{persistence::*, *};
use futures_util::future::BoxFuture;
use std::future::Future;

pub(crate) type RecoveryFn =
    Arc<dyn Fn(TaskCenter, StoredTask) -> BoxFuture<'static, Outcome<WorkflowId>> + Send + Sync>;

/// Stable logical task identity plus this execution's typed Run.
pub struct PersistentRun<T> {
    pub task_id: TaskId,
    pub run: Run<T>,
}
impl<T> std::ops::Deref for PersistentRun<T> {
    type Target = Run<T>;
    fn deref(&self) -> &Self::Target {
        &self.run
    }
}
impl<T> std::ops::DerefMut for PersistentRun<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.run
    }
}

impl TaskCenter {
    fn persistence(&self) -> Outcome<&TaskPersistence> {
        self.persistence
            .as_ref()
            .ok_or_else(|| Failure::Rejected("TaskStore is not configured".into()))
    }
    pub fn persistent_task_id(&self, workflow: WorkflowId) -> Outcome<TaskId> {
        self.task_ids
            .lock()
            .unwrap()
            .get(&workflow)
            .copied()
            .ok_or_else(|| Failure::Rejected("unknown persistent task mapping".into()))
    }
    pub fn persistence_health(&self) -> Outcome<PersistenceHealth> {
        Ok(self.persistence()?.health())
    }
    pub async fn flush_history(&self) -> Outcome<()> {
        self.persistence()?.flush().await
    }
    /// After shutdown, release this worker's adapter handle. Queries require another host.
    pub async fn close_task_store(&self) -> Outcome<()> {
        if *self.lifecycle.borrow() != Lifecycle::Stopped {
            return Err(Failure::Rejected(
                "shutdown TaskCenter before closing its store".into(),
            ));
        }
        self.persistence()?.close().await
    }
    pub async fn task_history(&self, query: TaskQuery) -> Outcome<TaskPage> {
        self.persistence()?.list(query).await
    }
    pub async fn task_detail(&self, id: TaskId) -> Outcome<StoredTask> {
        self.persistence()?.get(id).await
    }
    /// Call only after the previous Pool owner has stopped. Discovery does not run workflows.
    pub async fn recover_tasks(&self) -> Outcome<Vec<StoredTask>> {
        if self.stop.is_cancelled() {
            return Err(Failure::Unavailable);
        }
        let mut initialized = self.persistence_initialized.lock().await;
        if *initialized {
            return Err(Failure::Rejected(
                "task recovery is already initialized; query history instead".into(),
            ));
        }
        let tasks = self.persistence()?.recover().await?;
        let claims = tasks
            .iter()
            .filter(|t| t.recoverable())
            .map(|t| {
                (
                    t.id,
                    PoolOperation {
                        kind: OperationKind::from_task(t),
                        objects: t.objects.clone(),
                    },
                )
            })
            .collect();
        let (reply, received) = oneshot::channel();
        self.persistence_control
            .send(PersistenceControl::Restore { claims, reply })
            .await
            .map_err(|_| Failure::Unavailable)?;
        received.await.map_err(|_| Failure::Unavailable)?;
        *initialized = true;
        self.persistence_ready.store(true, Ordering::SeqCst);
        self.open_admission();
        Ok(tasks)
    }

    /// Extension point for application-owned workflows. Uses the same conflict/object rules.
    pub async fn submit_task<W: Workflow>(&self, workflow: W) -> Outcome<Run<W::Output>> {
        self.submit_classified(workflow, OperationKind::UserTask(W::NAME.into()))
            .await
    }
    /// Shrink participates in pool-wide topology coordination and retains its single slot
    /// while paused or waiting on business conditions. A read-only guard is required.
    pub async fn submit_shrink<W: Workflow>(&self, workflow: W) -> Outcome<Run<W::Output>> {
        if workflow.continuation_guard().is_none() {
            return Err(Failure::Rejected(
                "shrink requires a continuation guard".into(),
            ));
        }
        self.submit_classified(workflow, OperationKind::Shrink)
            .await
    }
    pub async fn submit_topology<W: Workflow>(&self, workflow: W) -> Outcome<Run<W::Output>> {
        self.submit_classified(workflow, OperationKind::Topology(W::NAME.into()))
            .await
    }
    async fn submit_classified<W: Workflow>(
        &self,
        workflow: W,
        kind: OperationKind,
    ) -> Outcome<Run<W::Output>> {
        let operation = PoolOperation {
            kind,
            objects: workflow.objects(),
        };
        let binding = self.persistence.as_ref().map(|p| {
            p.history_binding(
                W::NAME.into(),
                operation.objects.clone(),
                None,
                operation.kind.task_class(),
            )
        });
        self.submit_prepared(workflow, operation, false, binding)
            .await
    }
    pub async fn submit_resumable<W: Workflow>(
        &self,
        workflow: W,
        resume: ResumeSpec,
    ) -> Outcome<PersistentRun<W::Output>> {
        self.submit_resumable_class(workflow, resume, TaskClass::General)
            .await
    }
    pub async fn submit_resumable_shrink<W: Workflow>(
        &self,
        workflow: W,
        resume: ResumeSpec,
    ) -> Outcome<PersistentRun<W::Output>> {
        if workflow.continuation_guard().is_none() {
            return Err(Failure::Rejected(
                "shrink requires a continuation guard".into(),
            ));
        }
        self.submit_resumable_class(workflow, resume, TaskClass::Shrink)
            .await
    }
    async fn submit_resumable_class<W: Workflow>(
        &self,
        workflow: W,
        resume: ResumeSpec,
        class: TaskClass,
    ) -> Outcome<PersistentRun<W::Output>> {
        if !self.accepts() {
            return Err(Failure::Unavailable);
        }
        let operation = PoolOperation {
            kind: if class == TaskClass::Shrink {
                OperationKind::Shrink
            } else {
                OperationKind::UserTask(W::NAME.into())
            },
            objects: workflow.objects(),
        };
        if operation.objects.is_empty() {
            return Err(Failure::Rejected(
                "resumable tasks must declare their objects".into(),
            ));
        }
        let binding = self
            .persistence()?
            .resumable_binding(W::NAME.into(), operation.objects.clone(), resume, class)
            .await?;
        let task_id = binding.id;
        let run = self
            .submit_prepared(workflow, operation, false, Some(binding))
            .await?;
        Ok(PersistentRun { task_id, run })
    }
    /// The factory must verify real business progress/old effects before rebuilding the workflow.
    /// No service clients or futures are deserialized. Register against the NEW host's services.
    pub fn register_recovery<W, F, Fut>(
        &self,
        kind: impl Into<String>,
        version: u32,
        factory: F,
    ) -> Outcome<()>
    where
        W: Workflow,
        F: Fn(StoredTask) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Outcome<W>> + Send + 'static,
    {
        let kind = kind.into();
        if kind.trim().is_empty() || version == 0 {
            return Err(Failure::Rejected("invalid recovery kind/version".into()));
        }
        let factory = Arc::new(factory);
        let handler: RecoveryFn = Arc::new(move |center, task| {
            let factory = factory.clone();
            Box::pin(async move {
                let workflow = factory(task.clone()).await?;
                if W::NAME != task.name || workflow.objects() != task.objects {
                    return Err(Failure::Rejected(
                        "recovered workflow changed its name or conflict objects".into(),
                    ));
                }
                if task.class == TaskClass::Shrink && workflow.continuation_guard().is_none() {
                    return Err(Failure::Rejected(
                        "recovered shrink requires a continuation guard".into(),
                    ));
                }
                let kind = OperationKind::from_task(&task);
                let binding = center.persistence()?.claim(task).await?;
                let operation = PoolOperation {
                    kind,
                    objects: workflow.objects(),
                };
                let run = center
                    .submit_prepared(workflow, operation, false, Some(binding))
                    .await?;
                Ok(run.id())
            })
        });
        let mut recovery = self.recovery.lock().unwrap();
        if recovery.contains_key(&(kind.clone(), version)) {
            return Err(Failure::Rejected(
                "recovery function already registered".into(),
            ));
        }
        recovery.insert((kind, version), handler);
        Ok(())
    }
    pub async fn continue_task(&self, id: TaskId) -> Outcome<WorkflowId> {
        if !self.accepts() {
            return Err(Failure::Unavailable);
        }
        let task = self.task_detail(id).await?;
        // A business checkpoint failure only fences the TaskCenter's logical objects.
        // Uncertain Activities also fence the runtime resource broker, which has no in-place
        // reconciliation API. Keep those fences until a properly reconciled host takeover.
        if task.executions.last().is_some_and(|e| {
            e.instance == self.persistence.as_ref().unwrap().instance
                && e.activities.values().any(|a| {
                    matches!(
                        a.state,
                        RecordedActivityState::Failed(Failure::Uncertain(_))
                    )
                })
        }) {
            return Err(Failure::Rejected("uncertain activity resources require reconciliation and host takeover before continuation".into()));
        }
        let resume = task
            .resume
            .as_ref()
            .ok_or_else(|| Failure::Rejected("task is history-only".into()))?;
        if !matches!(task.status, TaskStatus::Ready | TaskStatus::NeedsCheck)
            || task.pause_requested
            || task.cancellation_requested
        {
            return Err(Failure::Rejected(
                "task is not ready for continuation".into(),
            ));
        }
        let handler = self
            .recovery
            .lock()
            .unwrap()
            .get(&(resume.kind.clone(), resume.version))
            .cloned()
            .ok_or_else(|| {
                Failure::Rejected("no recovery function for task kind/version".into())
            })?;
        handler(self.clone(), task).await
    }
    fn current_run(&self, t: &StoredTask) -> Option<WorkflowId> {
        let instance = self.persistence.as_ref()?.instance;
        t.executions
            .last()
            .filter(|e| e.instance == instance && !e.interrupted && e.finished_at.is_none())
            .and_then(|e| e.workflow)
    }
    pub async fn pause_task(&self, id: TaskId) -> Outcome<()> {
        let task = self.task_detail(id).await?;
        let run = self
            .current_run(&task)
            .ok_or_else(|| Failure::Rejected("task has no live execution".into()))?;
        self.suspend(run)?;
        self.flush_history().await
    }
    pub async fn resume_task(&self, id: TaskId) -> Outcome<WorkflowId> {
        let task = self.task_detail(id).await?;
        if let Some(run) = self.current_run(&task) {
            self.resume(run)?;
            self.flush_history().await?;
            return Ok(run);
        }
        self.persistence()?.clear_pause(id).await?;
        self.continue_task(id).await
    }
    pub async fn cancel_task(&self, id: TaskId) -> Outcome<()> {
        let task = self.task_detail(id).await?;
        if let Some(run) = self.current_run(&task) {
            self.cancel(run)?;
            self.flush_history().await
        } else {
            self.persistence()?.cancel_inactive(id).await?;
            let (reply, received) = oneshot::channel();
            self.persistence_control
                .send(PersistenceControl::Release { task: id, reply })
                .await
                .map_err(|_| Failure::Unavailable)?;
            received.await.map_err(|_| Failure::Unavailable)
        }
    }
}
