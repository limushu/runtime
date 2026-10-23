use super::{persistence::*, *};
use futures_util::future::BoxFuture;
use std::future::Future;

pub(crate) type RecoveryFn = Arc<
    dyn Fn(TaskCenter, StoredTask, bool) -> BoxFuture<'static, Outcome<WorkflowId>> + Send + Sync,
>;

/// Result of system-driven startup recovery. No TaskId is required from the caller.
#[derive(Debug, Default, Clone)]
pub struct RecoveryReport {
    pub executions: BTreeMap<TaskId, WorkflowId>,
    /// Unknown factories and uncertain outcomes retain their scheduling claims.
    pub blocked: Vec<RecoveryFailure>,
}
#[derive(Debug, Clone)]
pub struct RecoveryFailure {
    pub task_id: TaskId,
    pub error: Failure,
}

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
    pub(crate) fn recovery_initialized(&self) -> bool {
        self.persistence_ready.load(Ordering::SeqCst)
    }
    pub(super) fn persistence(&self) -> Outcome<&TaskPersistence> {
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
    /// Called once after this host owns the Pool and its services/topology are ready.
    /// Reconstruct eligible tasks automatically, preserving user suspension. Never replay
    /// NeedsCheck tasks or unknown task kinds; their claims remain and failures are reported.
    pub async fn recover_tasks(&self) -> Outcome<RecoveryReport> {
        if self.stop.is_cancelled() {
            return Err(Failure::Unavailable);
        }
        let mut initialized = self.persistence_initialized.lock().await;
        if *initialized {
            return Err(Failure::Rejected(
                "task recovery is already initialized".into(),
            ));
        }
        let mut tasks = self.persistence()?.recover().await?;
        // Preserve the original queue order when several resumable tasks share objects.
        tasks.sort_by_key(|t| (t.created_at, t.id));
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
        self.scheduling_commands
            .send(SchedulingCommand::Restore { claims, reply })
            .await
            .map_err(|_| Failure::Unavailable)?;
        received.await.map_err(|_| Failure::Unavailable)?;
        let mut report = RecoveryReport::default();
        for task in tasks.into_iter().filter(|t| t.recoverable()) {
            let id = task.id;
            if task.status == TaskStatus::NeedsCheck {
                report.blocked.push(RecoveryFailure {
                    task_id: id,
                    error: task.last_error.unwrap_or(Failure::Uncertain(
                        "business reconciliation required".into(),
                    )),
                });
                continue;
            }
            match self.rebuild_task(task, true).await {
                Ok(run) => {
                    report.executions.insert(id, run);
                }
                Err(error) => report.blocked.push(RecoveryFailure { task_id: id, error }),
            }
        }
        *initialized = true;
        self.persistence_ready.store(true, Ordering::SeqCst);
        self.open_admission();
        Ok(report)
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
        let handler: RecoveryFn = Arc::new(move |center, task, startup| {
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
                    .submit_prepared(workflow, operation, startup, Some(binding))
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
    /// Explicit repair/retry after a blocked recovery has been reconciled or a factory installed.
    /// Ordinary suspend/resume never creates another execution.
    pub async fn retry_task(&self, id: TaskId) -> Outcome<WorkflowId> {
        if !self.accepts() {
            return Err(Failure::Unavailable);
        }
        self.rebuild_task(self.task_detail(id).await?, false).await
    }
    async fn rebuild_task(&self, task: StoredTask, startup: bool) -> Outcome<WorkflowId> {
        if self.current_run(task.id).is_some() {
            return Err(Failure::Rejected(
                "task already has a live execution".into(),
            ));
        }
        if task.executions.last().is_some_and(|e| {
            e.instance == self.persistence.as_ref().unwrap().instance
                && e.activities.values().any(|a| {
                    matches!(
                        a.state,
                        RecordedActivityState::Failed(Failure::Uncertain(_))
                    )
                })
        }) {
            return Err(Failure::Rejected("uncertain activity resources require reconciliation and host takeover before retry".into()));
        }
        let resume = task
            .resume
            .as_ref()
            .ok_or_else(|| Failure::Rejected("task is history-only".into()))?;
        if !matches!(
            task.status,
            TaskStatus::Ready | TaskStatus::Paused | TaskStatus::NeedsCheck
        ) || task.cancellation_requested
        {
            return Err(Failure::Rejected(
                "task cannot be reconstructed in its current state".into(),
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
        handler(self.clone(), task, startup).await
    }
    fn current_run(&self, id: TaskId) -> Option<WorkflowId> {
        let runs: Vec<_> = self
            .task_ids
            .lock()
            .unwrap()
            .iter()
            .rev()
            .filter_map(|(run, task)| (*task == id).then_some(*run))
            .collect();
        runs.into_iter().find(|run| {
            self.execution
                .task(*run)
                .is_ok_and(|t| !matches!(t.state, RunState::Completed | RunState::Failed(_)))
        })
    }
    pub async fn suspend_task(&self, id: TaskId) -> Outcome<()> {
        let run = self
            .current_run(id)
            .ok_or_else(|| Failure::Rejected("task has no live execution".into()))?;
        self.suspend(run)?;
        self.flush_history().await
    }
    pub async fn resume_task(&self, id: TaskId) -> Outcome<WorkflowId> {
        let run = self.current_run(id).ok_or_else(|| {
            Failure::Rejected("task has no live execution; inspect its recovery report".into())
        })?;
        self.resume(run)?;
        self.flush_history().await?;
        Ok(run)
    }
    pub async fn cancel_task(&self, id: TaskId) -> Outcome<()> {
        if let Some(run) = self.current_run(id) {
            self.cancel(run)?;
            self.flush_history().await
        } else {
            self.persistence()?.cancel_inactive(id).await?;
            let (reply, received) = oneshot::channel();
            self.scheduling_commands
                .send(SchedulingCommand::Release { task: id, reply })
                .await
                .map_err(|_| Failure::Unavailable)?;
            received.await.map_err(|_| Failure::Unavailable)
        }
    }
}
