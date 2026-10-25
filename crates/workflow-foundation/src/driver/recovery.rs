//! Restore persisted Tasks. Read restore() first: load -> prepare -> claim -> register.
//! Workflow::restore only rebuilds an execution body during prepare; Task identity,
//! owner, control state, checkpoint and the scheduling start gate belong to this path.
use super::*;
use crate::persistence::{self, SavedTask, StoredOutcome, TaskStore};

impl WorkflowDriver {
    /// Recover the same Tasks, without executing their business logic.
    /// Every record is checked before any ownership write; every ownership write
    /// must succeed before any Task is registered. The owner opens start gates later.
    pub async fn restore(&mut self) -> Outcome<Vec<Restored>> {
        if self.recovery_ready || !self.running.is_empty() {
            return Err(Failure::Rejected(
                "restore is only valid on a fresh durable host".into(),
            ));
        }
        self.check_registration()?;
        let store = self.store.clone().ok_or(Failure::Unavailable)?;

        let records = self.load_pending_tasks(&store).await?;
        let mut tasks = self.prepare_tasks(records)?;
        self.claim_tasks(&store, &mut tasks).await?;
        self.check_registration()?;
        let restored = self.register_restored_tasks(tasks, store);

        self.recovery_ready = true;
        Ok(restored)
    }

    /// Configure Task storage after external leader fencing, before accepting work.
    /// claim() fences task-record writes; it does not fence external domain effects.
    pub async fn enable_persistence(&mut self, store: Arc<dyn TaskStore>) -> Outcome<()> {
        if self.ids.epoch() == 0 || !self.running.is_empty() || self.store.is_some() {
            return Err(Failure::Rejected(
                "configure persistence once, with a fresh leader epoch, before work".into(),
            ));
        }
        self.recovery_ready = false;
        let backend = store.clone();
        let pool = self.pool_uuid;
        let epoch = self.ids.epoch();
        persistence::blocking(move || backend.claim(pool, epoch)).await?;
        self.store = Some(store);
        Ok(())
    }

    /// Register how to rebuild this Task's execution body and bind local clients.
    /// No records are read and no Workflow is constructed until restore().
    pub fn define<W: Workflow>(&mut self, dependencies: Dependencies) -> Outcome<()> {
        if self.definitions.contains_key(W::NAME) {
            return Err(Failure::Rejected("workflow already defined".into()));
        }
        self.definitions.insert(
            W::NAME.into(),
            Arc::new(move |task| prepare_execution::<W>(task, &dependencies)),
        );
        Ok(())
    }

    async fn load_pending_tasks(&mut self, store: &Arc<dyn TaskStore>) -> Outcome<Vec<SavedTask>> {
        let backend = store.clone();
        let pool = self.pool_uuid;
        let mut records = persistence::blocking(move || backend.list(pool)).await?;
        records.retain(|task| {
            matches!(
                task.outcome,
                StoredOutcome::Pending | StoredOutcome::NeedsCheck(_)
            )
        });
        records.sort_by_key(|task| task.id);
        if records.len() > self.config.max_workflows {
            return Err(Failure::Busy);
        }
        Ok(records)
    }

    fn prepare_tasks(&self, records: Vec<SavedTask>) -> Outcome<Vec<PreparedRestore>> {
        let mut tasks = Vec::with_capacity(records.len());
        for task in records {
            if task.cancel_requested || matches!(task.outcome, StoredOutcome::NeedsCheck(_)) {
                return Err(Failure::Rejected(format!(
                    "task {} requires reconciliation",
                    task.id
                )));
            }
            let definition = self.definitions.get(task.id.business()).ok_or_else(|| {
                Failure::Rejected(format!("undefined workflow {}", task.id.business()))
            })?;
            tasks.push(definition(task)?);
        }
        Ok(tasks)
    }

    async fn claim_tasks(
        &mut self,
        store: &Arc<dyn TaskStore>,
        tasks: &mut [PreparedRestore],
    ) -> Outcome<()> {
        for prepared in tasks {
            let task = &mut prepared.record;
            let previous = task.revision;
            task.revision = task
                .revision
                .checked_add(1)
                .ok_or_else(|| Failure::Rejected("task revision exhausted".into()))?;
            task.owner_epoch = self.ids.epoch();
            let backend = store.clone();
            let saved = task.clone();
            persistence::blocking(move || backend.save(saved, Some(previous))).await?;
        }
        // These are individual CAS writes, not a batch transaction. A failed write
        // leaves admission closed and registers no Tasks. A retry reloads the store.
        Ok(())
    }

    fn register_restored_tasks(
        &mut self,
        tasks: Vec<PreparedRestore>,
        store: Arc<dyn TaskStore>,
    ) -> Vec<Restored> {
        // Build every inert Future before exposing any Task. Construction performs
        // no user callbacks, validation or IO; registration below cannot return Err.
        let executions: Vec<_> = tasks
            .into_iter()
            .map(|prepared| (prepared.build_execution)(self, prepared.record, store.clone()))
            .collect();
        let mut restored = Vec::with_capacity(executions.len());
        for (task, future) in executions {
            self.register_execution(task.control.clone(), future);
            restored.push(task);
        }
        self.publish();
        restored
    }
}

// Type erasure is confined here: stored Tasks may have different Workflow types.
// This is temporary recovery data, not another Task identity or runtime manager.
pub(super) type Definition = Arc<dyn Fn(SavedTask) -> Outcome<PreparedRestore> + Send + Sync>;
type BuildExecution = Box<
    dyn FnOnce(
            &WorkflowDriver,
            SavedTask,
            Arc<dyn TaskStore>,
        ) -> (Restored, BoxFuture<'static, TaskId>)
        + Send,
>;
pub(super) struct PreparedRestore {
    record: SavedTask,
    build_execution: BuildExecution,
}

fn prepare_execution<W: Workflow>(
    task: SavedTask,
    dependencies: &Dependencies,
) -> Outcome<PreparedRestore> {
    if task.version == 0 || task.revision == u64::MAX {
        return Err(Failure::Rejected(
            "invalid schema version or exhausted task revision".into(),
        ));
    }
    for object in &task.objects {
        object.validate()?;
    }
    let workflow = W::restore(&task, dependencies)?;
    if workflow.objects() != task.objects || task.id.business() != W::NAME {
        return Err(Failure::Rejected(
            "restoration changed workflow identity or claims".into(),
        ));
    }
    match workflow.persistence()? {
        Persistence::Durable { version, input }
            if version == task.version && input == task.input => {}
        _ => {
            return Err(Failure::Rejected(
                "restoration changed durable input/version".into(),
            ));
        }
    }
    Ok(PreparedRestore {
        record: task,
        build_execution: Box::new(move |driver, record, store| {
            let (tx, rx) = oneshot::channel();
            let journal = persistence::Journal {
                record: Arc::new(tokio::sync::Mutex::new(record.clone())),
                store,
            };
            let env =
                driver.execution_env(record.id, Some(rx), Some(journal), Some(record.clone()));
            let (run, future) = execution(workflow, env);
            (
                Restored {
                    record,
                    control: run.control,
                    start: StartPermit(tx),
                },
                future,
            )
        }),
    })
}

/// Handoff receipt: the original task control plus its scheduling metadata and start gate.
/// The owner installs ALL claims before opening ANY of these gates.
pub struct Restored {
    pub record: SavedTask,
    pub control: RunControl,
    pub start: StartPermit,
}
