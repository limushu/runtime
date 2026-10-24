use super::*;
use crate::persistence::{self, Dependencies, Restored, StoredOutcome, TaskStore};

impl WorkflowDriver {
    /// Define construction once for this workflow type, not a separate recovery workflow.
    pub fn define<W: Workflow>(&mut self, dependencies: Dependencies) -> Outcome<()> {
        if self.definitions.contains_key(W::NAME) {
            return Err(Failure::Rejected("workflow already defined".into()));
        }
        self.definitions.insert(
            W::NAME.into(),
            Arc::new(move |record| {
                let workflow = W::restore(record, &dependencies)?;
                if workflow.objects() != record.objects || record.id.business() != W::NAME {
                    return Err(Failure::Rejected(
                        "restoration changed workflow identity or claims".into(),
                    ));
                }
                match workflow.persistence()? {
                    Persistence::Durable { version, input }
                        if version == record.version && input == record.input => {}
                    _ => {
                        return Err(Failure::Rejected(
                            "restoration changed durable input/version".into(),
                        ));
                    }
                }
                Ok(Box::new(move |driver, record| {
                    let (tx, rx) = oneshot::channel();
                    let run = driver.enqueue_with(
                        workflow,
                        Some(rx),
                        record.metadata.clone(),
                        Some(record),
                    )?;
                    Ok((run.control, StartPermit(tx)))
                }))
            }),
        );
        Ok(())
    }
    /// Call after external leader fencing and before accepting tasks.
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
    /// Preflight EVERY pending task before starting any. Unknown versions/uncertain tasks
    /// fail closed; their claims must not silently disappear. No operation is replayed here.
    pub async fn restore(&mut self) -> Outcome<Vec<Restored>> {
        if self.recovery_ready || !self.running.is_empty() {
            return Err(Failure::Rejected(
                "restore is only valid on a fresh durable host".into(),
            ));
        }
        let store = self.store.clone().ok_or(Failure::Unavailable)?;
        let backend = store.clone();
        let pool = self.pool_uuid;
        let mut records = persistence::blocking(move || backend.list(pool)).await?;
        records.retain(|r| {
            matches!(
                r.outcome,
                StoredOutcome::Pending | StoredOutcome::NeedsCheck(_)
            )
        });
        records.sort_by_key(|r| r.id);
        if records.len() > self.config.max_workflows {
            return Err(Failure::Busy);
        }
        let mut prepared = Vec::new();
        for record in records {
            if record.cancel_requested || matches!(record.outcome, StoredOutcome::NeedsCheck(_)) {
                return Err(Failure::Rejected(format!(
                    "task {} requires reconciliation",
                    record.id
                )));
            }
            let definition = self.definitions.get(record.id.business()).ok_or_else(|| {
                Failure::Rejected(format!("undefined workflow {}", record.id.business()))
            })?;
            prepared.push((definition(&record)?, record));
        }
        let mut claimed = Vec::new();
        for (construct, mut record) in prepared {
            let previous = record.revision;
            record.revision = record
                .revision
                .checked_add(1)
                .ok_or_else(|| Failure::Rejected("task revision exhausted".into()))?;
            record.owner_epoch = self.ids.epoch();
            let backend = store.clone();
            let saved = record.clone();
            persistence::blocking(move || backend.save(saved, Some(previous))).await?;
            claimed.push((construct, record));
        }
        let mut restored = Vec::new();
        for (construct, record) in claimed {
            let (control, start) = construct(self, record.clone())?;
            restored.push(Restored {
                record,
                control,
                start,
            });
        }
        self.recovery_ready = true;
        Ok(restored)
    }
}
