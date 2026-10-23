use super::{
    service::{BalanceCommand, BalanceService},
    *,
};
use crate::{Failure, Outcome, ports::ArcJob, support::object};
use async_trait::async_trait;
use std::sync::Arc;
use workflow_foundation::*;

pub(crate) struct BalanceHost(pub Arc<BalanceService>);
struct JobPause {
    job: ArcJob,
    commands: tokio::sync::Mutex<()>,
}
#[async_trait]
impl ActivityPause for JobPause {
    async fn pause(&self) -> Outcome<()> {
        let _guard = self.commands.lock().await;
        self.job.pause().await
    }
    async fn resume(&self) -> Outcome<()> {
        let _guard = self.commands.lock().await;
        self.job.resume().await
    }
}
#[async_trait]
impl ServiceHandler for BalanceHost {
    type Request = BalanceCommand;
    type Reply = ();
    type Query = ();
    type View = ();
    fn activity_name(request: &BalanceCommand) -> &'static str {
        match request {
            BalanceCommand::Evacuate { .. } => "balance.evacuate-whole-disk",
            BalanceCommand::Cancel { .. } => "balance.cancel-evacuation",
        }
    }
    fn resources(request: &BalanceCommand) -> Vec<ResourceAccess> {
        let id = match request {
            BalanceCommand::Evacuate { id, .. } | BalanceCommand::Cancel { id, .. } => id,
        };
        vec![ResourceAccess::write(object(
            "balance-job",
            &id.to_string(),
        ))]
    }
    async fn execute(&self, request: BalanceCommand, context: ActivityContext) -> Outcome<()> {
        let (disk, id) = match request {
            BalanceCommand::Evacuate { disk, id } => (disk, id),
            BalanceCommand::Cancel { disk, id } => {
                if let Some(job) = self.0.find(id).await? {
                    if job.progress().borrow().disk != disk {
                        return Err(Failure::Rejected("job belongs to another disk".into()));
                    }
                    job.cancel().await?;
                    return match job.wait().await {
                        Ok(()) | Err(Failure::Cancelled) => Ok(()),
                        Err(error) if matches!(error, Failure::Uncertain(_)) => Err(error),
                        Err(_) => Ok(()),
                    };
                }
                return Ok(());
            }
        };
        // Never abandon an acceptance request: obtain the handle, then stop it if needed.
        let job = self.0.open(&disk, id).await?;
        let control = Arc::new(JobPause {
            job: job.clone(),
            commands: tokio::sync::Mutex::new(()),
        });
        context.register_pause(control.clone())?;
        let mut progress = job.progress();
        if progress.borrow().disk != disk {
            return Err(Failure::Uncertain("job disk does not match intent".into()));
        }
        if progress.borrow().state == EvacuationState::Paused
            && !context.control().request().pause_requested()
            && !context.control().is_cancelled()
        {
            control.resume().await?;
        }
        let operation = job.wait();
        tokio::pin!(operation);
        loop {
            let current = progress.borrow_and_update().clone();
            context.report_progress(format!("evacuating {id}"), current.completed, current.total);
            tokio::select! {
                biased;
                result = &mut operation => return result,
                _ = context.control().cancelled() => {
                    let _guard = control.commands.lock().await;
                    if context.control().cancellation_reason() == Some(CancellationReason::Interrupted) {
                        job.pause().await.map_err(|e| Failure::Uncertain(format!("handoff pause not confirmed: {e}")))?;
                        return Err(Failure::Cancelled);
                    }
                    let cancel = job.cancel().await;
                    let settled = operation.await;
                    return match cancel {
                        Ok(()) => settled,
                        Err(error) => Err(Failure::Uncertain(format!("evacuation cancel not confirmed: {error}"))),
                    };
                }
                changed = progress.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
            }
        }
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }
}
