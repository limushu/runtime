use super::*;
use workflow_foundation::{
    ActivityClient, ActivityContext, Failure, ResourceAccess, ServiceHandler,
};

pub type BalanceClient = ActivityClient<Migrate, MigrationExit, (), ()>;
pub struct BalanceHost(pub Arc<dyn BalanceBackend>);
#[async_trait]
impl ServiceHandler for BalanceHost {
    type Request = Migrate;
    type Reply = MigrationExit;
    type Query = ();
    type View = ();
    fn resources(_: &Migrate) -> Vec<ResourceAccess> {
        // Logical disk exclusion is owned by TaskCenter. Keeping a disk resource
        // locked across the long C wait would obstruct topology handoff.
        vec![]
    }
    fn activity_name(_: &Migrate) -> &'static str {
        "balance.evacuate"
    }
    fn query(&self, _: ()) -> Outcome<()> {
        Ok(())
    }

    async fn execute(&self, request: Migrate, ctx: ActivityContext) -> Outcome<MigrationExit> {
        let mut control = ctx.control();
        let operation = self.0.submit(request).await?;
        let mut progress = operation.progress();
        let completion = operation.completion();
        tokio::pin!(completion);
        let mut suspend_sent = false;
        let mut cancel_sent = false;
        let mut progress_open = true;
        let mut control_error = None;

        loop {
            let current = progress.borrow_and_update().clone();
            ctx.report_progress("migrating", current.completed, Some(current.total));

            let command = if control.is_cancelled() && !cancel_sent {
                cancel_sent = true;
                Some(operation.cancel().await)
            } else if control.request().paused() && !suspend_sent && !cancel_sent {
                suspend_sent = true;
                Some(operation.suspend().await)
            } else {
                None
            };
            if let Some(Err(error)) = command {
                // Keep the original invocation alive until it settles. Command failure
                // must not free callback memory or masquerade as a suspension ACK.
                control_error = Some(Failure::Uncertain(format!(
                    "balance control failed: {error}"
                )));
                if !cancel_sent {
                    cancel_sent = true;
                    let _ = operation.cancel().await;
                }
            }

            tokio::select! {
                biased;
                result = &mut completion => {
                    if matches!(result, Err(Failure::Uncertain(_))) { return result; }
                    if let Some(error) = control_error { return Err(error); }
                    let result = result?;
                    return match result {
                        // Completed is a business fact, including completion racing cancellation.
                        ActivityExit::Completed(done) => {
                            ctx.report_progress("migrating", done.completed, Some(done.total));
                            Ok(ActivityExit::Completed(done))
                        }
                        ActivityExit::Suspended(_) if control.is_cancelled() => Err(Failure::Cancelled),
                        ActivityExit::Suspended(_) if !suspend_sent => Err(Failure::Uncertain(
                            "balance returned Suspended without a suspension request".into())),
                        ActivityExit::Suspended(saved) => {
                            ctx.report_progress("migrating", saved.progress.completed, Some(saved.progress.total));
                            Ok(ActivityExit::Suspended(saved))
                        }
                    };
                }
                changed = control.changed(), if !cancel_sent => {
                    if let Err(error) = changed && error != Failure::Cancelled {
                        control_error = Some(Failure::Uncertain(format!("balance control channel: {error}")));
                        cancel_sent = true;
                        let _ = operation.cancel().await;
                    }
                }
                changed = progress.changed(), if progress_open => { progress_open = changed.is_ok(); }
            }
        }
    }
}
