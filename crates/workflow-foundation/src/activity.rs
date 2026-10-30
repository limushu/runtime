//! A business step over the existing service calls and Task control boundaries.
use crate::*;
use async_trait::async_trait;
use std::{sync::atomic::Ordering, time::Duration};

/// One step, retained in memory across retries and suspension. External effects
/// go through scope.call(); execute must consume the actual reply before returning.
/// Long local loops cooperate with scope.control(). No Future is forcibly dropped.
#[async_trait]
pub trait Activity: Send {
    type Reply: Send;
    type Output: Send;

    fn name(&self) -> &'static str;
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<Self::Reply>;
    /// Interpret a settled reply. Cancelled/Uncertain execution results bypass
    /// this handler. Retry limits and continuation data belong to the step.
    fn handle(&mut self, result: Outcome<Self::Reply>) -> Outcome<ActivityAction<Self::Output>>;
}

pub enum ActivityAction<T> {
    Continue(T),
    Retry(Duration),
    /// A domain operation stopped for a Task pause. Resume rechecks admission
    /// and calls execute on this same instance; the Workflow stays at its await.
    Suspend,
    /// Terminal cancellation, including when the Workflow is durable.
    Cancel,
}

/// Borrowed access to existing Workflow primitives. Each service call keeps its
/// own resource lease and observation; this adds no composite lock or executor.
pub struct ActivityScope<'a> {
    context: &'a mut WorkflowContext,
    name: &'static str,
}
impl ActivityScope<'_> {
    pub fn control(&self) -> ControlToken {
        self.context.control()
    }
    pub fn report_progress(&self, completed: u64, total: Option<u64>) {
        self.context.report_progress(self.name, completed, total);
    }
    pub async fn call<
        R: Send + 'static,
        P: Send + 'static,
        Q: Send + 'static,
        V: Send + 'static,
    >(
        &mut self,
        service: &ActivityClient<R, P, Q, V>,
        request: R,
    ) -> Outcome<P> {
        self.context
            .activity(service, request)
            .name(self.name)
            .await
    }
}

impl WorkflowContext {
    /// Interpret results in the Activity; return only its final business output
    /// to the Workflow. Local step state is not a persistent checkpoint.
    pub async fn run_activity<A: Activity>(&mut self, mut step: A) -> Outcome<A::Output> {
        let name = step.name();
        if name.chars().all(char::is_whitespace) || name.len() > 256 {
            return Err(Failure::Rejected("invalid Activity name".into()));
        }
        loop {
            self.wait_until_runnable().await?;
            let generation = self.control().request().generation;
            let result = step
                .execute(&mut ActivityScope {
                    context: self,
                    name,
                })
                .await;
            // Includes calls whose waiter was abandoned or whose Uncertain
            // result was swallowed in execute. Never start a new attempt then.
            self.tracker.ready()?;
            if let Err(error @ (Failure::Cancelled | Failure::Uncertain(_))) = result {
                return Err(error);
            }
            let action = match step.handle(result) {
                Err(Failure::Cancelled) => ActivityAction::Cancel,
                action => action?,
            };
            match action {
                ActivityAction::Continue(value) => return Ok(value),
                ActivityAction::Retry(delay) => self.sleep(delay).await?,
                ActivityAction::Suspend => {
                    self.check_execution()?;
                    let request = self.control().request();
                    // Resume can race the stopped reply. With no observed
                    // control change, Suspend would otherwise spin forever.
                    if !request.paused() && request.generation == generation {
                        return Err(Failure::Rejected("Suspend requires a Task pause".into()));
                    }
                }
                ActivityAction::Cancel => {
                    self.controls.user_cancelled.store(true, Ordering::SeqCst);
                    self.controls.revoke();
                    self.persist_intent().await.map_err(|error| {
                        Failure::Uncertain(format!("cancellation intent not persisted: {error}"))
                    })?;
                    return Err(Failure::Cancelled);
                }
            }
        }
    }
}
