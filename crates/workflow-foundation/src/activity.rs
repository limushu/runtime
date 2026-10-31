//! Business steps, result interpretation and cooperative execution capabilities.
use crate::{tasks::ActivityObservation, *};
use async_trait::async_trait;
use std::{sync::Arc, time::Duration};

mod policy;
mod runner;
pub use policy::{Backoff, RetryPolicy};

/// One logical step. The same instance and ActivityId survive retries and
/// suspension. External effects use scope.call(); local loops cooperate at
/// scope.wait_until_runnable()/sleep() or inspect scope.control().
#[async_trait]
pub trait Activity: Send {
    type Reply: Send;
    type Output: Send;
    fn name(&self) -> &'static str;
    fn description(&self) -> Option<&str> {
        None
    }
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::default()
    }
    /// When false, defer external Task cancellation from the first execute entry
    /// until this logical step finishes, including retries and continuations.
    /// Admission is still cancellable. Suspension, host interruption and failures
    /// remain effective; a deliberate Cancel result still ends the step.
    fn cancellable(&self) -> bool {
        true
    }
    /// This step's read-only business prerequisite. The framework checks it at
    /// entry, retry and cooperative resume boundaries. Update its source and
    /// call RunControl::recheck() to wake a blocked step. Task-to-task exclusion
    /// belongs to the owner's SchedulingPolicy, not to this condition.
    fn run_condition(&self) -> Option<Arc<dyn RunCondition>> {
        None
    }
    async fn execute(&mut self, scope: &mut ActivityScope<'_>) -> Outcome<Self::Reply>;
    /// Interpret a settled business result. Cancelled/Uncertain execution
    /// results bypass this handler and can never be retried or hidden as success.
    fn handle(&mut self, result: Outcome<Self::Reply>) -> Outcome<ActivityAction<Self::Output>>;
}

pub enum ActivityAction<T> {
    Continue(T),
    Retry {
        reason: String,
    },
    Suspend(ActivityWait),
    /// Deliberate terminal cancellation, also for existing durable Workflows.
    Cancel,
}
pub enum ActivityWait {
    /// A domain operation stopped for a Task pause. Resume rechecks admission.
    Resume,
    /// Read-only, cancellation-safe check. Update its source and call
    /// RunControl::recheck() to wake the waiting Activity.
    Condition(Arc<dyn RunCondition>),
}

/// Calls retain their own resource leases and are children of this step;
/// there is no implicit multi-call transaction.
pub struct ActivityScope<'a> {
    context: &'a mut WorkflowContext,
    observation: &'a ActivityObservation,
    terminal_error: Option<Failure>,
}
impl ActivityScope<'_> {
    fn record_terminal<T>(&mut self, result: &Outcome<T>) {
        if let Err(error @ (Failure::Cancelled | Failure::Uncertain(_))) = result {
            self.terminal_error = Some(error.clone());
        }
    }
    pub fn id(&self) -> ActivityId {
        self.observation.id()
    }
    pub fn control(&self) -> ControlToken {
        self.context.control()
    }
    pub fn report_progress(&self, completed: u64, total: Option<u64>) {
        if total.is_some_and(|total| completed > total) {
            return;
        }
        let progress = TaskProgress {
            stage: Some(self.observation.name().into()),
            completed,
            total,
        };
        self.context.progress.send_replace(progress.clone());
        self.observation.update(|entry| entry.progress = progress);
    }
    /// Park inside execute without ending the attempt. Use only after actual
    /// operations have settled, never while external IO still needs stopping.
    pub async fn wait_until_runnable(&mut self) -> Outcome<()> {
        if let Some(error) = &self.terminal_error {
            return Err(error.clone());
        }
        let result = self.context.wait_observed(Some(self.observation)).await;
        self.record_terminal(&result);
        result
    }
    /// Local-loop delay: suspension parks this same execute Future; cancellation
    /// ends the delay. In-flight IO is never raced/dropped.
    pub async fn sleep(&mut self, delay: Duration) -> Outcome<()> {
        if let Some(error) = &self.terminal_error {
            return Err(error.clone());
        }
        let result = self
            .context
            .sleep_observed(delay, Some(self.observation))
            .await;
        self.record_terminal(&result);
        result
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
        if let Some(error) = &self.terminal_error {
            return Err(error.clone());
        }
        let id = self.id();
        let result = self.context.activity(service, request).parent(id).await;
        self.record_terminal(&result);
        result
    }
}
