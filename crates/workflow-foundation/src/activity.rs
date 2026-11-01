//! Business steps, result interpretation and cooperative execution capabilities.
use crate::*;
use async_trait::async_trait;
use std::sync::Arc;

pub(crate) mod invocation;
mod policy;
mod runner;
pub use invocation::{control, id, report_progress, sleep, wait_until_runnable};
pub use policy::{Backoff, RetryPolicy};

/// One logical step. The same instance and ActivityId survive retries and
/// suspension. External effects use client.send(); the runner binds each call
/// to this execution. Local loops can use activity::sleep()/wait_until_runnable().
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
    async fn execute(&mut self) -> Outcome<Self::Reply>;
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
