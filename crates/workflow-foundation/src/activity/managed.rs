//! Opt-in execution that keeps the Workflow awaiting across suspension.
//! The existing Activity/run_activity contract remains independent and unchanged.
use super::{Invocation, Options, invoke};
use crate::*;
use async_trait::async_trait;
use std::{sync::Arc, time::SystemTime};
use tokio::{sync::watch, time::Instant};

/// One logical step with explicit execution and business interpretation.
/// The same instance survives retries and in-process suspension. Its fields are
/// not automatically persisted; Workflow::restore still reconstructs durable work.
#[async_trait]
pub trait ManagedActivity: Send + 'static {
    type Reply: Send + 'static;
    type Output: Send + 'static;

    fn name(&self) -> &'static str;
    fn description(&self) -> Option<&str> {
        None
    }
    fn resources(&self) -> Vec<ResourceAccess> {
        vec![]
    }
    fn retry(&self) -> Option<Retry> {
        None
    }

    async fn execute(&mut self, scope: &mut ActivityScope) -> Outcome<Self::Reply>;
    /// Called only after the attempt's actual operations have settled. Cancelled
    /// and Uncertain bypass this handler and cannot be reclassified as success.
    fn handle(&mut self, result: Outcome<Self::Reply>) -> Outcome<ActivityAction<Self::Output>>;

    /// Optional COMPLETE Task checkpoint, saved before any suspension ACK.
    /// Opting in requires a Durable Workflow. This replaces its checkpoint, so
    /// include the Workflow stage and all other recovery data, not just this step.
    fn checkpoint(&self) -> Outcome<Option<serde_json::Value>> {
        Ok(None)
    }
}

pub enum ActivityAction<T> {
    /// This step is finished; return the business value and advance the Workflow.
    Continue(T),
    Retry {
        reason: String,
    },
    Suspend(ActivityWait),
    /// Terminal task cancellation, including for a Durable Workflow.
    Cancel,
}

pub enum ActivityWait {
    /// An observed Task control request stopped the domain operation. Await the
    /// Task's existing pause reasons and admission condition; resume may race the reply.
    Runnable,
    /// Business waiting. The check must be read-only and cancellation-safe.
    /// Update its source state and call RunControl::recheck() to wake the waiter.
    Condition(Arc<dyn RunCondition>),
}

struct Backoff {
    deadline: Instant,
    reason: String,
    error: Option<Failure>,
}
struct Execution<A> {
    step: A,
    retry: Option<Retry>,
    retries: u32,
    backoff: Option<Backoff>,
}
enum Segment<A: ManagedActivity> {
    Complete(A::Output),
    Suspended(Execution<A>, ActivityWait),
    Cancelled,
}

impl WorkflowContext {
    /// Await the final business result. Suspension is handled here; the Workflow
    /// stays at this await. Each execution segment uses the existing Activity
    /// admission, guard, resource lease and observation machinery.
    pub async fn run_managed_activity<A: ManagedActivity>(
        &mut self,
        step: A,
    ) -> Outcome<A::Output> {
        let mut execution = Execution {
            retry: step.retry(),
            step,
            retries: 0,
            backoff: None,
        };
        loop {
            let name = execution.step.name();
            let options = Options {
                name: Some(name),
                description: execution.step.description().map(str::to_owned),
                retry: execution.retry,
                ..Options::default()
            };
            let history = self.activities.clone();
            let invocation = Invocation::body(
                self.scope.clone(),
                name,
                execution.step.resources(),
                move |scope| run_segment(scope, execution, history),
            );
            // The segment owns its step and guard until the actual operation
            // settles, even if the caller abandons this response Future.
            match self.execute_activity(invocation, options).await? {
                Segment::Complete(value) => return Ok(value),
                Segment::Cancelled => {
                    // Distinguish deliberate cancellation from host interruption:
                    // durable recovery must not restart a step that chose Cancel.
                    self.controls
                        .user_cancelled
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                    self.controls.revoke();
                    self.persist_intent().await.map_err(|error| {
                        Failure::Uncertain(format!(
                            "activity cancellation intent not persisted: {error}"
                        ))
                    })?;
                    return Err(Failure::Cancelled);
                }
                Segment::Suspended(next, until) => {
                    execution = next;
                    // Actual effects and the lease have settled. Persist any
                    // supplied continuation before acknowledging a Task pause.
                    if let Some(checkpoint) = execution.step.checkpoint()? {
                        self.save_checkpoint(&checkpoint).await?;
                    }
                    self.wait_for_managed_activity(until).await?;
                }
            }
        }
    }

    async fn wait_for_managed_activity(&mut self, until: ActivityWait) -> Outcome<()> {
        let ActivityWait::Condition(condition) = until else {
            return self.wait_until_runnable().await;
        };
        let mut control = self.control();
        loop {
            // This is also where external pause requests are acknowledged, after
            // resource release and checkpointing, including while business-blocked.
            self.wait_until_runnable().await?;
            let generation = control.request().generation;
            let decision = tokio::select! {
                biased;
                changed = control.changed() => { changed?; continue; },
                decision = condition.check() => decision?,
            };
            if generation != control.request().generation {
                continue;
            }
            match decision {
                Readiness::Ready => return Ok(()),
                Readiness::Blocked(reason) => {
                    self.state.send_replace(RunState::WaitingConditions(reason));
                    self.persist_observation().await?;
                    tokio::select! {
                        changed = control.changed() => { changed?; },
                        _ = self.controls.recheck.notified() => {},
                    }
                }
            }
        }
    }
}

async fn run_segment<A: ManagedActivity>(
    mut scope: ActivityScope,
    mut execution: Execution<A>,
    history: watch::Sender<ActivityHistory>,
) -> Outcome<Segment<A>> {
    let mut attempts = 0;
    observe(&history, scope.id(), |entry| entry.attempts = 0);
    loop {
        if scope.check_control()? == ActivityExit::Suspended(()) {
            return Ok(Segment::Suspended(execution, ActivityWait::Runnable));
        }
        if let Some(backoff) = &execution.backoff {
            let delay = backoff.deadline.saturating_duration_since(Instant::now());
            observe(&history, scope.id(), |entry| {
                entry.retry_reason = Some(backoff.reason.clone());
                entry.last_error = backoff.error.clone();
                entry.retry_at = SystemTime::now().checked_add(delay);
            });
            if scope.sleep(delay).await? == ActivityExit::Suspended(()) {
                return Ok(Segment::Suspended(execution, ActivityWait::Runnable));
            }
            execution.backoff = None;
            continue; // Recheck controls before starting the next attempt.
        }
        attempts += 1;
        observe(&history, scope.id(), |entry| {
            entry.attempts = attempts;
            entry.retry_at = None;
        });
        let generation = scope.control().request().generation;
        let step = &mut execution.step;
        let result = invoke(scope.attempt_scope(), |mut attempt| async move {
            step.execute(&mut attempt).await
        })
        .await;
        // Never abandon an in-flight Future or pass an uncertain result to policy.
        if let Err(error @ (Failure::Cancelled | Failure::Uncertain(_))) = result {
            return Err(error);
        }
        let last_error = result.as_ref().err().cloned();
        match execution.step.handle(result)? {
            ActivityAction::Continue(value) => return Ok(Segment::Complete(value)),
            ActivityAction::Cancel => return Ok(Segment::Cancelled),
            ActivityAction::Suspend(until) => {
                let request = scope.control().request();
                if matches!(until, ActivityWait::Runnable)
                    && !request.paused()
                    && generation == request.generation
                {
                    return Err(Failure::Rejected(
                        "Suspend(Runnable) requires a Task control request; use Condition for business waiting".into()
                    ));
                }
                return Ok(Segment::Suspended(execution, until));
            }
            ActivityAction::Retry { reason } => {
                observe(&history, scope.id(), |entry| {
                    entry.retry_reason = Some(reason.clone());
                    entry.last_error = last_error.clone();
                });
                let Some(policy) = execution.retry else {
                    return Err(Failure::Failed(format!(
                        "activity requested retry without a retry policy: {reason}"
                    )));
                };
                if execution.retries >= policy.attempts - 1 {
                    return Err(Failure::Failed(format!(
                        "activity retry limit exhausted (max attempts {}): {reason}",
                        policy.attempts
                    )));
                }
                execution.retries += 1;
                execution.backoff = Some(Backoff {
                    deadline: Instant::now() + policy.delay,
                    reason,
                    error: last_error,
                });
            }
        }
    }
}

fn observe(
    history: &watch::Sender<ActivityHistory>,
    id: ActivityId,
    update: impl FnOnce(&mut ActivitySnapshot),
) {
    history.send_modify(|history| {
        if let Some(entry) = history.entries.iter_mut().find(|entry| entry.id == id) {
            update(entry);
        }
    });
}
