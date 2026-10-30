use super::*;
use futures_util::FutureExt;
use std::{panic::AssertUnwindSafe, sync::atomic::Ordering, time::SystemTime};
use tokio_util::sync::CancellationToken;

/// A dropped caller requests cooperative cancellation. Observation stays in
/// Settling until its actual child calls finish; no native Future is aborted.
struct Execution {
    observation: ActivityObservation,
    cancel: CancellationToken,
    finished: bool,
}
impl Drop for Execution {
    fn drop(&mut self) {
        if !self.finished {
            self.cancel.cancel();
        }
    }
}
impl WorkflowContext {
    pub async fn run_activity<A: Activity>(&mut self, mut step: A) -> Outcome<A::Output> {
        let name = step.name();
        if name.chars().all(char::is_whitespace) || name.len() > 256 {
            return Err(Failure::Rejected("invalid Activity name".into()));
        }
        let policy = step.retry_policy();
        policy.validate()?;
        let description = step.description().map(str::to_owned);
        if description.as_ref().is_some_and(|text| text.len() > 4096) {
            return Err(Failure::Rejected(
                "Activity description exceeds 4 KiB".into(),
            ));
        }
        let id = self.next_activity_id()?;
        let mut execution = Execution {
            observation: ActivityObservation::logical(
                self.activities.clone(),
                self.activity_history,
                id,
                name,
                description,
            ),
            cancel: self.cancel.clone(),
            finished: false,
        };
        let result = AssertUnwindSafe(self.run_step(&mut step, policy, &execution.observation))
            .catch_unwind()
            .await;
        let mut result = match result {
            Ok(result) => result,
            Err(_) => {
                self.tracker.mark_uncertain();
                Err(Failure::Uncertain(
                    "Activity execution or result handler panicked".into(),
                ))
            }
        };
        if result.as_ref().is_err_and(Failure::uncertain) {
            self.tracker.mark_uncertain();
        }
        execution.observation.settled(&result);
        // A panic may have abandoned a service response before run_step could
        // check its tracker. Never return control to the WF with live effects.
        if self.tracker.settle(&self.cancel).await
            && !result.as_ref().is_err_and(Failure::uncertain)
        {
            result = Err(Failure::Uncertain(
                "Activity has an uncertain service result".into(),
            ));
            execution.observation.settled(&result);
        }
        execution.finished = true;
        result
    }
    async fn run_step<A: Activity>(
        &mut self,
        step: &mut A,
        policy: RetryPolicy,
        observation: &ActivityObservation,
    ) -> Outcome<A::Output> {
        let mut retries = 0;
        loop {
            tokio::task::yield_now().await;
            self.wait_observed(Some(observation)).await?;
            observation.started();
            let generation = self.control().request().generation;
            let mut scope = ActivityScope {
                context: self,
                observation,
                terminal_error: None,
            };
            let result = step.execute(&mut scope).await;
            let result = match scope.terminal_error {
                Some(error) => Err(error),
                None => result,
            };
            if let Err(error) = self.tracker.ready() {
                let uncertain = self.tracker.settle(&self.cancel).await;
                return Err(if uncertain {
                    Failure::Uncertain("Activity has an uncertain service result".into())
                } else {
                    error
                });
            }
            observation.update(|entry| entry.last_error = result.as_ref().err().cloned());
            if let Err(error @ (Failure::Cancelled | Failure::Uncertain(_))) = result {
                return Err(error);
            }
            let action = match step.handle(result) {
                Err(Failure::Cancelled) => ActivityAction::Cancel,
                action => action?,
            };
            match action {
                ActivityAction::Continue(value) => return Ok(value),
                ActivityAction::Retry { reason } => {
                    observation.update(|entry| entry.retry_reason = Some(reason.clone()));
                    if retries == policy.max_retries {
                        return Err(Failure::Failed(format!(
                            "Activity retry limit exhausted ({} retries): {reason}",
                            policy.max_retries
                        )));
                    }
                    retries += 1;
                    let delay = policy.delay(retries);
                    let retry_at = SystemTime::now()
                        .checked_add(delay)
                        .ok_or_else(|| Failure::Rejected("retry deadline overflow".into()))?;
                    observation.update(|entry| {
                        entry.retries = retries;
                        entry.retry_at = Some(retry_at);
                    });
                    self.sleep_observed(delay, Some(observation)).await?;
                    observation.update(|entry| entry.retry_at = None);
                }
                ActivityAction::Suspend(ActivityWait::Resume) => {
                    self.check_execution()?;
                    let request = self.control().request();
                    if !request.paused() && request.generation == generation {
                        return Err(Failure::Rejected(
                            "Suspend(Resume) requires a Task pause".into(),
                        ));
                    }
                }
                ActivityAction::Suspend(ActivityWait::Condition(condition)) => {
                    self.wait_for_activity_condition(condition, observation)
                        .await?;
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
    async fn wait_for_activity_condition(
        &mut self,
        condition: Arc<dyn RunCondition>,
        observation: &ActivityObservation,
    ) -> Outcome<()> {
        let mut control = self.control();
        loop {
            self.wait_observed(Some(observation)).await?;
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
                    observation.state(ActivityState::WaitingConditions(reason.clone()));
                    self.state.send_replace(RunState::WaitingConditions(reason));
                    tokio::select! {
                        changed = control.changed() => { changed?; },
                        _ = self.controls.recheck.notified() => {},
                    }
                }
            }
        }
    }
}
