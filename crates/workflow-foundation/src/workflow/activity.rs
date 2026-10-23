//! In-flight control state machine. The service owns cancellation and real settlement;
//! this executor owns pause acknowledgements and the next-step gate.
use super::*;
use crate::{control::pause_result, tasks::ActivityReporter};
use std::future::Future;

enum Phase {
    Running,
    Pausing,
    Paused,
    Checking,
    Resuming,
    Settling,
}

impl WorkflowContext {
    pub(super) async fn drive_activity<P>(
        &mut self,
        id: ActivityId,
        mut token: ControlToken,
        mut hooks: watch::Receiver<Option<Arc<dyn ActivityPause>>>,
        reporter: ActivityReporter,
        operation: impl Future<Output = Outcome<P>>,
    ) -> Outcome<P> {
        tokio::pin!(operation);
        let mut phase = Phase::Running;
        let mut failure = None;
        let mut hooks_open = true;
        let mut checked_generation = None;
        let result = loop {
            // Real completion wins cancellation. Never abandon an issued operation.
            if let Some(result) = operation.as_mut().now_or_never() {
                break result;
            }
            if token.is_cancelled() {
                phase = Phase::Settling;
            }
            let hook = hooks.borrow().clone();
            match phase {
                Phase::Running if token.request().pause_requested() => {
                    phase = Phase::Pausing;
                    continue;
                }
                Phase::Running => {}
                Phase::Pausing => {
                    if !token.request().pause_requested() {
                        self.transition(RunState::Activity(id));
                        phase = Phase::Running;
                        continue;
                    }
                    self.transition(RunState::Pausing);
                    if let Some(hook) = hook {
                        reporter.state(ActivityState::Pausing);
                        match pause_result(hook.pause()).await {
                            Ok(()) => {
                                reporter.state(ActivityState::Paused);
                                phase = Phase::Paused;
                            }
                            Err(error) => {
                                failure = Some(error);
                                token.cancellation().cancel();
                                phase = Phase::Settling;
                            }
                        }
                        continue;
                    }
                    // No hook: wait for normal completion, never manufacture a pause ack.
                }
                Phase::Paused => {
                    let request = token.request();
                    if !request.pause_requested() {
                        phase = Phase::Checking;
                        continue;
                    }
                    if self.source.acknowledge_pause(request.generation, &token) {
                        self.transition(RunState::Paused);
                    }
                }
                Phase::Checking => {
                    match self.continue_when_ready(&token).await {
                        Ok(generation) => {
                            checked_generation = Some(generation);
                            phase = Phase::Resuming;
                        }
                        Err(error) => {
                            if error != Failure::Cancelled {
                                failure = Some(error);
                            }
                            token.cancellation().cancel();
                            phase = Phase::Settling;
                        }
                    }
                    continue;
                }
                Phase::Resuming => {
                    if token.request().pause_requested() {
                        phase = Phase::Paused;
                        continue;
                    }
                    if checked_generation != Some(token.request().generation) {
                        phase = Phase::Checking;
                        continue;
                    }
                    self.transition(RunState::Activity(id));
                    let hook = hook.expect("paused Activity retains its registered hook");
                    match pause_result(hook.resume()).await {
                        Ok(()) => {
                            reporter.state(ActivityState::Running);
                            phase = Phase::Running;
                        }
                        Err(error) => {
                            failure = Some(error);
                            token.cancellation().cancel();
                            phase = Phase::Settling;
                        }
                    }
                    continue;
                }
                Phase::Settling => {
                    self.transition(RunState::Settling);
                    break operation.await;
                }
            }
            tokio::select! {
                biased;
                result = &mut operation => break result,
                changed = token.changed() => {
                    if let Err(error) = changed {
                        if error != Failure::Cancelled { failure = Some(error); }
                        token.cancellation().cancel();
                        phase = Phase::Settling;
                    }
                }
                changed = hooks.changed(), if hooks_open => {
                    hooks_open = changed.is_ok();
                }
            }
        };
        self.transition(RunState::Running);
        let result = match failure {
            Some(error) => {
                if result.as_ref().is_err_and(|e| e.uncertain()) {
                    result
                } else {
                    Err(error)
                }
            }
            None => result,
        };
        if result.as_ref().is_err_and(|e| e.uncertain()) {
            self.tracker.mark_uncertain();
        }
        result
    }
}
