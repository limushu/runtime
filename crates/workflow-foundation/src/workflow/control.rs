use super::*;

impl WorkflowContext {
    pub fn control(&self) -> ControlToken {
        self.controls.token(self.cancel.clone())
    }

    /// Override the control adapter for the NEXT ordinary activity. Different workflows
    /// may send different domain commands without teaching the runtime their semantics.
    pub fn set_activity_control(&mut self, control: Arc<dyn ActivityControl>) {
        self.next_control = Some(control);
    }

    pub(super) async fn wait_for_readiness(&mut self, cancel: &CancellationToken) -> Outcome<()> {
        let mut token = self.controls.token(cancel.clone());
        loop {
            self.persist_intent().await?;
            if cancel.is_cancelled() {
                return Err(Failure::Cancelled);
            }
            let request = token.request();
            if request.paused() {
                if self.controls.acknowledge(request.generation, cancel) {
                    self.state.send_replace(RunState::Suspended);
                }
                token.changed().await?;
                continue;
            }
            self.controls.revoke();
            let Some(check) = self.continuation.clone() else {
                return Ok(());
            };
            let generation = request.generation;
            let decision = tokio::select! {
                biased;
                r = token.changed() => { r?; continue; },
                r = check.check() => r?,
            };
            if generation != token.request().generation {
                continue;
            }
            match decision {
                Readiness::Ready => return Ok(()),
                Readiness::Blocked(reason) => {
                    self.state.send_replace(RunState::WaitingConditions(reason));
                    tokio::select! {
                        r = token.changed() => { r?; },
                        _ = self.controls.recheck.notified() => {},
                    }
                }
            }
        }
    }

    pub(super) async fn drive_activity<P>(
        &mut self,
        id: ActivityId,
        cancel: CancellationToken,
        mut hooks: watch::Receiver<Option<Arc<dyn ActivityControl>>>,
        override_hook: Option<Arc<dyn ActivityControl>>,
        operation: impl std::future::Future<Output = Outcome<P>>,
    ) -> Outcome<P> {
        tokio::pin!(operation);
        let mut token = self.controls.token(cancel.clone());
        let mut stopped = false;
        let mut hook_open = true;
        let mut failure = None;
        let mut activity_events = self.activities.subscribe();
        loop {
            // Completion remains a business result. The caller may save a partial result
            // before wait_until_runnable(); the next Activity is always behind that same gate.
            if let Some(result) = operation.as_mut().now_or_never() {
                self.controls.revoke();
                return if result.as_ref().is_err_and(|e| e.uncertain()) {
                    result
                } else if let Some(error) = failure {
                    Err(error)
                } else {
                    result
                };
            }
            if cancel.is_cancelled() {
                let result = operation.await;
                self.controls.revoke();
                return if result.as_ref().is_err_and(|e| e.uncertain()) {
                    result
                } else if let Some(error) = failure {
                    Err(error)
                } else {
                    result
                };
            }
            if let Err(error) = self.persist_intent().await {
                failure = Some(Failure::Uncertain(format!(
                    "control intent not persisted: {error}"
                )));
                self.tracker.mark_uncertain();
                cancel.cancel();
                continue;
            }
            let request = token.request();
            let registered = hooks.borrow().clone();
            let started = activity_events
                .borrow()
                .entries
                .iter()
                .any(|e| e.id == id && e.started_at.is_some());
            let hook = if started {
                override_hook.clone().or(registered)
            } else {
                None
            };
            if request.paused() {
                if !stopped {
                    self.state.send_replace(RunState::Suspending);
                    if let Some(hook) = hook {
                        // A dispatched control command must settle even if requests change.
                        match AssertUnwindSafe(hook.suspend()).catch_unwind().await {
                            Ok(Ok(())) => stopped = true,
                            error => {
                                failure = Some(Failure::Uncertain(format!(
                                    "suspend not confirmed: {}",
                                    match error {
                                        Ok(Err(e)) => e.to_string(),
                                        _ => "panic".into(),
                                    }
                                )));
                                self.tracker.mark_uncertain();
                                cancel.cancel();
                            }
                        }
                        continue;
                    }
                } else if self.controls.acknowledge(request.generation, &cancel) {
                    self.state.send_replace(RunState::Suspended);
                }
            } else if stopped {
                // Keep the original operation alive while business readiness is checked.
                let checked = tokio::select! {
                    biased;
                    result = &mut operation => { self.controls.revoke(); return result; },
                    checked = AssertUnwindSafe(self.wait_for_readiness(&cancel)).catch_unwind() => checked,
                };
                match checked {
                    Ok(Ok(())) => {}
                    error => {
                        failure = Some(match error {
                            Ok(Err(e)) => e,
                            _ => Failure::Uncertain("continuation check panicked".into()),
                        });
                        if failure.as_ref().is_some_and(Failure::uncertain) {
                            self.tracker.mark_uncertain();
                        }
                        cancel.cancel();
                        continue;
                    }
                }
                if token.request().paused() {
                    continue;
                }
                self.controls.revoke();
                let hook = hook.expect("stopped activity retains its control adapter");
                match AssertUnwindSafe(hook.resume()).catch_unwind().await {
                    Ok(Ok(())) => {
                        stopped = false;
                        self.state.send_replace(RunState::Activity(id));
                    }
                    error => {
                        failure = Some(Failure::Uncertain(format!(
                            "resume not confirmed: {}",
                            match error {
                                Ok(Err(e)) => e.to_string(),
                                _ => "panic".into(),
                            }
                        )));
                        self.tracker.mark_uncertain();
                        cancel.cancel();
                    }
                }
                continue;
            }
            tokio::select! {
                biased;
                result = &mut operation => { self.controls.revoke(); return result; },
                _ = token.changed() => {},
                _ = activity_events.changed() => {},
                r = hooks.changed(), if hook_open => { hook_open = r.is_ok(); },
            }
        }
    }
}
