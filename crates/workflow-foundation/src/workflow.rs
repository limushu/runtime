//! Business-facing Workflow contract and context: Activity calls and safe boundaries.
//! Task execution and settlement live in driver/execution.rs; controls in control.rs.
use crate::{
    driver::execution::{Tracker, sync_controls},
    locks::{Broker, Kind, Permit},
    model::Scope,
    service::ActivityClient,
    tasks::ActivityObservation,
    *,
};
use async_trait::async_trait;
use futures_util::{FutureExt, future::BoxFuture};
use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

/// Dependency binding is local to a host; clients and Futures are never serialized.
#[derive(Clone, Default)]
pub struct Dependencies(Arc<BTreeMap<TypeId, Arc<dyn Any + Send + Sync>>>);
impl Dependencies {
    pub fn with<T: Send + Sync + 'static>(mut self, value: T) -> Self {
        Arc::make_mut(&mut self.0).insert(TypeId::of::<T>(), Arc::new(value));
        self
    }
    pub fn get<T: Send + Sync + 'static>(&self) -> Outcome<Arc<T>> {
        self.0
            .get(&TypeId::of::<T>())
            .cloned()
            .and_then(|v| v.downcast().ok())
            .ok_or_else(|| {
                Failure::Rejected(format!(
                    "missing workflow dependency {}",
                    std::any::type_name::<T>()
                ))
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    Ready,
    Blocked(String),
}
/// Business-defined, side-effect-free check. It can use stage state shared with run().
#[async_trait]
pub trait RunCondition: Send + Sync + 'static {
    async fn check(&self) -> Outcome<Readiness>;
}

#[async_trait]
pub trait Workflow: Send + 'static {
    type Output: Send + 'static;
    const NAME: &'static str;
    /// Logical ownership for this execution, not every resource a step will touch.
    fn objects(&self) -> Vec<ObjectKey> {
        vec![]
    }
    /// Optional initial business observation, independent of durable recovery state.
    fn details(&self) -> Outcome<serde_json::Value> {
        Ok(serde_json::Value::Null)
    }
    /// Persistence is declared here, never inferred from whether a store happens to exist.
    fn persistence(&self) -> Outcome<Persistence> {
        Ok(Persistence::Memory)
    }
    /// Rebuild ONLY the execution body for a persisted Task, binding this host's clients.
    /// Driver restores Task identity, control state and checkpoint separately; this
    /// constructor must not start domain operations. Called during full preflight.
    fn restore(
        _record: &crate::persistence::SavedTask,
        _dependencies: &Dependencies,
    ) -> Outcome<Self>
    where
        Self: Sized,
    {
        Err(Failure::Rejected(
            "workflow does not define restoration".into(),
        ))
    }
    fn run_condition(&self) -> Option<Arc<dyn RunCondition>> {
        None
    }
    async fn run(self, context: &mut WorkflowContext) -> Outcome<Self::Output>;
}

/// Non-cloneable execution capability. All effects go through activity().
pub struct WorkflowContext {
    pub(crate) id: TaskId,
    pub(crate) scope: Arc<Scope>,
    pub(crate) pool: String,
    pub(crate) broker: Broker,
    pub(crate) cancel: CancellationToken,
    pub(crate) shutdown: CancellationToken,
    pub(crate) signals: mpsc::Receiver<Signal>,
    // Accepted execution lifetime is independent of the last public Run handle.
    pub(crate) _signal_sender: mpsc::Sender<Signal>,
    pub(crate) state: watch::Sender<RunState>,
    pub(crate) tracker: Tracker,
    pub(crate) info: watch::Sender<TaskInfo>,
    pub(crate) progress: watch::Sender<TaskProgress>,
    pub(crate) activities: watch::Sender<ActivityHistory>,
    pub(crate) activity_history: usize,
    pub(crate) objects: Arc<Mutex<Vec<ObjectKey>>>,
    pub(crate) next_activity: u64,
    pub(crate) pause: watch::Receiver<ControlRequest>,
    pub(crate) controls: crate::control::Controls,
    pub(crate) continuation: Option<Arc<dyn RunCondition>>,
    pub(crate) journal: Option<crate::persistence::Journal>,
    pub(crate) restored_checkpoint: serde_json::Value,
}
impl WorkflowContext {
    pub fn id(&self) -> TaskId {
        self.id
    }
    pub fn pool(&self) -> &str {
        &self.pool
    }
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancel
    }
    /// Observational only. Invalid/oversized reports are ignored, never failing business work.
    pub fn report_progress(&self, stage: impl Into<String>, completed: u64, total: Option<u64>) {
        let stage = stage.into();
        if stage.trim().is_empty() || stage.len() > 256 || total.is_some_and(|t| completed > t) {
            return;
        }
        self.progress.send_replace(TaskProgress {
            stage: Some(stage),
            completed,
            total,
        });
    }
    /// Replace typed business observation in this same task snapshot.
    pub fn report_details<T: serde::Serialize>(&self, details: &T) -> Outcome<()> {
        let value = serde_json::to_value(details).map_err(|e| Failure::Rejected(e.to_string()))?;
        if serde_json::to_vec(&value)
            .map_err(|e| Failure::Rejected(e.to_string()))?
            .len()
            > 65536
        {
            return Err(Failure::Rejected("task details exceed 64 KiB".into()));
        }
        self.info.send_modify(|info| info.details = value);
        Ok(())
    }
    pub fn restored_checkpoint(&self) -> &serde_json::Value {
        &self.restored_checkpoint
    }
    pub async fn save_checkpoint<T: serde::Serialize>(&mut self, checkpoint: &T) -> Outcome<()> {
        self.tracker.ready()?;
        let value =
            serde_json::to_value(checkpoint).map_err(|e| Failure::Rejected(e.to_string()))?;
        let journal = self
            .journal
            .as_ref()
            .ok_or_else(|| Failure::Rejected("workflow is not persistent".into()))?;
        let details = self.info.borrow().details.clone();
        journal
            .update(|r| {
                r.checkpoint = value.clone();
                r.details = details;
            })
            .await?;
        self.restored_checkpoint = value;
        Ok(())
    }
    pub(crate) async fn persist_intent(&self) -> Outcome<()> {
        sync_controls(&self.journal, &self.controls, &self.cancel).await
    }
    pub fn check_execution(&self) -> Outcome<()> {
        if self.cancel.is_cancelled() || self.scope.is_closed() {
            Err(Failure::Cancelled)
        } else {
            self.tracker.ready()
        }
    }
    /// Wait for suspension reasons to clear and the workflow's RunCondition to
    /// become Ready. All earlier activities must have settled at this boundary.
    pub async fn wait_until_runnable(&mut self) -> Outcome<()> {
        self.check_execution()?;
        let cancel = self.cancel.clone();
        self.wait_for_readiness(&cancel).await?;
        self.state.send_replace(RunState::Running);
        Ok(())
    }
    pub fn control(&self) -> ControlToken {
        self.controls.token(self.cancel.clone())
    }

    async fn wait_for_readiness(&mut self, cancel: &CancellationToken) -> Outcome<()> {
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

    pub async fn next_signal(&mut self) -> Outcome<Signal> {
        loop {
            self.wait_until_runnable().await?;
            self.state.send_replace(RunState::WaitingSignal);
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(Failure::Cancelled),
                changed = self.pause.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
                signal = self.signals.recv() => {
                    let signal = signal.ok_or(Failure::Unavailable)?;
                    self.wait_until_runnable().await?;
                    return Ok(signal);
                }
            }
        }
    }
    pub async fn sleep(&mut self, delay: Duration) -> Outcome<()> {
        let deadline = tokio::time::Instant::now() + delay;
        loop {
            self.wait_until_runnable().await?;
            self.state.send_replace(RunState::WaitingTimer);
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(Failure::Cancelled),
                changed = self.pause.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
                _ = tokio::time::sleep_until(deadline) => return self.wait_until_runnable().await,
            }
        }
    }
    /// Configure a single invocation; ordinary `.activity(...).await` is unchanged.
    pub fn activity<
        'a,
        R: Send + 'static,
        P: Send + 'static,
        Q: Send + 'static,
        V: Send + 'static,
    >(
        &'a mut self,
        service: &'a ActivityClient<R, P, Q, V>,
        request: R,
    ) -> ActivityCall<'a, R, P, Q, V> {
        ActivityCall {
            context: Some(self),
            service,
            request: Some(request),
            name: None,
            uninterruptible: false,
            future: None,
        }
    }
    async fn execute_activity<
        R: Send + 'static,
        P: Send + 'static,
        Q: Send + 'static,
        V: Send + 'static,
    >(
        &mut self,
        service: &ActivityClient<R, P, Q, V>,
        request: R,
        uninterruptible: bool,
        name: Option<&'static str>,
    ) -> Outcome<P> {
        if name.is_some_and(|name| name.trim().is_empty() || name.len() > 256) {
            return Err(Failure::Rejected("invalid Activity name".into()));
        }
        if uninterruptible {
            // Task control may already be requested. Mandatory work still runs;
            // host shutdown/fencing and resource safety are never bypassed.
            if self.scope.is_closed() {
                return Err(Failure::Cancelled);
            }
            self.tracker.ready()?;
            self.persist_intent().await?;
            self.controls.revoke();
        } else {
            self.wait_until_runnable().await?;
        }
        if !Arc::ptr_eq(&self.scope, &service.scope) {
            return Err(Failure::Rejected(
                "service belongs to another Pool instance".into(),
            ));
        }
        let resources = (service.resources)(&request);
        self.next_activity += 1;
        let id = ActivityId {
            task_id: self.id,
            sequence: self.next_activity,
        };
        let mut observation = ActivityObservation::new(
            self.activities.clone(),
            self.activity_history,
            id,
            service.name.to_string(),
            name.unwrap_or_else(|| (service.activity_name)(&request)),
            resources.clone(),
        );
        let permit: Outcome<Permit> = async {
            if uninterruptible {
                self.state.send_replace(RunState::WaitingResources);
                return self.broker.acquire(Kind::Activity, resources.clone(), &self.shutdown).await;
            }
            loop {
                self.wait_until_runnable().await?;
                self.state.send_replace(RunState::WaitingResources);
                let broker = self.broker.clone();
                tokio::select! {
                    biased;
                    changed = self.pause.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
                    permit = broker.acquire(Kind::Activity, resources.clone(), &self.cancel) => {
                        let permit = permit?;
                        if self.pause.borrow().paused() { drop(permit); continue; }
                        self.check_execution()?;
                        break Ok(permit);
                    }
                }
            }
        }
        .await;
        let permit = match permit {
            Ok(permit) => permit,
            Err(error) => {
                observation.settled(&Err::<(), _>(error.clone()));
                return Err(error);
            }
        };
        observation.queued();
        let guard = self.tracker.register(permit, observation);
        self.state.send_replace(RunState::Activity(id));
        let cancel = if uninterruptible {
            self.shutdown.child_token()
        } else {
            self.cancel.child_token()
        };
        // Keep the detached request sender alive until the service reply. The
        // domain must not see task pause/cancel through a second control path.
        let protected_controls =
            uninterruptible.then(|| crate::control::Controls::new(self.controls.wake.clone()));
        let control = protected_controls
            .as_ref()
            .unwrap_or(&self.controls)
            .token(cancel.clone());
        let operation = service.dispatch(
            request,
            ActivityContext {
                id,
                cancel: cancel.clone(),
                control,
                progress: self.progress.clone(),
            },
            guard,
        );
        let result = if uninterruptible {
            operation.await
        } else {
            self.drive_activity(cancel, operation).await
        };
        self.state.send_replace(RunState::Running);
        result
    }
    /// ActivityContext already carries the task's control token into the service.
    /// This loop observes the request but never invokes domain-specific control hooks.
    /// A pause is acknowledged only later, at wait_until_runnable(), after the
    /// caller has consumed the business reply and saved its checkpoint.
    async fn drive_activity<P>(
        &mut self,
        cancel: CancellationToken,
        operation: impl std::future::Future<Output = Outcome<P>>,
    ) -> Outcome<P> {
        tokio::pin!(operation);
        let mut token = self.controls.token(cancel.clone());
        let mut failure = None;
        let result = loop {
            // Preserve a completed business result, even when control races it.
            if let Some(result) = operation.as_mut().now_or_never() {
                break result;
            }
            if cancel.is_cancelled() {
                // The service must settle its operation before resources can be released.
                break operation.await;
            }
            if let Err(error) = self.persist_intent().await {
                failure = Some(Failure::Uncertain(format!(
                    "control intent not persisted: {error}"
                )));
                self.tracker.mark_uncertain();
                cancel.cancel();
                continue;
            }
            if token.request().paused() {
                self.state.send_replace(RunState::Suspending);
            }
            tokio::select! {
                biased;
                result = &mut operation => break result,
                _ = token.changed() => {},
            }
        };
        self.controls.revoke();
        if result.as_ref().is_err_and(Failure::uncertain) {
            result
        } else if let Some(error) = failure {
            Err(error)
        } else {
            result
        }
    }
}

/// Options for the existing Activity invocation, not a separate activity kind.
/// It remains a Future, including when used in select!/timeout.
#[must_use = "an Activity does nothing until awaited"]
pub struct ActivityCall<'a, R, P, Q, V> {
    context: Option<&'a mut WorkflowContext>,
    service: &'a ActivityClient<R, P, Q, V>,
    request: Option<R>,
    name: Option<&'static str>,
    uninterruptible: bool,
    future: Option<BoxFuture<'a, Outcome<P>>>,
}
impl<R, P, Q, V> ActivityCall<'_, R, P, Q, V> {
    /// Override the observation label for this service invocation.
    pub fn name(mut self, name: &'static str) -> Self {
        assert!(
            self.future.is_none(),
            "configure an Activity before polling it"
        );
        self.name = Some(name);
        self
    }
    /// Defer task cancellation and suspension through resource acquisition,
    /// dispatch and the actual reply, even if already requested before dispatch.
    /// Return the business result so the caller can save its checkpoint. The next
    /// ordinary boundary observes the unchanged task control request.
    /// Host shutdown, service failure and lost leadership are NOT masked.
    pub fn uninterruptible(mut self) -> Self {
        assert!(
            self.future.is_none(),
            "configure an Activity before polling it"
        );
        self.uninterruptible = true;
        self
    }
}
// The request moves into a separately pinned Future on the first poll. No field
// borrows another field, and the boxed Future itself never moves after polling.
impl<R, P, Q, V> Unpin for ActivityCall<'_, R, P, Q, V> {}
impl<R: Send + 'static, P: Send + 'static, Q: Send + 'static, V: Send + 'static> Future
    for ActivityCall<'_, R, P, Q, V>
{
    type Output = Outcome<P>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if this.future.is_none() {
            let context = this.context.take().expect("Activity already started");
            this.future = Some(Box::pin(context.execute_activity(
                this.service,
                this.request.take().unwrap(),
                this.uninterruptible,
                this.name,
            )));
        }
        this.future.as_mut().unwrap().as_mut().poll(cx)
    }
}
