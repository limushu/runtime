//! Business-facing Workflow contract and context: Activity calls and safe boundaries.
//! Task execution and settlement live in driver/execution.rs; controls in control.rs.
use crate::{
    activity::{ActivityFnFactory, Invocation, Options, run_step},
    driver::execution::{Tracker, sync_controls},
    locks::{Broker, Kind, Permit},
    model::Scope,
    service::ActivityClient,
    tasks::ActivityObservation,
    *,
};
use async_trait::async_trait;
use futures_util::FutureExt;
use std::{
    any::{Any, TypeId},
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
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
    /// Ordinary tasks inherit host history recording. Durable explicitly opts
    /// into continuation; Memory opts out of all task-record persistence.
    fn persistence(&self) -> Outcome<Persistence> {
        Ok(Persistence::History)
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
        if !journal.record.lock().await.recoverable {
            return Err(Failure::Rejected(
                "history-only task does not support checkpoints".into(),
            ));
        }
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
    pub(crate) async fn persist_observation(&self) -> Outcome<()> {
        crate::driver::execution::sync_observation(
            &self.journal,
            &self.state,
            &self.progress,
            &self.info,
        )
        .await
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
                self.persist_observation().await?;
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
                    self.persist_observation().await?;
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
    ) -> ActivityCall<'a, P> {
        ActivityCall::new(move |options| {
            Box::pin(async move {
                let invocation = Invocation {
                    scope: service.scope.clone(),
                    service: service.name.to_string(),
                    name: (service.activity_name)(&request),
                    resources: service.resources(&request),
                    dispatch: Box::new(move |context, guard| {
                        Box::pin(service.dispatch(request, context, guard))
                    }),
                };
                self.execute_activity(invocation, options).await
            })
        })
    }
    /// Run a named step. Metadata, resources and retry budget are defined by the
    /// Activity; execution, observation and controls use the usual Activity path.
    pub fn run_activity<A: Activity>(
        &mut self,
        step: A,
    ) -> ActivityCall<'_, ActivityExit<A::Output, ()>> {
        ActivityCall::new(move |mut options| {
            Box::pin(async move {
                let name = step.name();
                options.name.get_or_insert(name);
                if options.description.is_none() {
                    options.description = step.description().map(str::to_owned);
                }
                let retry = step.retry();
                options.retry = retry;
                let history = self.activities.clone();
                let invocation =
                    Invocation::body(self.scope.clone(), name, step.resources(), move |scope| {
                        run_step(scope, step, retry, history)
                    });
                self.execute_activity(invocation, options).await
            })
        })
    }
    /// Function adapter for Activity, with optional result handling and retries.
    /// Use `|| function(args)` or `|scope| function(scope, args)` when calling
    /// managed services. Each attempt must own its arguments (Send + 'static),
    /// because an abandoned waiter must not destroy in-flight effects.
    pub fn activity_fn<T, F, Args: 'static>(&mut self, function: F) -> ActivityFn<'_, T>
    where
        T: Send + 'static,
        F: ActivityFnFactory<Args, T>,
    {
        ActivityFn::new(self, function)
    }
    /// Advanced body for business loops and typed suspension checkpoints.
    /// Declare the union of all service accesses up front.
    /// The body owns its captures so that dropping its waiter cannot drop live IO.
    /// Return ActivityExit::Suspended(checkpoint), then save and acknowledge in
    /// the Workflow just as for a direct service Activity.
    pub fn activity_body<T, F, Fut>(
        &mut self,
        name: &'static str,
        resources: Vec<ResourceAccess>,
        body: F,
    ) -> ActivityCall<'_, T>
    where
        T: Send + 'static,
        F: FnOnce(ActivityScope) -> Fut + Send + 'static,
        Fut: Future<Output = Outcome<T>> + Send + 'static,
    {
        ActivityCall::new(move |options| {
            Box::pin(async move {
                if name.trim().is_empty() || name.len() > 256 {
                    return Err(Failure::Rejected("invalid Activity name".into()));
                }
                let invocation = Invocation::body(self.scope.clone(), name, resources, body);
                self.execute_activity(invocation, options).await
            })
        })
    }
    pub(crate) async fn execute_activity<P: Send + 'static>(
        &mut self,
        invocation: Invocation<'_, P>,
        options: Options,
    ) -> Outcome<P> {
        options.validate()?;
        let uninterruptible = options.uninterruptible;
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
        if !Arc::ptr_eq(&self.scope, &invocation.scope) {
            return Err(Failure::Rejected(
                "service belongs to another Pool instance".into(),
            ));
        }
        let resources = invocation.resources;
        self.next_activity += 1;
        let id = ActivityId {
            task_id: self.id,
            sequence: self.next_activity,
        };
        let mut observation = ActivityObservation::new(
            self.activities.clone(),
            self.activity_history,
            id,
            invocation.service,
            options.name.unwrap_or(invocation.name),
            options.description,
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
        let operation = (invocation.dispatch)(
            ActivityContext {
                id,
                cancel: cancel.clone(),
                control,
                progress: self.progress.clone(),
                protected_controls: protected_controls.clone(),
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
