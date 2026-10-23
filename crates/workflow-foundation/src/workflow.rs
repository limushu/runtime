use crate::audit::AuditEmitter;
use crate::control::ControlSource;
mod activity;
use crate::{
    locks::{Broker, Kind, Permit},
    model::Scope,
    service::ActivityClient,
    tasks::ActivityObservation,
    *,
};
use async_trait::async_trait;
use futures_util::FutureExt;
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait Workflow: Send + 'static {
    type Output: Send + 'static;
    const NAME: &'static str;
    /// Logical ownership for this execution, not every resource a step will touch.
    fn objects(&self) -> Vec<ObjectKey> {
        vec![]
    }
    fn continuation_guard(&self) -> Option<Arc<dyn ContinuationGuard>> {
        None
    }
    async fn run(self, context: &mut WorkflowContext) -> Outcome<Self::Output>;
}

pub struct Run<T> {
    pub control: RunControl,
    result: oneshot::Receiver<Outcome<T>>,
    consumed: bool,
}
impl<T> Run<T> {
    pub fn id(&self) -> WorkflowId {
        self.control.id
    }
    /// Cancellation-safe wait. Dropping the caller does not cancel the workflow.
    pub async fn result(&mut self) -> Outcome<T> {
        if self.consumed {
            return Err(Failure::Rejected("result already consumed".into()));
        }
        let result = (&mut self.result)
            .await
            .unwrap_or_else(|_| Err(Failure::Uncertain("workflow host disappeared".into())));
        self.consumed = true;
        result
    }
}
#[derive(Clone)]
pub struct RunControl {
    pub id: WorkflowId,
    pub name: &'static str,
    source: ControlSource,
    signals: mpsc::Sender<Signal>,
    state: watch::Receiver<RunState>,
    max_signal_bytes: usize,
    status: watch::Sender<RunState>,
    recheck: Arc<Notify>,
    pub(crate) progress: watch::Receiver<TaskProgress>,
    pub(crate) activities: watch::Receiver<ActivityHistory>,
    pub(crate) objects: Arc<Mutex<Vec<ObjectKey>>>,
    audit: AuditEmitter,
}
impl RunControl {
    /// Request a pause at the next safe boundary, not preemption of external IO.
    pub fn pause(&self) -> Outcome<()> {
        self.set_pause(true)
    }
    pub fn resume(&self) -> Outcome<()> {
        self.set_pause(false)
    }
    fn set_pause(&self, value: bool) -> Outcome<()> {
        self.ensure_controllable()?;
        self.source.set_pause(true, value)?;
        self.audit.emit(AuditEvent::Control {
            pause: Some(value),
            cancel: false,
        });
        self.publish_control();
        Ok(())
    }
    pub fn set_system_pause(&self, value: bool) -> Outcome<()> {
        self.ensure_controllable()?;
        if self.source.set_pause(false, value)? {
            self.audit.emit(AuditEvent::SystemPause(value));
            self.publish_control();
        }
        Ok(())
    }
    fn ensure_controllable(&self) -> Outcome<()> {
        if self.source.is_cancelled()
            || matches!(self.state(), RunState::Completed | RunState::Failed(_))
        {
            return Err(Failure::Rejected("run is cancelled or terminal".into()));
        }
        Ok(())
    }
    fn publish_control(&self) {
        if !self.pause_requested() && self.state() == RunState::Paused {
            self.status.send_replace(RunState::Running);
            self.audit.emit(AuditEvent::State(RunState::Running));
        } else {
            self.status.send_modify(|_| {});
        }
    }
    pub fn token(&self) -> ControlToken {
        self.source.token()
    }
    pub fn user_pause_requested(&self) -> bool {
        self.source.request().user_paused
    }
    pub fn system_pause_requested(&self) -> bool {
        self.source.request().system_paused
    }
    pub fn pause_acknowledged(&self) -> bool {
        self.state() == RunState::Paused && self.source.pause_acknowledged()
    }
    /// Wake a workflow waiting for a business precondition; never bypass its check.
    pub fn recheck(&self) {
        self.recheck.notify_one();
        self.status.send_modify(|_| {});
    }
    pub fn progress(&self) -> TaskProgress {
        self.progress.borrow().clone()
    }
    pub fn activities(&self) -> ActivityHistory {
        self.activities.borrow().clone()
    }
    pub fn pause_requested(&self) -> bool {
        self.source.request().pause_requested()
    }
    /// Cancellation-safe wait for acknowledgement, not merely request delivery.
    pub async fn wait_paused(&self) -> Outcome<()> {
        let mut state = self.state.clone();
        let mut token = self.source.token();
        let mut acknowledgements = self.source.acknowledgements();
        loop {
            if token.is_cancelled() {
                return Err(Failure::Cancelled);
            }
            if !token.request().pause_requested() {
                return Err(Failure::Rejected("pause request is not active".into()));
            }
            if self.pause_acknowledged() {
                return Ok(());
            }
            if matches!(
                *state.borrow_and_update(),
                RunState::Completed | RunState::Failed(_)
            ) {
                return Err(Failure::Rejected(
                    "run ended before pause acknowledgement".into(),
                ));
            }
            tokio::select! {
                changed = state.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
                changed = token.changed() => { changed?; }
                changed = acknowledgements.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
            }
        }
    }
    pub fn cancel(&self) {
        self.audit.emit(AuditEvent::Control {
            pause: None,
            cancel: true,
        });
        self.source.cancel();
        if self.state() == RunState::Paused {
            self.status.send_replace(RunState::Settling);
            self.audit.emit(AuditEvent::State(RunState::Settling));
        }
    }
    pub fn cancellation_requested(&self) -> bool {
        self.source.is_cancelled()
    }
    pub fn state(&self) -> RunState {
        self.state.borrow().clone()
    }
    pub async fn changed(&mut self) -> Outcome<RunState> {
        self.state
            .changed()
            .await
            .map_err(|_| Failure::Unavailable)?;
        Ok(self.state())
    }
    pub async fn signal(&self, signal: Signal) -> Outcome<()> {
        if signal.name.trim().is_empty()
            || signal.name.len().saturating_add(signal.payload.len()) > self.max_signal_bytes
        {
            return Err(Failure::Rejected("invalid or oversized signal".into()));
        }
        self.signals
            .send(signal)
            .await
            .map_err(|_| Failure::Unavailable)
    }
}

#[derive(Default)]
struct Tracking {
    pending: usize,
    uncertain: bool,
}
#[derive(Clone, Default)]
pub(crate) struct Tracker {
    state: Arc<Mutex<Tracking>>,
    changed: Arc<Notify>,
}
impl Tracker {
    fn mark_uncertain(&self) {
        self.state.lock().unwrap().uncertain = true;
    }
    fn ready(&self) -> Outcome<()> {
        let state = self.state.lock().unwrap();
        if state.uncertain {
            Err(Failure::Uncertain(
                "earlier activity needs reconciliation".into(),
            ))
        } else if state.pending != 0 {
            Err(Failure::Rejected(
                "previous activity has not settled".into(),
            ))
        } else {
            Ok(())
        }
    }
    fn register(&self, permit: Permit, observation: ActivityObservation) -> ActivityGuard {
        self.state.lock().unwrap().pending += 1;
        ActivityGuard {
            tracker: self.clone(),
            permit,
            uncertain: false,
            observation,
        }
    }
    async fn settle(&self, cancel: &CancellationToken) -> bool {
        if self.state.lock().unwrap().pending != 0 {
            cancel.cancel();
        }
        loop {
            let changed = self.changed.notified();
            {
                let state = self.state.lock().unwrap();
                if state.pending == 0 {
                    return state.uncertain;
                }
            }
            changed.await;
        }
    }
}
pub(crate) struct ActivityGuard {
    tracker: Tracker,
    permit: Permit,
    uncertain: bool,
    observation: ActivityObservation,
}
impl ActivityGuard {
    pub fn started(&mut self) {
        self.uncertain = true;
        self.permit.started();
        self.observation.started();
    }
    pub fn settled<T>(&mut self, result: &Outcome<T>) {
        self.uncertain = result.as_ref().is_err_and(|e| e.uncertain());
        self.permit.settled(result);
        self.observation.settled(result);
    }
}
impl Drop for ActivityGuard {
    fn drop(&mut self) {
        // Publish the final observation before waking a workflow settling its guards.
        self.observation.abandon();
        let mut state = self.tracker.state.lock().unwrap();
        state.pending -= 1;
        state.uncertain |= self.uncertain;
        drop(state);
        self.tracker.changed.notify_one();
    }
}

/// Non-cloneable execution capability. All effects go through activity().
pub struct WorkflowContext {
    pub(crate) id: WorkflowId,
    pub(crate) scope: Arc<Scope>,
    pub(crate) pool: String,
    pub(crate) broker: Broker,
    requests: ControlToken,
    source: ControlSource,
    signals: mpsc::Receiver<Signal>,
    // Accepted execution lifetime is independent of the last public Run handle.
    _signal_sender: mpsc::Sender<Signal>,
    state: watch::Sender<RunState>,
    tracker: Tracker,
    progress: watch::Sender<TaskProgress>,
    activities: watch::Sender<ActivityHistory>,
    activity_history: usize,
    objects: Arc<Mutex<Vec<ObjectKey>>>,
    next_activity: u64,
    audit: AuditEmitter,
    checkpoints: Option<Arc<dyn CheckpointWriter>>,
    wake: Arc<Notify>,
    recheck: Arc<Notify>,
    continuation: Option<Arc<dyn ContinuationGuard>>,
}
impl WorkflowContext {
    pub fn id(&self) -> WorkflowId {
        self.id
    }
    pub fn pool(&self) -> &str {
        &self.pool
    }
    pub fn control(&self) -> &ControlToken {
        &self.requests
    }
    /// Observational only. Invalid/oversized reports are ignored, never failing business work.
    pub fn report_progress(&self, stage: impl Into<String>, completed: u64, total: Option<u64>) {
        crate::tasks::report_progress(&self.progress, &self.audit, stage.into(), completed, total);
    }
    /// Persist a BUSINESS checkpoint; this does not serialize or replay the async future.
    /// A pause is acknowledged at the next safe_point, AFTER the caller saves settled work.
    pub async fn save_checkpoint(&self, checkpoint: Vec<u8>) -> Outcome<()> {
        self.tracker.ready()?;
        self.checkpoints
            .as_ref()
            .ok_or_else(|| Failure::Rejected("task has no checkpoint writer".into()))?
            .save(checkpoint)
            .await
    }
    fn transition(&self, state: RunState) {
        if *self.state.borrow() == state {
            return;
        }
        self.audit.emit(AuditEvent::State(state.clone()));
        self.state.send_replace(state);
        self.wake.notify_one();
    }
    pub fn checkpoint(&self) -> Outcome<()> {
        if self.requests.is_cancelled() || self.scope.is_closed() {
            Err(Failure::Cancelled)
        } else {
            self.tracker.ready()
        }
    }
    /// Cooperative boundary for custom computation; checkpoint() only checks cancel.
    pub async fn safe_point(&mut self) -> Outcome<()> {
        self.checkpoint()?;
        let token = self.requests.clone();
        self.continue_when_ready(&token).await?;
        self.transition(RunState::Running);
        Ok(())
    }
    async fn continue_when_ready(&mut self, token: &ControlToken) -> Outcome<u64> {
        let mut token = token.clone();
        loop {
            if token.is_cancelled() || self.scope.is_closed() {
                return Err(Failure::Cancelled);
            }
            while token.request().pause_requested() {
                let generation = token.request().generation;
                if self.source.acknowledge_pause(generation, &token) {
                    self.transition(RunState::Paused);
                }
                token.changed().await?;
            }
            let Some(guard) = self.continuation.clone() else {
                return Ok(token.request().generation);
            };
            let generation = token.request().generation;
            let decision = tokio::select! {
                biased;
                changed = token.changed() => { changed?; continue; }
                result = AssertUnwindSafe(guard.check()).catch_unwind() => {
                    result.unwrap_or_else(|_| Err(Failure::Uncertain("continuation check panicked".into())))?
                },
            };
            if token.request().generation != generation || token.request().pause_requested() {
                continue;
            }
            match decision {
                ContinueDecision::Ready => return Ok(generation),
                ContinueDecision::Wait(reason) => {
                    self.transition(RunState::WaitingConditions(reason));
                    tokio::select! {
                        changed = token.changed() => { changed?; }
                        _ = self.recheck.notified() => {}
                    }
                }
            }
        }
    }
    pub async fn next_signal(&mut self) -> Outcome<Signal> {
        let cancel = self.requests.cancellation().clone();
        loop {
            self.safe_point().await?;
            self.transition(RunState::WaitingSignal);
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Failure::Cancelled),
                changed = self.requests.changed() => { changed?; }
                signal = self.signals.recv() => {
                    let signal = signal.ok_or(Failure::Unavailable)?;
                    self.safe_point().await?;
                    return Ok(signal);
                }
            }
        }
    }
    pub async fn sleep(&mut self, delay: Duration) -> Outcome<()> {
        let cancel = self.requests.cancellation().clone();
        let deadline = tokio::time::Instant::now() + delay;
        loop {
            self.safe_point().await?;
            self.transition(RunState::WaitingTimer);
            tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err(Failure::Cancelled),
                changed = self.requests.changed() => { changed?; }
                _ = tokio::time::sleep_until(deadline) => return self.safe_point().await,
            }
        }
    }
    pub async fn activity<
        R: Send + 'static,
        P: Send + 'static,
        Q: Send + 'static,
        V: Send + 'static,
    >(
        &mut self,
        service: &ActivityClient<R, P, Q, V>,
        request: R,
    ) -> Outcome<P> {
        self.safe_point().await?;
        if !Arc::ptr_eq(&self.scope, &service.scope) {
            return Err(Failure::Rejected(
                "service belongs to another Pool instance".into(),
            ));
        }
        let resources = (service.resources)(&request);
        self.next_activity += 1;
        let id = ActivityId {
            workflow: self.id,
            sequence: self.next_activity,
        };
        let mut observation = ActivityObservation::new(
            self.activities.clone(),
            self.activity_history,
            id,
            service.name.to_string(),
            (service.activity_name)(&request),
            resources.clone(),
            self.audit.clone(),
        );
        let cancel = self.requests.cancellation().clone();
        let permit: Outcome<Permit> = async {
            loop {
                self.safe_point().await?;
                self.transition(RunState::WaitingResources);
                let broker = self.broker.clone();
                tokio::select! {
                    biased;
                    changed = self.requests.changed() => { changed?; }
                    permit = broker.acquire(Kind::Activity, resources.clone(), &cancel) => {
                        let permit = permit?;
                        if self.requests.request().pause_requested() { drop(permit); continue; }
                        self.checkpoint()?;
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
        let reporter = observation.reporter();
        let guard = self.tracker.register(permit, observation);
        self.transition(RunState::Activity(id));
        let (pause_hook, current_hook) = watch::channel(None);
        let token = self.requests.child();
        let operation = service.dispatch(
            request,
            ActivityContext {
                id,
                control: token.clone(),
                pause_hook,
                progress: self.progress.clone(),
                audit: self.audit.clone(),
            },
            guard,
        );
        self.drive_activity(id, token, current_hook, reporter, operation)
            .await
    }
}

struct Completion<T> {
    reply: Option<oneshot::Sender<Outcome<T>>>,
    state: watch::Sender<RunState>,
    audit: AuditEmitter,
}
impl<T> Completion<T> {
    fn finish(&mut self, result: Outcome<T>) {
        let state = match &result {
            Ok(_) => RunState::Completed,
            Err(e) => RunState::Failed(e.clone()),
        };
        self.audit.emit(AuditEvent::State(state.clone()));
        self.state.send_replace(state);
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(result);
        }
    }
}
impl<T> Drop for Completion<T> {
    fn drop(&mut self) {
        if self.reply.is_some() {
            self.finish(Err(Failure::Uncertain(
                "workflow forcefully terminated".into(),
            )));
        }
    }
}

pub(crate) struct ExecutionEnv {
    pub id: WorkflowId,
    pub scope: Arc<Scope>,
    pub pool: String,
    pub broker: Broker,
    pub cancel: CancellationToken,
    pub config: Config,
    pub start: Option<oneshot::Receiver<Outcome<()>>>,
    pub options: ExecutionOptions,
    pub wake: Arc<Notify>,
}
pub(crate) fn execution<W: Workflow>(
    workflow: W,
    env: ExecutionEnv,
) -> (
    Run<W::Output>,
    futures_util::future::BoxFuture<'static, WorkflowId>,
) {
    let ExecutionEnv {
        id,
        scope,
        pool,
        broker,
        cancel,
        config,
        start,
        options,
        wake,
    } = env;
    let audit = AuditEmitter {
        workflow: id,
        sink: options.audit,
    };
    audit.emit(AuditEvent::Registered { name: W::NAME });
    let (reply, result) = oneshot::channel();
    let (signals, signal_rx) = mpsc::channel(config.capacity);
    let (state, state_rx) = watch::channel(RunState::WaitingObjects);
    let source = ControlSource::new(cancel, wake.clone());
    let token = source.token();
    let (progress, progress_rx) = watch::channel(TaskProgress::default());
    let (activities, activities_rx) = watch::channel(ActivityHistory::default());
    let objects = Arc::new(Mutex::new(Vec::new()));
    let recheck = Arc::new(Notify::new());
    let control = RunControl {
        id,
        name: W::NAME,
        source: source.clone(),
        signals: signals.clone(),
        state: state_rx,
        max_signal_bytes: config.max_signal_bytes,
        status: state.clone(),
        recheck: recheck.clone(),
        progress: progress_rx,
        activities: activities_rx,
        objects: objects.clone(),
        audit: audit.clone(),
    };
    let mut completion = Completion {
        reply: Some(reply),
        state: state.clone(),
        audit: audit.clone(),
    };
    let mut context = WorkflowContext {
        id,
        scope,
        pool,
        broker,
        requests: token,
        source,
        signals: signal_rx,
        _signal_sender: signals,
        state,
        tracker: Tracker::default(),
        progress,
        activities,
        activity_history: config.activity_history,
        objects,
        next_activity: 0,
        audit,
        checkpoints: options.checkpoints,
        wake,
        recheck,
        continuation: None,
    };
    let future = Box::pin(async move {
        // Include declaration/constructor panics in the workflow boundary.
        let mut ownership = None;
        let result = AssertUnwindSafe(async {
            context.continuation = workflow.continuation_guard();
            let declared = workflow.objects();
            *context.objects.lock().unwrap() = declared.clone();
            for object in &declared {
                object.validate()?;
            }
            if let Some(start) = start {
                // The application owns logical objects for a managed run.
                tokio::select! {
                    biased;
                    _ = context.requests.cancelled() => return Err(Failure::Cancelled),
                    allowed = start => allowed.unwrap_or(Err(Failure::Unavailable))?,
                }
            } else {
                // Standalone runtime use retains low-level ownership mechanics.
                let objects = declared.into_iter().map(ResourceAccess::write).collect();
                ownership = Some(
                    context
                        .broker
                        .acquire(Kind::Workflow, objects, context.requests.cancellation())
                        .await?,
                );
            }
            context.safe_point().await?;
            workflow.run(&mut context).await
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(Failure::Uncertain("workflow panicked".into())));
        context.signals.close();
        context.transition(RunState::Settling);
        let uncertain = context
            .tracker
            .settle(context.requests.cancellation())
            .await;
        let result = if uncertain {
            Err(Failure::Uncertain(
                "activity outcome requires reconciliation".into(),
            ))
        } else {
            result
        };
        if let Some(mut permit) = ownership {
            permit.settled(&result);
        }
        completion.finish(result);
        id
    });
    (
        Run {
            control,
            result,
            consumed: false,
        },
        future,
    )
}
