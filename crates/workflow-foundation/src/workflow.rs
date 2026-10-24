mod control;
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
    /// Optional initial business observation, independent of durable recovery state.
    fn details(&self) -> Outcome<serde_json::Value> {
        Ok(serde_json::Value::Null)
    }
    /// Persistence is declared here, never inferred from whether a store happens to exist.
    fn persistence(&self) -> Outcome<Persistence> {
        Ok(Persistence::Memory)
    }
    /// Construct this same workflow definition on another host from durable business data.
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
    fn continuation_guard(&self) -> Option<Arc<dyn ContinuationGuard>> {
        None
    }
    async fn run(self, context: &mut WorkflowContext) -> Outcome<Self::Output>;
}

pub struct Run<T> {
    pub control: RunControl,
    result: oneshot::Receiver<Outcome<T>>,
    consumed: bool,
    accepted: watch::Receiver<Option<Outcome<()>>>,
}
impl<T> Run<T> {
    pub fn id(&self) -> WorkflowId {
        self.control.id
    }
    /// Confirms durable acceptance (or immediate in-memory registration). Driver owners
    /// must keep polling tick() while waiting; application submit APIs await this for you.
    pub async fn accepted(&mut self) -> Outcome<()> {
        loop {
            if let Some(result) = self.accepted.borrow_and_update().clone() {
                return result;
            }
            self.accepted
                .changed()
                .await
                .map_err(|_| Failure::Unavailable)?;
        }
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
    cancel: CancellationToken,
    signals: mpsc::Sender<Signal>,
    state: watch::Receiver<RunState>,
    max_signal_bytes: usize,
    controls: crate::control::Controls,
    durable: bool,
    acceptance: watch::Receiver<Option<Outcome<()>>>,
    pub(crate) info: watch::Sender<TaskInfo>,
    pub(crate) progress: watch::Receiver<TaskProgress>,
    pub(crate) activities: watch::Receiver<ActivityHistory>,
    pub(crate) objects: Arc<Mutex<Vec<ObjectKey>>>,
}
impl RunControl {
    /// None while durable registration is pending. Not a workflow completion result.
    pub fn acceptance_status(&self) -> Option<Outcome<()>> {
        self.acceptance.borrow().clone()
    }
    /// Request a pause at the next safe boundary, not preemption of external IO.
    pub fn pause(&self) -> Outcome<()> {
        self.set_pause(true)
    }
    pub fn resume(&self) -> Outcome<()> {
        self.set_pause(false)
    }
    fn set_pause(&self, value: bool) -> Outcome<()> {
        if self.cancel.is_cancelled()
            || matches!(self.state(), RunState::Completed | RunState::Failed(_))
        {
            return Err(Failure::Rejected("run is cancelled or terminal".into()));
        }
        self.controls.update(|reasons| {
            if value {
                reasons.insert("user".into());
            } else {
                reasons.remove("user");
            }
        });
        Ok(())
    }
    pub fn set_blockers(&self, blockers: Vec<TaskId>) {
        self.info.send_if_modified(|info| {
            if info.blockers == blockers {
                false
            } else {
                info.blockers = blockers;
                true
            }
        });
    }
    pub fn explain(&self, conflicts: Vec<ConflictMatch>, blockers: Vec<TaskId>) {
        self.info.send_modify(|info| {
            info.conflicts = conflicts;
            info.blockers = blockers;
        });
    }
    pub fn progress(&self) -> TaskProgress {
        self.progress.borrow().clone()
    }
    pub fn activities(&self) -> ActivityHistory {
        let mut history = self.activities.borrow().clone();
        if self.pause_acknowledged() {
            for e in &mut history.entries {
                if e.state == ActivityState::Running {
                    e.state = ActivityState::Paused;
                }
            }
        }
        history
    }
    pub fn pause_requested(&self) -> bool {
        self.controls.tx.borrow().paused()
    }
    pub fn pause_acknowledged(&self) -> bool {
        !self.cancel.is_cancelled()
            && !matches!(
                self.state.borrow().clone(),
                RunState::Completed | RunState::Failed(_)
            )
            && self.controls.acknowledged()
    }
    pub fn pause_reasons(&self) -> std::collections::BTreeSet<String> {
        self.controls.reasons()
    }
    pub fn set_policy_pauses(&self, reasons: &std::collections::BTreeSet<String>) {
        self.controls.update(|current| {
            current.retain(|s| !s.starts_with("policy:"));
            current.extend(reasons.iter().map(|s| format!("policy:{s}")));
        });
    }
    pub fn recheck(&self) {
        self.controls.recheck.notify_one();
    }
    /// Cancellation-safe wait for acknowledgement, not merely request delivery.
    pub async fn wait_paused(&self) -> Outcome<()> {
        let mut state = self.state.clone();
        let mut request = self.controls.tx.subscribe();
        loop {
            if self.cancel.is_cancelled() {
                return Err(Failure::Cancelled);
            }
            if !request.borrow_and_update().paused() {
                return Err(Failure::Rejected("pause request is not active".into()));
            }
            match state.borrow_and_update().clone() {
                RunState::Paused if self.pause_acknowledged() => return Ok(()),
                RunState::Completed | RunState::Failed(_) => {
                    return Err(Failure::Rejected(
                        "run ended before pause acknowledgement".into(),
                    ));
                }
                _ => {}
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return Err(Failure::Cancelled),
                _ = self.controls.changed.notified() => {},
                changed = state.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
                changed = request.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
            }
        }
    }
    pub fn cancel(&self) {
        self.controls
            .user_cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.controls.revoke();
        if self.durable {
            self.controls.tx.send_modify(|r| r.generation += 1);
        } else {
            self.cancel.cancel();
        }
    }
    pub fn cancellation_requested(&self) -> bool {
        self.cancel.is_cancelled()
            || self
                .controls
                .user_cancelled
                .load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn state(&self) -> RunState {
        if self.pause_acknowledged() {
            RunState::Paused
        } else {
            self.state.borrow().clone()
        }
    }
    pub async fn changed(&mut self) -> Outcome<RunState> {
        tokio::select! {
            r = self.state.changed() => { r.map_err(|_| Failure::Unavailable)?; },
            _ = self.controls.changed.notified() => {},
        }
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
    pub(crate) cancel: CancellationToken,
    signals: mpsc::Receiver<Signal>,
    // Accepted execution lifetime is independent of the last public Run handle.
    _signal_sender: mpsc::Sender<Signal>,
    state: watch::Sender<RunState>,
    tracker: Tracker,
    info: watch::Sender<TaskInfo>,
    progress: watch::Sender<TaskProgress>,
    activities: watch::Sender<ActivityHistory>,
    activity_history: usize,
    objects: Arc<Mutex<Vec<ObjectKey>>>,
    next_activity: u64,
    pause: watch::Receiver<ControlRequest>,
    controls: crate::control::Controls,
    continuation: Option<Arc<dyn ContinuationGuard>>,
    next_control: Option<Arc<dyn ActivityControl>>,
    journal: Option<crate::persistence::Journal>,
    restored_checkpoint: serde_json::Value,
}
impl WorkflowContext {
    pub fn id(&self) -> WorkflowId {
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
    async fn persist_intent(&self) -> Outcome<()> {
        sync_controls(&self.journal, &self.controls, &self.cancel).await
    }
    pub fn checkpoint(&self) -> Outcome<()> {
        if self.cancel.is_cancelled() || self.scope.is_closed() {
            Err(Failure::Cancelled)
        } else {
            self.tracker.ready()
        }
    }
    /// Cooperative boundary for custom computation; checkpoint() only checks cancel.
    pub async fn safe_point(&mut self) -> Outcome<()> {
        self.checkpoint()?;
        let cancel = self.cancel.clone();
        self.continue_when_ready(&cancel).await?;
        self.state.send_replace(RunState::Running);
        Ok(())
    }
    pub async fn next_signal(&mut self) -> Outcome<Signal> {
        loop {
            self.safe_point().await?;
            self.state.send_replace(RunState::WaitingSignal);
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(Failure::Cancelled),
                changed = self.pause.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
                signal = self.signals.recv() => {
                    let signal = signal.ok_or(Failure::Unavailable)?;
                    self.safe_point().await?;
                    return Ok(signal);
                }
            }
        }
    }
    pub async fn sleep(&mut self, delay: Duration) -> Outcome<()> {
        let deadline = tokio::time::Instant::now() + delay;
        loop {
            self.safe_point().await?;
            self.state.send_replace(RunState::WaitingTimer);
            tokio::select! {
                biased;
                _ = self.cancel.cancelled() => return Err(Failure::Cancelled),
                changed = self.pause.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
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
        let override_hook = self.next_control.take();
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
        );
        let permit: Outcome<Permit> = async {
            loop {
                self.safe_point().await?;
                self.state.send_replace(RunState::WaitingResources);
                let broker = self.broker.clone();
                tokio::select! {
                    biased;
                    changed = self.pause.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
                    permit = broker.acquire(Kind::Activity, resources.clone(), &self.cancel) => {
                        let permit = permit?;
                        if self.pause.borrow().paused() { drop(permit); continue; }
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
        let guard = self.tracker.register(permit, observation);
        self.state.send_replace(RunState::Activity(id));
        let cancel = self.cancel.child_token();
        let (hook_tx, hooks) = watch::channel(None);
        let operation = service.dispatch(
            request,
            ActivityContext {
                id,
                cancel: cancel.clone(),
                control: self.controls.token(cancel.clone()),
                hooks: hook_tx,
            },
            guard,
        );
        let result = self
            .drive_activity(id, cancel, hooks, override_hook, operation)
            .await;
        self.state.send_replace(RunState::Running);
        result
    }
}

struct Completion<T> {
    reply: Option<oneshot::Sender<Outcome<T>>>,
    state: watch::Sender<RunState>,
    accepted: watch::Sender<Option<Outcome<()>>>,
}
impl<T> Completion<T> {
    fn finish(&mut self, result: Outcome<T>) {
        self.accepted.send_if_modified(|accepted| {
            if accepted.is_none() {
                *accepted = Some(result.as_ref().map(|_| ()).map_err(Clone::clone));
                true
            } else {
                false
            }
        });
        self.state.send_replace(match &result {
            Ok(_) => RunState::Completed,
            Err(e) => RunState::Failed(e.clone()),
        });
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
    pub wake: Arc<Notify>,
    pub scope: Arc<Scope>,
    pub pool: String,
    pub broker: Broker,
    pub cancel: CancellationToken,
    pub config: Config,
    pub start: Option<oneshot::Receiver<Outcome<()>>>,
    pub journal: Option<crate::persistence::Journal>,
    pub restored: Option<crate::persistence::SavedTask>,
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
        wake,
        scope,
        pool,
        broker,
        cancel,
        config,
        start,
        journal,
        restored,
    } = env;
    let (reply, result) = oneshot::channel();
    let (accepted, accepted_rx) = watch::channel(None);
    let (signals, signal_rx) = mpsc::channel(config.capacity);
    let (state, state_rx) = watch::channel(RunState::WaitingObjects);
    let controls = crate::control::Controls::new(wake);
    if restored.as_ref().is_some_and(|r| r.user_paused) {
        controls.update(|r| {
            r.insert("user".into());
        });
    }
    if restored.as_ref().is_some_and(|r| r.cancel_requested) {
        controls
            .user_cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        cancel.cancel();
    }
    let pause_rx = controls.tx.subscribe();
    let info = watch::channel(TaskInfo::default()).0;
    let (progress, progress_rx) = watch::channel(TaskProgress::default());
    let (activities, activities_rx) = watch::channel(ActivityHistory::default());
    let objects = Arc::new(Mutex::new(Vec::new()));
    let control = RunControl {
        id,
        name: W::NAME,
        cancel: cancel.clone(),
        signals: signals.clone(),
        state: state_rx,
        max_signal_bytes: config.max_signal_bytes,
        controls: controls.clone(),
        durable: journal.is_some(),
        acceptance: accepted_rx.clone(),
        progress: progress_rx,
        info: info.clone(),
        activities: activities_rx,
        objects: objects.clone(),
    };
    let mut completion = Completion {
        reply: Some(reply),
        state: state.clone(),
        accepted: accepted.clone(),
    };
    let mut context = WorkflowContext {
        id,
        scope,
        pool,
        broker,
        cancel,
        signals: signal_rx,
        _signal_sender: signals,
        state,
        tracker: Tracker::default(),
        progress,
        info,
        activities,
        activity_history: config.activity_history,
        objects,
        next_activity: 0,
        pause: pause_rx,
        controls,
        continuation: None,
        next_control: None,
        journal,
        restored_checkpoint: restored
            .as_ref()
            .map(|r| r.checkpoint.clone())
            .unwrap_or_default(),
    };
    let future = Box::pin(async move {
        // Include declaration/constructor panics in the workflow boundary.
        let mut ownership = None;
        let mut journal_started = restored.is_some();
        let result = AssertUnwindSafe(async {
            if restored.is_none() && let Some(journal) = &context.journal {
                journal.create().await?;
                journal_started = true;
            }
            accepted.send_replace(Some(Ok(())));
            context.controls.wake.notify_one();
            context.continuation = workflow.continuation_guard();
            context.report_details(
                &restored
                    .as_ref()
                    .map(|r| Ok(r.details.clone()))
                    .unwrap_or_else(|| workflow.details())?,
            )?;
            let declared = workflow.objects();
            *context.objects.lock().unwrap() = declared.clone();
            for object in &declared {
                object.validate()?;
            }
            if let Some(start) = start {
                // The application owns logical objects for a managed run.
                tokio::pin!(start);
                let mut requests = context.controls.tx.subscribe();
                loop {
                    context.persist_intent().await?;
                    tokio::select! {
                        biased;
                        _ = context.cancel.cancelled() => return Err(Failure::Cancelled),
                        _ = requests.changed() => {},
                        allowed = &mut start => { allowed.unwrap_or(Err(Failure::Unavailable))?; break; },
                    }
                }
            } else {
                // Standalone runtime use retains low-level ownership mechanics.
                let objects: Vec<_> = declared.into_iter().map(ResourceAccess::write).collect();
                let mut requests = context.controls.tx.subscribe();
                loop {
                    context.persist_intent().await?;
                    tokio::select! {
                        biased;
                        _ = context.cancel.cancelled() => return Err(Failure::Cancelled),
                        _ = requests.changed() => {},
                        permit = context.broker.acquire(Kind::Workflow, objects.clone(), &context.cancel) => {
                            ownership = Some(permit?);
                            break;
                        }
                    }
                }
            }
            context.safe_point().await?;
            let journal = context.journal.clone();
            let controls = context.controls.clone();
            let cancel = context.cancel.clone();
            let mut requests = controls.tx.subscribe();
            let body = workflow.run(&mut context);
            tokio::pin!(body);
            // Poll persistence alongside the body. Awaiting it inside a select branch
            // can deadlock if the body owns the journal lock across a blocking write.
            let updates_stop = CancellationToken::new();
            let updates = async {
                loop {
                    sync_controls(&journal, &controls, &cancel).await?;
                    tokio::select! {
                        _ = updates_stop.cancelled() => return Ok::<(), Failure>(()),
                        r = requests.changed() => r.map_err(|_| Failure::Unavailable)?,
                    }
                }
            };
            tokio::pin!(updates);
            let mut control_error = None;
            loop {
                tokio::select! {
                    biased;
                    result = &mut body => {
                        // Finish a dispatched write before the final CAS update.
                        updates_stop.cancel();
                        if control_error.is_none() {
                            control_error = updates.await.err().map(|error| Failure::Uncertain(
                                format!("control intent not persisted: {error}")
                            ));
                        }
                        break match control_error {
                            Some(error) if !result.as_ref().is_err_and(|e| e.uncertain()) => Err(error),
                            _ => result,
                        };
                    },
                    result = &mut updates, if control_error.is_none() => {
                        if let Err(error) = result {
                            control_error = Some(Failure::Uncertain(format!("control intent not persisted: {error}")));
                            cancel.cancel();
                        }
                    }
                }
            }
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(Failure::Uncertain("workflow panicked".into())));
        context.signals.close();
        context.state.send_replace(RunState::Settling);
        let uncertain = context.tracker.settle(&context.cancel).await;
        let mut result = if uncertain {
            Err(Failure::Uncertain(
                "activity outcome requires reconciliation".into(),
            ))
        } else {
            result
        };
        if let Some(journal) = &context.journal
            && journal_started
        {
            let user_cancelled = context
                .controls
                .user_cancelled
                .load(std::sync::atomic::Ordering::SeqCst);
            let outcome = match &result {
                Ok(_) => crate::persistence::StoredOutcome::Completed,
                Err(Failure::Cancelled) if !user_cancelled => {
                    crate::persistence::StoredOutcome::Pending
                }
                Err(Failure::Cancelled) => crate::persistence::StoredOutcome::Cancelled,
                Err(Failure::Uncertain(e)) => {
                    crate::persistence::StoredOutcome::NeedsCheck(e.clone())
                }
                Err(e) => crate::persistence::StoredOutcome::Failed(e.to_string()),
            };
            let details = context.info.borrow().details.clone();
            let paused = context.controls.reasons().contains("user");
            if let Err(error) = journal
                .update(|r| {
                    r.outcome = outcome;
                    r.details = details;
                    r.user_paused = paused;
                    r.cancel_requested = user_cancelled;
                })
                .await
            {
                result = Err(Failure::Uncertain(format!(
                    "task result not persisted: {error}"
                )));
            }
        }
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
            accepted: accepted_rx,
        },
        future,
    )
}

async fn sync_controls(
    journal: &Option<crate::persistence::Journal>,
    controls: &crate::control::Controls,
    cancel: &CancellationToken,
) -> Outcome<()> {
    loop {
        let request = controls.tx.borrow().clone();
        let paused = request.pause_reasons.contains("user");
        let cancelled = controls
            .user_cancelled
            .load(std::sync::atomic::Ordering::SeqCst);
        if let Some(journal) = journal {
            let changed = {
                let r = journal.record.lock().await;
                r.user_paused != paused || r.cancel_requested != cancelled
            };
            if changed {
                journal
                    .update(|r| {
                        r.user_paused = paused;
                        r.cancel_requested = cancelled;
                    })
                    .await?;
            }
        }
        if request.generation != controls.tx.borrow().generation {
            continue;
        }
        if cancelled {
            cancel.cancel();
        }
        return Ok(());
    }
}
