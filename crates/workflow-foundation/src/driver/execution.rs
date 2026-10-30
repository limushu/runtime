//! One Task execution: create handles, await admission, run the Workflow,
//! persist control intent, settle Activities, and save the final outcome.
use crate::{
    locks::{Broker, Kind, Permit},
    model::Scope,
    tasks::ActivityObservation,
    *,
};
use futures_util::FutureExt;
use std::{
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

pub struct Run<T> {
    pub control: RunControl,
    result: oneshot::Receiver<Outcome<T>>,
    consumed: bool,
    accepted: watch::Receiver<Option<Outcome<()>>>,
}
impl<T> Run<T> {
    pub fn id(&self) -> TaskId {
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
    pub(crate) fn mark_uncertain(&self) {
        self.state.lock().unwrap().uncertain = true;
    }
    pub(crate) fn ready(&self) -> Outcome<()> {
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
    pub(crate) fn register(
        &self,
        permit: Permit,
        observation: ActivityObservation,
    ) -> ActivityGuard {
        self.state.lock().unwrap().pending += 1;
        ActivityGuard {
            tracker: self.clone(),
            permit,
            uncertain: false,
            observation,
        }
    }
    pub(crate) async fn settle(&self, cancel: &CancellationToken) -> bool {
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
    pub id: TaskId,
    pub wake: Arc<Notify>,
    pub scope: Arc<Scope>,
    pub pool: String,
    pub broker: Broker,
    pub cancel: CancellationToken,
    pub shutdown: CancellationToken,
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
    futures_util::future::BoxFuture<'static, TaskId>,
) {
    let ExecutionEnv {
        id,
        wake,
        scope,
        pool,
        broker,
        cancel,
        shutdown,
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
    let info = watch::channel(TaskInfo {
        details: restored
            .as_ref()
            .map(|task| task.details.clone())
            .unwrap_or_default(),
        ..TaskInfo::default()
    })
    .0;
    let (progress, progress_rx) = watch::channel(TaskProgress::default());
    let (activities, activities_rx) = watch::channel(ActivityHistory::default());
    let objects = Arc::new(Mutex::new(
        restored
            .as_ref()
            .map(|task| task.objects.clone())
            .unwrap_or_default(),
    ));
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
        shutdown,
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
            context.continuation = workflow.run_condition();
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
            context.wait_until_runnable().await?;
            let journal = context.journal.clone();
            let controls = context.controls.clone();
            let cancel = context.cancel.clone();
            let mut requests = controls.tx.subscribe();
            let body = async {
                context.controls.entered.store(true, std::sync::atomic::Ordering::SeqCst);
                context.controls.wake.notify_one();
                workflow.run(&mut context).await
            };
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

pub(crate) async fn sync_controls(
    journal: &Option<crate::persistence::Journal>,
    controls: &crate::control::Controls,
    cancel: &CancellationToken,
) -> Outcome<()> {
    loop {
        let request = controls.tx.borrow().clone();
        let paused = request.suspend_reasons.contains("user");
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
