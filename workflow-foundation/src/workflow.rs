use crate::{
    locks::{Broker, Kind, Permit},
    model::Scope,
    service::{DomainService, ServiceClient},
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
    cancel: CancellationToken,
    signals: mpsc::Sender<Signal>,
    state: watch::Receiver<RunState>,
    max_signal_bytes: usize,
}
impl RunControl {
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    pub fn cancellation_requested(&self) -> bool {
        self.cancel.is_cancelled()
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
    fn register(&self, permit: Permit) -> ActivityGuard {
        self.state.lock().unwrap().pending += 1;
        ActivityGuard {
            tracker: self.clone(),
            permit,
            uncertain: false,
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
}
impl ActivityGuard {
    pub fn started(&mut self) {
        self.uncertain = true;
        self.permit.started();
    }
    pub fn settled<T>(&mut self, result: &Outcome<T>) {
        self.uncertain = result.as_ref().is_err_and(|e| e.uncertain());
        self.permit.settled(result);
    }
}
impl Drop for ActivityGuard {
    fn drop(&mut self) {
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
    next_activity: u64,
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
    pub fn checkpoint(&self) -> Outcome<()> {
        if self.cancel.is_cancelled() || self.scope.is_closed() {
            Err(Failure::Cancelled)
        } else {
            self.tracker.ready()
        }
    }
    pub async fn next_signal(&mut self) -> Outcome<Signal> {
        self.checkpoint()?;
        self.state.send_replace(RunState::WaitingSignal);
        let result = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(Failure::Cancelled),
            signal = self.signals.recv() => signal.ok_or(Failure::Unavailable),
        };
        self.state.send_replace(RunState::Running);
        result
    }
    pub async fn sleep(&mut self, delay: Duration) -> Outcome<()> {
        self.checkpoint()?;
        self.state.send_replace(RunState::WaitingTimer);
        let result = tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(Failure::Cancelled),
            _ = tokio::time::sleep(delay) => Ok(()),
        };
        self.state.send_replace(RunState::Running);
        result
    }
    pub async fn activity<S: DomainService>(
        &mut self,
        service: &ServiceClient<S>,
        request: S::Request,
    ) -> Outcome<S::Reply> {
        self.checkpoint()?;
        if !Arc::ptr_eq(&self.scope, &service.scope) {
            return Err(Failure::Rejected(
                "service belongs to another Pool instance".into(),
            ));
        }
        let resources = S::resources(&request);
        self.state.send_replace(RunState::WaitingResources);
        let permit = self
            .broker
            .acquire(Kind::Activity, resources, &self.cancel)
            .await?;
        self.checkpoint()?;
        self.next_activity += 1;
        let id = ActivityId {
            workflow: self.id,
            sequence: self.next_activity,
        };
        let guard = self.tracker.register(permit);
        self.state.send_replace(RunState::Activity(id));
        let result = service
            .dispatch(
                request,
                ActivityContext {
                    id,
                    cancel: self.cancel.child_token(),
                },
                guard,
            )
            .await;
        self.state.send_replace(RunState::Running);
        result
    }
}

struct Completion<T> {
    reply: Option<oneshot::Sender<Outcome<T>>>,
    state: watch::Sender<RunState>,
}
impl<T> Completion<T> {
    fn finish(&mut self, result: Outcome<T>) {
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
    pub scope: Arc<Scope>,
    pub pool: String,
    pub broker: Broker,
    pub cancel: CancellationToken,
    pub config: Config,
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
    } = env;
    let (reply, result) = oneshot::channel();
    let (signals, signal_rx) = mpsc::channel(config.capacity);
    let (state, state_rx) = watch::channel(RunState::WaitingObjects);
    let control = RunControl {
        id,
        name: W::NAME,
        cancel: cancel.clone(),
        signals: signals.clone(),
        state: state_rx,
        max_signal_bytes: config.max_signal_bytes,
    };
    let mut completion = Completion {
        reply: Some(reply),
        state: state.clone(),
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
        next_activity: 0,
    };
    let future = Box::pin(async move {
        // Include declaration/constructor panics in the workflow boundary.
        let mut ownership = None;
        let result = AssertUnwindSafe(async {
            let objects = workflow
                .objects()
                .into_iter()
                .map(ResourceAccess::write)
                .collect();
            ownership = Some(
                context
                    .broker
                    .acquire(Kind::Workflow, objects, &context.cancel)
                    .await?,
            );
            context.checkpoint()?;
            context.state.send_replace(RunState::Running);
            workflow.run(&mut context).await
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| Err(Failure::Uncertain("workflow panicked".into())));
        context.signals.close();
        context.state.send_replace(RunState::Settling);
        let uncertain = context.tracker.settle(&context.cancel).await;
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
