use crate::{driver::execution::ActivityGuard, model::Scope, *};
use async_trait::async_trait;
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{panic::AssertUnwindSafe, sync::Arc};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

/// Completed finishes the business step. Suspended ends this invocation after
/// actual effects and callback lifetimes have settled, retaining only resumable
/// data. Cancellation uses Err(Failure::Cancelled), never Suspended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivityExit<T, C> {
    Completed(T),
    Suspended(C),
}

/// Runtime adapter protocol, not a DDD domain service. Business capabilities live behind it.
#[async_trait]
pub trait ServiceHandler: Send + Sync + 'static {
    type Request: Send + 'static;
    type Reply: Send + 'static;
    type Query: Send + 'static;
    type View: Send + 'static;
    /// Stable observation label; never include request payloads or secrets.
    fn activity_name(_request: &Self::Request) -> &'static str {
        std::any::type_name::<Self>()
    }
    fn resources(request: &Self::Request) -> Vec<ResourceAccess>;
    async fn execute(
        &self,
        request: Self::Request,
        context: ActivityContext,
    ) -> Outcome<Self::Reply>;
    fn query(&self, query: Self::Query) -> Outcome<Self::View>;
}
pub struct ActivityContext {
    pub id: ActivityId,
    pub(crate) cancel: CancellationToken,
    pub(crate) control: ControlToken,
    pub(crate) progress: watch::Sender<TaskProgress>,
    pub(crate) activity_progress: Option<(watch::Sender<ActivityHistory>, ActivityId)>,
    pub(crate) _protected_controls: Option<crate::control::Controls>,
}
impl ActivityContext {
    /// Live task progress from a long domain operation; reporting has no business effects.
    pub fn report_progress(&self, stage: impl Into<String>, completed: u64, total: Option<u64>) {
        let stage = stage.into();
        if stage.chars().all(char::is_whitespace)
            || stage.len() > 256
            || total.is_some_and(|n| completed > n)
        {
            return;
        }
        let progress = TaskProgress {
            stage: Some(stage),
            completed,
            total,
        };
        self.progress.send_replace(progress.clone());
        if let Some((history, id)) = &self.activity_progress {
            history.send_modify(|history| {
                if let Some(entry) = history.entries.iter_mut().find(|entry| entry.id == *id) {
                    entry.progress = progress;
                }
            });
        }
    }
    /// A stopping-return service can watch this token and return its normal typed partial result.
    pub fn control(&self) -> ControlToken {
        self.control.clone()
    }
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancel
    }
}
/// Optional shorthand when the caller knows the concrete host type.
pub type ServiceClient<S> = ActivityClient<
    <S as ServiceHandler>::Request,
    <S as ServiceHandler>::Reply,
    <S as ServiceHandler>::Query,
    <S as ServiceHandler>::View,
>;
/// Typed capability channel. Application code need not know the concrete host.
enum Message<R, P, Q, V> {
    Execute {
        request: R,
        context: Box<ActivityContext>,
        guard: Box<ActivityGuard>,
        reply: oneshot::Sender<Outcome<P>>,
    },
    Query {
        query: Q,
        reply: oneshot::Sender<Outcome<V>>,
    },
}
pub struct ActivityClient<R, P, Q, V> {
    pub(crate) scope: Arc<Scope>,
    pub(crate) resources: fn(&R) -> Vec<ResourceAccess>,
    pub(crate) activity_name: fn(&R) -> &'static str,
    pub(crate) name: Arc<str>,
    tx: mpsc::Sender<Message<R, P, Q, V>>,
    stop: CancellationToken,
    state: watch::Receiver<ServiceSnapshot>,
}
impl<R, P, Q, V> Clone for ActivityClient<R, P, Q, V> {
    fn clone(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            resources: self.resources,
            activity_name: self.activity_name,
            name: self.name.clone(),
            tx: self.tx.clone(),
            stop: self.stop.clone(),
            state: self.state.clone(),
        }
    }
}
impl<R: Send + 'static, P: Send + 'static, Q: Send + 'static, V: Send + 'static>
    ActivityClient<R, P, Q, V>
{
    pub fn snapshot(&self) -> ServiceSnapshot {
        self.state.borrow().clone()
    }
    pub async fn query(&self, query: Q) -> Outcome<V> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Message::Query { query, reply })
            .await
            .map_err(|_| Failure::Unavailable)?;
        response.await.map_err(|_| Failure::Unavailable)?
    }
    /// Independent service shutdown; in-flight work is cancelled and awaited.
    pub async fn shutdown(&self) -> Outcome<()> {
        self.stop.cancel();
        let mut state = self.state.clone();
        loop {
            if state.borrow_and_update().lifecycle == Lifecycle::Stopped {
                return Ok(());
            }
            state.changed().await.map_err(|_| Failure::Unavailable)?;
        }
    }
    pub(crate) async fn dispatch(
        &self,
        request: R,
        context: ActivityContext,
        guard: ActivityGuard,
    ) -> Outcome<P> {
        let (reply, response) = oneshot::channel();
        if let Err(error) = self
            .tx
            .send(Message::Execute {
                request,
                context: Box::new(context),
                guard: Box::new(guard),
                reply,
            })
            .await
        {
            if let Message::Execute { mut guard, .. } = error.0 {
                guard.settled(&Err::<(), _>(Failure::Unavailable));
            }
            return Err(Failure::Unavailable);
        }
        response
            .await
            .map_err(|_| Failure::Uncertain("activity response lost".into()))?
    }
}

pub(crate) struct ServiceOwner {
    stop: CancellationToken,
    task: Option<JoinHandle<()>>,
}
impl ServiceOwner {
    pub async fn shutdown(&mut self) -> Outcome<()> {
        self.stop.cancel();
        let result = match &mut self.task {
            Some(task) => task
                .await
                .map_err(|_| Failure::Uncertain("service host terminated".into())),
            None => return Ok(()),
        };
        self.task.take();
        result
    }
}
impl Drop for ServiceOwner {
    fn drop(&mut self) {
        self.stop.cancel();
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
struct StateGuard(watch::Sender<ServiceSnapshot>);
impl Drop for StateGuard {
    fn drop(&mut self) {
        self.0.send_modify(|s| {
            s.lifecycle = Lifecycle::Stopped;
            s.running = 0;
        });
    }
}

pub(crate) fn spawn<S: ServiceHandler>(
    service: S,
    name: Arc<str>,
    scope: Arc<Scope>,
    capacity: usize,
) -> (ServiceClient<S>, ServiceOwner) {
    let (tx, mut rx) = mpsc::channel::<Message<S::Request, S::Reply, S::Query, S::View>>(capacity);
    let stop = CancellationToken::new();
    let (state_tx, state) = watch::channel(ServiceSnapshot {
        lifecycle: Lifecycle::Running,
        running: 0,
        completed: 0,
    });
    let stop_task = stop.clone();
    let task = tokio::spawn(async move {
        let state = StateGuard(state_tx);
        let service = Arc::new(service);
        let mut running: FuturesUnordered<BoxFuture<'static, ()>> = FuturesUnordered::new();
        let mut closing = false;
        let mut completed = 0;
        loop {
            state.0.send_replace(ServiceSnapshot {
                lifecycle: if closing {
                    Lifecycle::Stopping
                } else {
                    Lifecycle::Running
                },
                running: running.len(),
                completed,
            });
            if closing && running.is_empty() {
                break;
            }
            tokio::select! {
                biased;
                _ = stop_task.cancelled(), if !closing => {
                    closing = true; rx.close();
                    while let Ok(message) = rx.try_recv() {
                        match message {
                            Message::Execute { mut guard, reply, .. } => {
                                guard.settled(&Err::<(), _>(Failure::Cancelled));
                                drop(guard);
                                let _ = reply.send(Err(Failure::Cancelled));
                            },
                            Message::Query { reply, .. } => { let _ = reply.send(Err(Failure::Unavailable)); },
                        }
                    }
                }
                _ = running.next(), if !running.is_empty() => { completed += 1; }
                message = rx.recv(), if !closing => match message {
                    None => { closing = true; stop_task.cancel(); }
                    Some(Message::Query { query, reply }) => {
                        let result = std::panic::catch_unwind(AssertUnwindSafe(|| service.query(query)))
                            .map_err(|_| Failure::Uncertain("query panicked".into())).and_then(|result| result);
                        let _ = reply.send(result);
                    }
                    Some(Message::Execute { request, context, mut guard, reply }) => {
                        let service = service.clone(); let stop = stop_task.clone();
                        running.push(Box::pin(async move {
                            let result = if context.cancel.is_cancelled() || stop.is_cancelled() { Err(Failure::Cancelled) } else {
                                guard.started();
                                // The adapter may drop its context after cloning the token.
                                // Keep protected controls alive through actual settlement.
                                let _protected_controls = context._protected_controls.clone();
                                let cancel = context.cancel.clone();
                                let operation = AssertUnwindSafe(async move { service.execute(request, *context).await }).catch_unwind();
                                tokio::pin!(operation);
                                let result = tokio::select! {
                                    biased;
                                    result = &mut operation => result,
                                    _ = stop.cancelled() => { cancel.cancel(); operation.await },
                                };
                                result.map_err(|_| Failure::Uncertain("activity panicked".into())).and_then(|result| result)
                            };
                            guard.settled(&result); drop(guard);
                            let _ = reply.send(result);
                        }));
                    }
                }
            }
        }
    });
    (
        ActivityClient {
            scope,
            resources: S::resources,
            activity_name: S::activity_name,
            name,
            tx,
            stop: stop.clone(),
            state,
        },
        ServiceOwner {
            stop,
            task: Some(task),
        },
    )
}
