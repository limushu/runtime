use crate::{model::Scope, workflow::ActivityGuard, *};
use async_trait::async_trait;
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{panic::AssertUnwindSafe, sync::Arc};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

/// Runtime adapter protocol, not a DDD domain service. Business capabilities live behind it.
#[async_trait]
pub trait ServiceHandler: Send + Sync + 'static {
    type Request: Send + 'static;
    type Reply: Send + 'static;
    type Query: Send + 'static;
    type View: Send + 'static;
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
}
impl ActivityContext {
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
        context: ActivityContext,
        guard: ActivityGuard,
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
    tx: mpsc::Sender<Message<R, P, Q, V>>,
    stop: CancellationToken,
    state: watch::Receiver<ServiceSnapshot>,
}
impl<R, P, Q, V> Clone for ActivityClient<R, P, Q, V> {
    fn clone(&self) -> Self {
        Self {
            scope: self.scope.clone(),
            resources: self.resources,
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
        response.await.unwrap_or(Err(Failure::Unavailable))
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
        self.tx
            .send(Message::Execute {
                request,
                context,
                guard,
                reply,
            })
            .await
            .map_err(|_| Failure::Unavailable)?;
        response
            .await
            .unwrap_or_else(|_| Err(Failure::Uncertain("activity response lost".into())))
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
                            Message::Execute { reply, .. } => { let _ = reply.send(Err(Failure::Cancelled)); },
                            Message::Query { reply, .. } => { let _ = reply.send(Err(Failure::Unavailable)); },
                        }
                    }
                }
                _ = running.next(), if !running.is_empty() => { completed += 1; }
                message = rx.recv(), if !closing => match message {
                    None => { closing = true; stop_task.cancel(); }
                    Some(Message::Query { query, reply }) => {
                        let result = std::panic::catch_unwind(AssertUnwindSafe(|| service.query(query)))
                            .unwrap_or_else(|_| Err(Failure::Uncertain("query panicked".into())));
                        let _ = reply.send(result);
                    }
                    Some(Message::Execute { request, context, mut guard, reply }) => {
                        let service = service.clone(); let stop = stop_task.clone();
                        running.push(Box::pin(async move {
                            let result = if context.cancel.is_cancelled() || stop.is_cancelled() { Err(Failure::Cancelled) } else {
                                guard.started();
                                let cancel = context.cancel.clone();
                                let operation = AssertUnwindSafe(async move { service.execute(request, context).await }).catch_unwind();
                                tokio::pin!(operation);
                                let result = tokio::select! {
                                    biased;
                                    result = &mut operation => result,
                                    _ = stop.cancelled() => { cancel.cancel(); operation.await },
                                };
                                result.unwrap_or_else(|_| Err(Failure::Uncertain("activity panicked".into())))
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
