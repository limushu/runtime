use super::RuntimeEventSink;
use super::{
    ControlCommand, ControlError, ObservationHub, OperationContext, OperationSpec, RequestId,
    ServiceActivity, ServiceControl, ServiceLifecycle, ServiceObserver, ServiceUnavailable,
    TaskAttempt, TaskControl, TaskId, TaskMeta,
};
use async_trait::async_trait;
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::any::Any;
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc};
use tokio::{
    sync::{mpsc, oneshot},
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;

/// Separates transport/lifecycle failures from errors returned by a domain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError<E> {
    Unavailable(ServiceUnavailable),
    ServiceStopped,
    RequestAborted,
    HandlerPanicked(String),
    ProtocolViolation(&'static str),
    Business(E),
}

impl<E: fmt::Display> fmt::Display for CallError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(reason) => reason.fmt(formatter),
            Self::ServiceStopped => formatter.write_str("service has stopped"),
            Self::RequestAborted => formatter.write_str("request was aborted before completion"),
            Self::HandlerPanicked(message) => {
                write!(formatter, "service handler panicked: {message}")
            }
            Self::ProtocolViolation(message) => message.fmt(formatter),
            Self::Business(error) => error.fmt(formatter),
        }
    }
}

impl<E> std::error::Error for CallError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Unavailable(reason) => Some(reason),
            Self::Business(error) => Some(error),
            Self::ServiceStopped
            | Self::RequestAborted
            | Self::HandlerPanicked(_)
            | Self::ProtocolViolation(_) => None,
        }
    }
}

type ReplyResult<R, E> = Result<R, CallError<E>>;

/// The one reply belonging to a service request.
///
/// A handler may send it before its Future finishes (for example, an event can
/// return `Accepted` and then continue converging). The runtime retains the
/// same reply cell so it can report an early business error or a missing reply.
pub struct ServiceReply<R, E> {
    sender: Option<oneshot::Sender<ReplyResult<R, E>>>,
}

impl<R, E> ServiceReply<R, E> {
    fn new(sender: oneshot::Sender<ReplyResult<R, E>>) -> Self {
        Self {
            sender: Some(sender),
        }
    }

    /// Sends the service's unified reply. Only the first reply is accepted.
    pub fn send(&mut self, reply: R) -> bool {
        self.send_result(Ok(reply))
    }

    fn send_business_error(&mut self, error: E) -> bool {
        self.send_result(Err(CallError::Business(error)))
    }

    fn send_protocol_violation(&mut self, message: &'static str) -> bool {
        self.send_result(Err(CallError::ProtocolViolation(message)))
    }

    fn send_handler_panicked(&mut self, message: String) -> bool {
        self.send_result(Err(CallError::HandlerPanicked(message)))
    }

    fn send_result(&mut self, result: ReplyResult<R, E>) -> bool {
        let sender = self.sender.take();
        sender.is_some_and(|sender| sender.send(result).is_ok())
    }

    fn is_pending(&self) -> bool {
        self.sender.is_some()
    }
}

impl<R, E> Drop for ServiceReply<R, E> {
    fn drop(&mut self) {
        if let Some(sender) = self.sender.take() {
            let _ = sender.send(Err(CallError::RequestAborted));
        }
    }
}

/// Minimal contract between a domain service and the common runtime. A domain
/// owns one explicit request enum, one reply enum and one business error type.
#[async_trait]
pub trait ManagedService: Send + Sync + 'static {
    type Request: Send + 'static;
    type Reply: Send + 'static;
    type Error: fmt::Display + Send + 'static;

    /// Queries return `None`; meaningful operations return a causal identity.
    fn operation(_request: &Self::Request) -> Option<OperationSpec> {
        None
    }

    async fn initialize(&self, _context: ServiceContext) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn handle(
        self: Arc<Self>,
        request: Self::Request,
        reply: &mut ServiceReply<Self::Reply, Self::Error>,
        context: RequestContext,
    ) -> Result<(), Self::Error>;

    async fn shutdown(&self, _context: ServiceContext) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Optional non-blocking hook for caches or metrics that care about the
    /// Idle/Busy edge. The observer remains the primary integration surface.
    fn activity_changed(&self, _activity: ServiceActivity) {}
}

struct BusinessEnvelope<S: ManagedService> {
    operation: OperationContext,
    request: S::Request,
    reply: oneshot::Sender<ReplyResult<S::Reply, S::Error>>,
}

impl<S: ManagedService> BusinessEnvelope<S> {
    fn reject(self, reason: ServiceUnavailable) {
        let _ = self.reply.send(Err(CallError::Unavailable(reason)));
    }
}

/// Thin typed business handle. It owns no service state, task, or routing
/// policy; a caller obtains it from the exact Pool/service instance it targets.
pub struct ServiceClient<S: ManagedService> {
    sender: mpsc::Sender<BusinessEnvelope<S>>,
    observer: ServiceObserver,
    hub: ObservationHub,
    service: PhantomData<fn() -> S>,
}

impl<S: ManagedService> ServiceClient<S> {
    pub async fn call(&self, request: S::Request) -> ReplyResult<S::Reply, S::Error> {
        let operation = S::operation(&request)
            .map(OperationContext::root)
            .unwrap_or_else(OperationContext::transient);
        self.call_in(&operation, request).await
    }

    /// Preserves a parent operation across an explicit cross-service call.
    pub async fn call_in(
        &self,
        operation: &OperationContext,
        request: S::Request,
    ) -> ReplyResult<S::Reply, S::Error> {
        let lifecycle = self.observer.snapshot().lifecycle;
        if !matches!(
            lifecycle,
            ServiceLifecycle::Initializing | ServiceLifecycle::Running
        ) {
            let unavailable = ServiceUnavailable::from(lifecycle);
            self.hub
                .request_rejected(operation.id(), unavailable.to_string());
            return Err(CallError::Unavailable(unavailable));
        }
        let (reply, response) = oneshot::channel();
        self.hub.request_enqueued();
        if self
            .sender
            .send(BusinessEnvelope {
                operation: operation.clone(),
                request,
                reply,
            })
            .await
            .is_err()
        {
            self.hub.request_dequeued();
            return Err(CallError::ServiceStopped);
        }
        response.await.map_err(|_| CallError::ServiceStopped)?
    }
}

impl<S: ManagedService> Clone for ServiceClient<S> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            observer: self.observer.clone(),
            hub: self.hub.clone(),
            service: PhantomData,
        }
    }
}

#[derive(Clone)]
pub struct ServiceConfig {
    pub name: String,
    pub domain: String,
    pub business_capacity: usize,
    pub control_capacity: usize,
    pub max_in_flight_requests: usize,
    pub event_history: usize,
    pub event_sink: Option<Arc<dyn RuntimeEventSink>>,
}

impl ServiceConfig {
    pub fn new(name: impl Into<String>, domain: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            domain: domain.into(),
            business_capacity: 128,
            control_capacity: 16,
            max_in_flight_requests: 64,
            event_history: 1024,
            event_sink: None,
        }
    }
}

#[derive(Clone)]
pub struct ServiceContext {
    cancellation: CancellationToken,
    observer: ServiceObserver,
}

impl ServiceContext {
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub fn observer(&self) -> ServiceObserver {
        self.observer.clone()
    }
}

#[derive(Clone)]
pub struct RequestContext {
    request_id: RequestId,
    operation: OperationContext,
    cancellation: CancellationToken,
    hub: ObservationHub,
}

impl RequestContext {
    pub fn request_id(&self) -> RequestId {
        self.request_id
    }

    pub fn operation(&self) -> &OperationContext {
        &self.operation
    }

    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    /// Attaches task control and observation to work already being performed
    /// by this request Future. This call does not spawn or wrap the workflow.
    pub fn start_task(&self, meta: TaskMeta, cancellation: CancellationToken) -> TaskAttempt {
        TaskAttempt::start(self.hub.clone(), &self.operation, meta, cancellation)
    }

    pub fn control_task(&self, task: TaskId) -> Option<TaskControl> {
        self.hub.task_control(task)
    }
}

pub struct ServiceRuntime;

impl ServiceRuntime {
    pub fn spawn<S>(service: S, config: ServiceConfig) -> ServiceInstance<S>
    where
        S: ManagedService,
    {
        Self::spawn_arc(Arc::new(service), config)
    }

    pub fn spawn_arc<S>(service: Arc<S>, config: ServiceConfig) -> ServiceInstance<S>
    where
        S: ManagedService,
    {
        assert!(
            config.business_capacity > 0,
            "business capacity must be non-zero"
        );
        assert!(
            config.control_capacity > 0,
            "control capacity must be non-zero"
        );
        assert!(
            config.max_in_flight_requests > 0,
            "request concurrency must be non-zero"
        );

        let (business_tx, business_rx) = mpsc::channel(config.business_capacity);
        let (control_tx, control_rx) = mpsc::channel(config.control_capacity);
        let (hub, observer) = ObservationHub::new(
            config.name.clone(),
            config.domain.clone(),
            config.event_history,
            config.event_sink.clone(),
        );
        let client = ServiceClient {
            sender: business_tx,
            observer: observer.clone(),
            hub: hub.clone(),
            service: PhantomData,
        };
        let control = ServiceControl::new(control_tx, observer.clone());
        let service_cancellation = CancellationToken::new();
        let root_hub = hub.clone();
        let root_observer = observer.clone();
        let task = tokio::spawn(async move {
            let result = AssertUnwindSafe(run_service(
                service,
                config,
                business_rx,
                control_rx,
                root_hub.clone(),
                root_observer,
                service_cancellation,
            ))
            .catch_unwind()
            .await;
            if let Err(panic) = result {
                root_hub.fail(format!(
                    "service root panicked: {}",
                    panic_message(panic.as_ref())
                ));
            }
        });

        ServiceInstance {
            client,
            control,
            observer,
            task: ServiceTask {
                task,
                hub,
                force_aborted: AtomicBool::new(false),
            },
        }
    }
}

pub struct ServiceInstance<S: ManagedService> {
    pub client: ServiceClient<S>,
    pub control: ServiceControl,
    pub observer: ServiceObserver,
    pub task: ServiceTask,
}

/// Ownership handle for the one Tokio task that contains every service Future.
pub struct ServiceTask {
    task: JoinHandle<()>,
    hub: ObservationHub,
    force_aborted: AtomicBool,
}

impl ServiceTask {
    pub fn abort(&self) {
        self.force_abort_once("root task was force-aborted by its owner");
    }

    pub async fn join(mut self) -> Result<(), JoinError> {
        (&mut self.task).await
    }

    fn force_abort_once(&self, reason: &str) {
        if !self.force_aborted.swap(true, Ordering::AcqRel) {
            self.hub.force_abort(reason);
            self.task.abort();
        }
    }
}

impl Future for ServiceTask {
    type Output = Result<(), JoinError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.task).poll(context)
    }
}

impl Drop for ServiceTask {
    fn drop(&mut self) {
        if !self.task.is_finished() {
            self.force_abort_once("root task ownership was dropped before service shutdown");
        }
    }
}

struct RequestFinished {
    request: RequestId,
    operation: OperationContext,
    error: Option<String>,
    fatal_error: Option<String>,
}

type RequestFuture = Pin<Box<dyn Future<Output = RequestFinished> + Send>>;

/// Closes observation if the root task drops an accepted request Future.
/// Normal completion is finalized by `run_service`, where the service's
/// activity callback can still be invoked safely.
struct RequestCompletionGuard {
    request: RequestId,
    operation: OperationContext,
    hub: ObservationHub,
    armed: bool,
}

impl RequestCompletionGuard {
    fn new(request: RequestId, operation: OperationContext, hub: ObservationHub) -> Self {
        Self {
            request,
            operation,
            hub,
            armed: true,
        }
    }

    fn finish(mut self, error: Option<String>, fatal_error: Option<String>) -> RequestFinished {
        self.armed = false;
        RequestFinished {
            request: self.request,
            operation: self.operation.clone(),
            error,
            fatal_error,
        }
    }
}

impl Drop for RequestCompletionGuard {
    fn drop(&mut self) {
        if self.armed {
            self.hub.request_finished(
                self.request,
                self.operation.id(),
                Some("request was aborted before completion".to_owned()),
                self.operation.is_observable(),
            );
        }
    }
}

async fn run_service<S>(
    service: Arc<S>,
    config: ServiceConfig,
    mut business_rx: mpsc::Receiver<BusinessEnvelope<S>>,
    mut control_rx: mpsc::Receiver<ControlCommand>,
    hub: ObservationHub,
    observer: ServiceObserver,
    service_cancellation: CancellationToken,
) where
    S: ManagedService,
{
    let service_context = ServiceContext {
        cancellation: service_cancellation.clone(),
        observer,
    };

    let mut lifecycle = ServiceLifecycle::Initializing;
    let mut requests = FuturesUnordered::<RequestFuture>::new();
    let mut shutdown_waiters: Vec<oneshot::Sender<Result<(), ControlError>>> = Vec::new();
    let mut business_open = true;

    let initialization = service.initialize(service_context.clone());
    tokio::pin!(initialization);
    let initialization_result = loop {
        tokio::select! {
            biased;

            Some(command) = control_rx.recv() => {
                handle_control(
                    command,
                    &mut lifecycle,
                    &hub,
                    &service_cancellation,
                    &mut shutdown_waiters,
                );
            }

            result = &mut initialization => break result,
        }
    };

    if let Err(error) = initialization_result
        && lifecycle != ServiceLifecycle::Stopping
    {
        hub.fail(format_error(&error));
        reject_remaining(&mut business_rx, ServiceUnavailable::Failed, &hub).await;
        for waiter in shutdown_waiters.drain(..) {
            let _ = waiter.send(Err(ControlError::ServiceStopped));
        }
        return;
    }

    if lifecycle == ServiceLifecycle::Initializing {
        lifecycle = ServiceLifecycle::Running;
        hub.lifecycle(lifecycle, "initialization completed");
    }

    let mut terminal_failure = None;
    loop {
        if requests.is_empty() {
            if terminal_failure.is_some() {
                reject_remaining(&mut business_rx, ServiceUnavailable::Failed, &hub).await;
                for waiter in shutdown_waiters.drain(..) {
                    let _ = waiter.send(Err(ControlError::ServiceStopped));
                }
                return;
            }

            if matches!(
                lifecycle,
                ServiceLifecycle::Draining | ServiceLifecycle::Stopping
            ) {
                let shutdown = AssertUnwindSafe(service.shutdown(service_context.clone()))
                    .catch_unwind()
                    .await;
                match shutdown {
                    Ok(Ok(())) => {
                        // Close and reject the queue before acknowledging the
                        // lifecycle command. After drain/stop returns, no
                        // caller can still be waiting in the old queue.
                        reject_remaining(&mut business_rx, ServiceUnavailable::Stopped, &hub).await;
                        lifecycle = ServiceLifecycle::Stopped;
                        hub.lifecycle(lifecycle, "shutdown completed");
                        for waiter in shutdown_waiters.drain(..) {
                            let _ = waiter.send(Ok(()));
                        }
                    }
                    Ok(Err(error)) => {
                        hub.fail(format_error(&error));
                        reject_remaining(&mut business_rx, ServiceUnavailable::Failed, &hub).await;
                        for waiter in shutdown_waiters.drain(..) {
                            let _ = waiter.send(Err(ControlError::ServiceStopped));
                        }
                    }
                    Err(panic) => {
                        hub.fail(format!(
                            "service shutdown panicked: {}",
                            panic_message(panic.as_ref())
                        ));
                        reject_remaining(&mut business_rx, ServiceUnavailable::Failed, &hub).await;
                        for waiter in shutdown_waiters.drain(..) {
                            let _ = waiter.send(Err(ControlError::ServiceStopped));
                        }
                    }
                }
                return;
            }
        }

        let can_receive_business = business_open
            && match lifecycle {
                ServiceLifecycle::Running => requests.len() < config.max_in_flight_requests,
                // Paused work that entered the channel before the control
                // transition can be rejected immediately. Drain/stop retain
                // their old queue until the shutdown outcome is known, so
                // every queued caller receives the same terminal reason.
                ServiceLifecycle::Initializing | ServiceLifecycle::Paused => true,
                ServiceLifecycle::Draining
                | ServiceLifecycle::Stopping
                | ServiceLifecycle::Stopped
                | ServiceLifecycle::Failed => false,
            };

        tokio::select! {
            biased;

            Some(command) = control_rx.recv() => {
                handle_control(
                    command,
                    &mut lifecycle,
                    &hub,
                    &service_cancellation,
                    &mut shutdown_waiters,
                );
            }

            Some(finished) = requests.next(), if !requests.is_empty() => {
                let fatal_error = finished.fatal_error;
                if let Some(activity) = hub.request_finished(
                    finished.request,
                    finished.operation.id(),
                    finished.error,
                    finished.operation.is_observable(),
                ) {
                    service.activity_changed(activity);
                }
                if let Some(error) = fatal_error {
                    hub.cancel_all("a service handler panicked");
                    service_cancellation.cancel();
                    // A handler panic invalidates the service instance. Do
                    // not let another handler that ignores cancellation keep
                    // the failed root alive: dropping these Futures invokes
                    // their completion guards and closes every accepted
                    // request/operation explicitly.
                    let dropped_sibling_requests = !requests.is_empty();
                    requests = FuturesUnordered::new();
                    lifecycle = ServiceLifecycle::Failed;
                    // Aborted sibling requests are recorded first; publish
                    // the panic last so `last_error` retains the root cause.
                    hub.fail(error.clone());
                    if dropped_sibling_requests {
                        service.activity_changed(ServiceActivity::Idle);
                    }
                    terminal_failure = Some(error);
                }
            }

            message = business_rx.recv(), if can_receive_business => {
                match message {
                    Some(envelope) if lifecycle.accepts_requests() => {
                        hub.request_dequeued();
                        let request = RequestId::next();
                        if let Some(activity) = hub.request_accepted(request, &envelope.operation) {
                            service.activity_changed(activity);
                        }
                        let context = RequestContext {
                            request_id: request,
                            operation: envelope.operation.clone(),
                            cancellation: service_cancellation.child_token(),
                            hub: hub.clone(),
                        };
                        let service = service.clone();
                        let request_hub = hub.clone();
                        requests.push(Box::pin(async move {
                            let completion = RequestCompletionGuard::new(
                                request,
                                envelope.operation.clone(),
                                request_hub,
                            );
                            let mut reply = ServiceReply::new(envelope.reply);
                            let result = AssertUnwindSafe(service.handle(
                                envelope.request,
                                &mut reply,
                                context,
                            ))
                            .catch_unwind()
                            .await;
                            match result {
                                Ok(Ok(())) if reply.is_pending() => {
                                    let error =
                                        "service handler completed without replying".to_owned();
                                    reply.send_protocol_violation(
                                        "service handler completed without replying",
                                    );
                                    completion.finish(Some(error), None)
                                }
                                Ok(Ok(())) => completion.finish(None, None),
                                Ok(Err(error)) => {
                                    let message = format_error(&error);
                                    reply.send_business_error(error);
                                    completion.finish(Some(message), None)
                                }
                                Err(panic) => {
                                    let panic = panic_message(panic.as_ref()).to_owned();
                                    let error = format!("service handler panicked: {panic}");
                                    reply.send_handler_panicked(panic);
                                    completion.finish(Some(error.clone()), Some(error))
                                }
                            }
                        }));
                    }
                    Some(envelope) => {
                        hub.request_dequeued();
                        let unavailable = ServiceUnavailable::from(lifecycle);
                        hub.request_rejected(envelope.operation.id(), unavailable.to_string());
                        envelope.reject(unavailable);
                    }
                    None => {
                        business_open = false;
                        if matches!(lifecycle, ServiceLifecycle::Running | ServiceLifecycle::Paused) {
                            lifecycle = ServiceLifecycle::Draining;
                            hub.lifecycle(lifecycle, "all business clients were dropped");
                        }
                    }
                }
            }
        }
    }
}

async fn reject_remaining<S: ManagedService>(
    business_rx: &mut mpsc::Receiver<BusinessEnvelope<S>>,
    reason: ServiceUnavailable,
    hub: &ObservationHub,
) {
    business_rx.close();
    while let Some(envelope) = business_rx.recv().await {
        hub.request_dequeued();
        hub.request_rejected(envelope.operation.id(), reason.to_string());
        envelope.reject(reason.clone());
    }
}

fn handle_control(
    command: ControlCommand,
    lifecycle: &mut ServiceLifecycle,
    hub: &ObservationHub,
    service_cancellation: &CancellationToken,
    shutdown_waiters: &mut Vec<oneshot::Sender<Result<(), ControlError>>>,
) {
    match command {
        ControlCommand::Pause(reply) => {
            let result = if *lifecycle == ServiceLifecycle::Running {
                *lifecycle = ServiceLifecycle::Paused;
                hub.lifecycle(*lifecycle, "pause requested");
                Ok(())
            } else {
                Err(ControlError::InvalidTransition {
                    from: *lifecycle,
                    command: "pause",
                })
            };
            let _ = reply.send(result);
        }
        ControlCommand::Resume(reply) => {
            let result = if *lifecycle == ServiceLifecycle::Paused {
                *lifecycle = ServiceLifecycle::Running;
                hub.lifecycle(*lifecycle, "resume requested");
                Ok(())
            } else {
                Err(ControlError::InvalidTransition {
                    from: *lifecycle,
                    command: "resume",
                })
            };
            let _ = reply.send(result);
        }
        ControlCommand::Drain(reply) => {
            if matches!(
                *lifecycle,
                ServiceLifecycle::Initializing
                    | ServiceLifecycle::Running
                    | ServiceLifecycle::Paused
            ) {
                *lifecycle = ServiceLifecycle::Draining;
                hub.lifecycle(*lifecycle, "drain requested");
                shutdown_waiters.push(reply);
            } else if *lifecycle == ServiceLifecycle::Draining {
                shutdown_waiters.push(reply);
            } else {
                let _ = reply.send(Err(ControlError::InvalidTransition {
                    from: *lifecycle,
                    command: "drain",
                }));
            }
        }
        ControlCommand::Stop(reply) => {
            if matches!(
                *lifecycle,
                ServiceLifecycle::Initializing
                    | ServiceLifecycle::Running
                    | ServiceLifecycle::Paused
                    | ServiceLifecycle::Draining
            ) {
                *lifecycle = ServiceLifecycle::Stopping;
                hub.lifecycle(*lifecycle, "cooperative stop requested");
                hub.cancel_all("service is stopping");
                service_cancellation.cancel();
                shutdown_waiters.push(reply);
            } else if *lifecycle == ServiceLifecycle::Stopping {
                shutdown_waiters.push(reply);
            } else {
                let _ = reply.send(Err(ControlError::InvalidTransition {
                    from: *lifecycle,
                    command: "stop",
                }));
            }
        }
        ControlCommand::CancelTask { task, cause, reply } => {
            let _ = reply.send(hub.cancel_task(task, cause));
        }
    }
}

fn format_error<E: std::fmt::Display>(error: &E) -> String {
    error.to_string()
}

fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}
