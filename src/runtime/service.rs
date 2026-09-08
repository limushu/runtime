use super::RuntimeEventSink;
use super::{
    ControlCommand, ControlError, ObservationHub, OperationContext, OperationSpec, RequestId,
    ServiceActivity, ServiceControl, ServiceLifecycle, ServiceObserver, ServiceUnavailable,
    TaskAttempt, TaskControl, TaskId, TaskMeta,
};
use async_trait::async_trait;
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc};
use tokio::{
    sync::{mpsc, oneshot},
    task::{JoinError, JoinHandle},
};
use tokio_util::sync::CancellationToken;

/// Private message protocol implemented by one domain service.
pub trait ServiceMessage: Send + 'static {
    type Error: std::fmt::Display + Send + 'static;

    fn stopped() -> Self::Error;
    fn unavailable(reason: ServiceUnavailable) -> Self::Error;
    fn reject(self, reason: ServiceUnavailable);
}

/// Associates one public request with its response type and private message.
pub trait ServiceRequest<M: ServiceMessage>: Send + 'static {
    type Response: Send + 'static;

    /// Queries return `None`; meaningful operations return a causal identity.
    fn operation(&self) -> Option<OperationSpec> {
        None
    }

    #[doc(hidden)]
    fn into_message(self, reply: oneshot::Sender<Result<Self::Response, M::Error>>) -> M;
}

struct BusinessEnvelope<M> {
    operation: OperationContext,
    message: M,
}

/// Thin typed business handle. It owns no service state, task, or routing
/// policy; a caller obtains it from the exact Pool/service instance it targets.
pub struct ServiceClient<M: ServiceMessage> {
    sender: mpsc::Sender<BusinessEnvelope<M>>,
    observer: ServiceObserver,
    hub: ObservationHub,
    message: PhantomData<fn() -> M>,
}

impl<M: ServiceMessage> ServiceClient<M> {
    pub async fn call<R>(&self, request: R) -> Result<R::Response, M::Error>
    where
        R: ServiceRequest<M>,
    {
        let operation = request
            .operation()
            .map(OperationContext::root)
            .unwrap_or_else(OperationContext::transient);
        self.call_in(&operation, request).await
    }

    /// Preserves a parent operation across an explicit cross-service call.
    pub async fn call_in<R>(
        &self,
        operation: &OperationContext,
        request: R,
    ) -> Result<R::Response, M::Error>
    where
        R: ServiceRequest<M>,
    {
        let lifecycle = self.observer.snapshot().lifecycle;
        if !matches!(
            lifecycle,
            ServiceLifecycle::Initializing | ServiceLifecycle::Running
        ) {
            let unavailable = ServiceUnavailable::from(lifecycle);
            self.hub
                .request_rejected(operation.id(), unavailable.to_string());
            return Err(M::unavailable(unavailable));
        }
        let (reply, response) = oneshot::channel();
        self.hub.request_enqueued();
        if self
            .sender
            .send(BusinessEnvelope {
                operation: operation.clone(),
                message: request.into_message(reply),
            })
            .await
            .is_err()
        {
            self.hub.request_dequeued();
            return Err(M::stopped());
        }
        response.await.map_err(|_| M::stopped())?
    }
}

impl<M: ServiceMessage> Clone for ServiceClient<M> {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            observer: self.observer.clone(),
            hub: self.hub.clone(),
            message: PhantomData,
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

/// Minimal contract between a domain service and the common runtime. Business
/// workflows remain normal async methods on `Self`.
#[async_trait]
pub trait ManagedService: Send + Sync + 'static {
    type Message: ServiceMessage;

    async fn initialize(
        &self,
        _context: ServiceContext,
    ) -> Result<(), <Self::Message as ServiceMessage>::Error> {
        Ok(())
    }

    async fn handle(
        self: Arc<Self>,
        message: Self::Message,
        context: RequestContext,
    ) -> Result<(), <Self::Message as ServiceMessage>::Error>;

    async fn shutdown(
        &self,
        _context: ServiceContext,
    ) -> Result<(), <Self::Message as ServiceMessage>::Error> {
        Ok(())
    }

    /// Optional non-blocking hook for caches or metrics that care about the
    /// Idle/Busy edge. The observer remains the primary integration surface.
    fn activity_changed(&self, _activity: ServiceActivity) {}
}

pub struct ServiceRuntime;

impl ServiceRuntime {
    pub fn spawn<S>(service: S, config: ServiceConfig) -> ServiceInstance<S::Message>
    where
        S: ManagedService,
    {
        Self::spawn_arc(Arc::new(service), config)
    }

    pub fn spawn_arc<S>(service: Arc<S>, config: ServiceConfig) -> ServiceInstance<S::Message>
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
            message: PhantomData,
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

pub struct ServiceInstance<M: ServiceMessage> {
    pub client: ServiceClient<M>,
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

struct RequestFinished<E> {
    request: RequestId,
    operation: OperationContext,
    result: Result<(), E>,
}

type RequestFuture<E> = Pin<Box<dyn Future<Output = RequestFinished<E>> + Send>>;

async fn run_service<S>(
    service: Arc<S>,
    config: ServiceConfig,
    mut business_rx: mpsc::Receiver<BusinessEnvelope<S::Message>>,
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
    let mut requests = FuturesUnordered::<RequestFuture<_>>::new();
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
        for waiter in shutdown_waiters.drain(..) {
            let _ = waiter.send(Err(ControlError::ServiceStopped));
        }
        reject_remaining(&mut business_rx, ServiceUnavailable::Failed, &hub).await;
        return;
    }

    if lifecycle == ServiceLifecycle::Initializing {
        lifecycle = ServiceLifecycle::Running;
        hub.lifecycle(lifecycle, "initialization completed");
    }

    loop {
        if matches!(
            lifecycle,
            ServiceLifecycle::Draining | ServiceLifecycle::Stopping
        ) && requests.is_empty()
        {
            let shutdown = service.shutdown(service_context.clone()).await;
            match shutdown {
                Ok(()) => {
                    lifecycle = ServiceLifecycle::Stopped;
                    hub.lifecycle(lifecycle, "shutdown completed");
                    for waiter in shutdown_waiters.drain(..) {
                        let _ = waiter.send(Ok(()));
                    }
                }
                Err(error) => {
                    hub.fail(format_error(&error));
                    for waiter in shutdown_waiters.drain(..) {
                        let _ = waiter.send(Err(ControlError::ServiceStopped));
                    }
                }
            }
            break;
        }

        let can_receive_business = business_open
            && (!matches!(lifecycle, ServiceLifecycle::Running)
                || requests.len() < config.max_in_flight_requests);

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
                let error = finished.result.as_ref().err().map(format_error);
                if let Some(activity) = hub.request_finished(
                    finished.request,
                    finished.operation.id(),
                    error,
                    finished.operation.is_observable(),
                ) {
                    service.activity_changed(activity);
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
                        requests.push(Box::pin(async move {
                            let result = service.handle(envelope.message, context).await;
                            RequestFinished {
                                request,
                                operation: envelope.operation,
                                result,
                            }
                        }));
                    }
                    Some(envelope) => {
                        hub.request_dequeued();
                        let unavailable = ServiceUnavailable::from(lifecycle);
                        hub.request_rejected(envelope.operation.id(), unavailable.to_string());
                        envelope.message.reject(unavailable);
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

    // Any sender racing with terminal transition receives a deterministic
    // rejection instead of waiting for its oneshot to be dropped.
    reject_remaining(&mut business_rx, ServiceUnavailable::Stopped, &hub).await;
}

async fn reject_remaining<M: ServiceMessage>(
    business_rx: &mut mpsc::Receiver<BusinessEnvelope<M>>,
    reason: ServiceUnavailable,
    hub: &ObservationHub,
) {
    business_rx.close();
    while let Some(envelope) = business_rx.recv().await {
        hub.request_dequeued();
        hub.request_rejected(envelope.operation.id(), reason.to_string());
        envelope.message.reject(reason.clone());
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
