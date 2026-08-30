use super::object_actor::{Intent, ObjectActor, ReplaceOutcome};
use crate::context::CancellationScope;
use crate::observation::{ObservationHub, TaskOutcome};
use crate::router::{BusinessEnvelope, BusinessHandle};
use crate::{
    Activity, CancelCause, ExecutionClass, ObjectActivity, ObjectDecision, ObjectKey,
    ObservationEvent, Router, RuntimeError, RuntimeResult, Service, ServiceId, ServiceLifecycle,
    ServiceObserver, ServiceRequest, ServiceSnapshot, TaskAttemptId, WorkflowContext,
};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub(super) type ResponseOf<S> = <<S as Service>::Request as ServiceRequest>::Response;
pub(super) type Reply<R> = oneshot::Sender<RuntimeResult<R>>;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub business_capacity: usize,
    pub control_capacity: usize,
    pub max_active_workflows: usize,
    pub shutdown_timeout: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            business_capacity: 128,
            control_capacity: 16,
            max_active_workflows: 64,
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

enum ControlRequest {
    Pause(Reply<()>),
    Resume(Reply<()>),
    Drain(Reply<()>),
    Stop(Reply<()>),
}

#[derive(Clone)]
pub struct ControlHandle {
    service_id: ServiceId,
    sender: mpsc::Sender<ControlRequest>,
}

impl ControlHandle {
    async fn request(&self, build: impl FnOnce(Reply<()>) -> ControlRequest) -> RuntimeResult<()> {
        let (reply, ticket) = oneshot::channel();
        self.sender
            .send(build(reply))
            .await
            .map_err(|_| RuntimeError::ChannelClosed(self.service_id.clone()))?;
        ticket
            .await
            .map_err(|_| RuntimeError::ChannelClosed(self.service_id.clone()))?
    }

    pub async fn pause(&self) -> RuntimeResult<()> {
        self.request(ControlRequest::Pause).await
    }

    pub async fn resume(&self) -> RuntimeResult<()> {
        self.request(ControlRequest::Resume).await
    }

    pub async fn drain(&self) -> RuntimeResult<()> {
        self.request(ControlRequest::Drain).await
    }

    pub async fn stop(&self) -> RuntimeResult<()> {
        self.request(ControlRequest::Stop).await
    }
}

pub struct ManagedService {
    pub service_id: ServiceId,
    pub control: ControlHandle,
    pub observer: ServiceObserver,
    shutdown_timeout: Duration,
    cancellation: CancellationScope,
    join: Option<JoinHandle<()>>,
}

impl ManagedService {
    pub fn force_abort(&self) {
        self.cancellation
            .request(CancelCause::new("service force aborted"));
        if let Some(join) = &self.join {
            join.abort();
        }
    }

    pub async fn shutdown(mut self) -> RuntimeResult<()> {
        self.cancellation
            .request(CancelCause::new("service shutting down"));
        match tokio::time::timeout(self.shutdown_timeout, self.control.stop()).await {
            Ok(result) => result?,
            Err(_) => {
                self.force_abort();
                return Err(RuntimeError::Timeout(format!(
                    "service {} did not stop cooperatively",
                    self.service_id
                )));
            }
        }
        if let Some(join) = self.join.take() {
            join.await
                .map_err(|error| RuntimeError::Internal(error.to_string()))?;
        }
        Ok(())
    }
}

impl Drop for ManagedService {
    fn drop(&mut self) {
        self.cancellation
            .request(CancelCause::new("service owner dropped"));
        if let Some(join) = &self.join {
            join.abort();
        }
    }
}

enum CompletionKind<R> {
    Inline {
        reply: Reply<R>,
    },
    Managed {
        key: ObjectKey,
        task_attempt_id: TaskAttemptId,
        context: WorkflowContext,
    },
}

struct Completion<R> {
    kind: CompletionKind<R>,
    result: RuntimeResult<R>,
}

struct ServiceLoop<S: Service> {
    service: Arc<S>,
    service_id: ServiceId,
    config: RuntimeConfig,
    lifecycle: ServiceLifecycle,
    business_rx: mpsc::Receiver<BusinessEnvelope<S::Request>>,
    control_rx: mpsc::Receiver<ControlRequest>,
    running: FuturesUnordered<BoxFuture<'static, Completion<ResponseOf<S>>>>,
    actors: HashMap<ObjectKey, ObjectActor<S>>,
    drain_waiters: Vec<Reply<()>>,
    stop_waiters: Vec<Reply<()>>,
    observation: ObservationHub,
    cancellation: CancellationScope,
    last_snapshot: ServiceSnapshot,
}

pub fn spawn_service<S: Service>(
    service: Arc<S>,
    router: &Router,
    mut config: RuntimeConfig,
) -> ManagedService {
    config.max_active_workflows = config.max_active_workflows.max(1);
    let service_id = service.id();
    let (business_tx, business_rx) = mpsc::channel(config.business_capacity.max(1));
    let (control_tx, control_rx) = mpsc::channel(config.control_capacity.max(1));
    let (observation, observer) = ObservationHub::new(service_id.clone());
    let cancellation = CancellationScope::root();
    router.register(
        BusinessHandle::new(service_id.clone(), business_tx),
        observation.clone(),
    );
    let last_snapshot = ServiceSnapshot::initial(service_id.clone());
    let shutdown_timeout = config.shutdown_timeout;
    let runtime = ServiceLoop {
        service,
        service_id: service_id.clone(),
        config,
        lifecycle: ServiceLifecycle::Running,
        business_rx,
        control_rx,
        running: FuturesUnordered::new(),
        actors: HashMap::new(),
        drain_waiters: Vec::new(),
        stop_waiters: Vec::new(),
        observation,
        cancellation: cancellation.clone(),
        last_snapshot,
    };
    let join = tokio::spawn(runtime.run());

    ManagedService {
        service_id: service_id.clone(),
        control: ControlHandle {
            service_id,
            sender: control_tx,
        },
        observer,
        shutdown_timeout,
        cancellation,
        join: Some(join),
    }
}

impl<S: Service> ServiceLoop<S> {
    async fn run(mut self) {
        loop {
            self.finish_lifecycle_if_ready();
            if self.lifecycle == ServiceLifecycle::Stopped {
                return;
            }
            tokio::select! {
                biased;
                Some(control) = self.control_rx.recv() => self.handle_control(control),
                Some(completion) = self.running.next(), if !self.running.is_empty() => {
                    self.handle_completion(completion);
                }
                Some(envelope) = self.business_rx.recv() => self.handle_business(envelope),
                else => {
                    self.cancel_active(CancelCause::new("service channels closed"));
                    self.reject_pending(RuntimeError::ChannelClosed(self.service_id.clone()));
                    if self.running.is_empty() {
                        self.set_lifecycle(ServiceLifecycle::Stopped);
                    }
                }
            }
            self.launch_pending();
            self.publish_snapshot();
        }
    }

    fn handle_business(&mut self, envelope: BusinessEnvelope<S::Request>) {
        match self.lifecycle {
            ServiceLifecycle::Running => {}
            ServiceLifecycle::Paused => {
                let _ = envelope
                    .reply
                    .send(Err(RuntimeError::ServicePaused(self.service_id.clone())));
                return;
            }
            ServiceLifecycle::Draining => {
                let _ = envelope
                    .reply
                    .send(Err(RuntimeError::ServiceDraining(self.service_id.clone())));
                return;
            }
            ServiceLifecycle::Stopping | ServiceLifecycle::Stopped => {
                let _ = envelope
                    .reply
                    .send(Err(RuntimeError::ServiceStopping(self.service_id.clone())));
                return;
            }
        }

        match self.service.classify(&envelope.request) {
            ExecutionClass::Inline => self.launch_inline(envelope),
            ExecutionClass::Workflow(meta) => self.admit_intent(Intent::new(envelope, meta)),
        }
    }

    fn admit_intent(&mut self, intent: Intent<S>) {
        let key = intent.meta.key.clone();
        let activity = self
            .actors
            .entry(key.clone())
            .or_insert_with(|| ObjectActor::new(key))
            .activity();
        let decision =
            match self
                .service
                .decide(&intent.context, &intent.request, &intent.meta, &activity)
            {
                Ok(decision) => decision,
                Err(error) => {
                    Self::reply_intent(intent, Err(error));
                    return;
                }
            };
        self.apply_object_decision(intent, decision);
    }

    fn apply_object_decision(
        &mut self,
        mut intent: Intent<S>,
        decision: ObjectDecision<ResponseOf<S>>,
    ) {
        let key = intent.meta.key.clone();
        match decision {
            ObjectDecision::Start => {
                let busy = self.actors.get(&key).is_some_and(|actor| !actor.is_idle());
                if busy {
                    Self::reply_intent(
                        intent,
                        Err(RuntimeError::Rejected(
                            "domain returned Start for a non-idle object actor".into(),
                        )),
                    );
                } else {
                    self.start_or_queue(intent);
                }
            }
            ObjectDecision::JoinExisting => {
                let footprint = intent.meta.footprint.clone();
                let waiters = std::mem::take(&mut intent.waiters);
                let operation_id = self
                    .actors
                    .get_mut(&key)
                    .expect("admission created the object actor")
                    .join_current(waiters);
                self.publish_join_or_reject(operation_id, footprint);
            }
            ObjectDecision::JoinPending => {
                let footprint = intent.meta.footprint.clone();
                let waiters = std::mem::take(&mut intent.waiters);
                let operation_id = self
                    .actors
                    .get_mut(&key)
                    .expect("admission created the object actor")
                    .join_pending(waiters);
                self.publish_join_or_reject(operation_id, footprint);
            }
            ObjectDecision::JoinReplacement => {
                let footprint = intent.meta.footprint.clone();
                let waiters = std::mem::take(&mut intent.waiters);
                let operation_id = self
                    .actors
                    .get_mut(&key)
                    .expect("admission created the object actor")
                    .join_replacement(waiters);
                self.publish_join_or_reject(operation_id, footprint);
            }
            ObjectDecision::Queue => self.queue_intent(intent),
            ObjectDecision::CancelThenStart { cause } => {
                let footprint = intent.meta.footprint.clone();
                let Some(actor) = self.actors.get_mut(&key) else {
                    self.start_or_queue(intent);
                    return;
                };
                let outcome = actor.replace(intent, cause.clone());
                match outcome {
                    Ok(ReplaceOutcome::CancelRequested(notice)) => {
                        self.publish_queued(footprint);
                        self.observation
                            .publish(ObservationEvent::TaskCancelRequested {
                                service_id: self.service_id.clone(),
                                task_attempt_id: notice.task_attempt_id,
                                operation_id: notice.operation_id,
                                cause,
                            });
                    }
                    Ok(ReplaceOutcome::ReplacementSuperseded(waiters)) => {
                        self.publish_queued(footprint.clone());
                        Self::reply_all(
                            waiters,
                            Err(RuntimeError::Superseded(
                                "a newer object intent replaced the pending intent".into(),
                            )),
                        );
                        self.observation.publish(ObservationEvent::RequestMerged {
                            service_id: self.service_id.clone(),
                            operation_id: self
                                .actors
                                .get(&key)
                                .map(|actor| match actor.activity() {
                                    ObjectActivity::Pending { operation_id, .. } => operation_id,
                                    ObjectActivity::Cancelling {
                                        replacement_operation_id,
                                        ..
                                    } => replacement_operation_id,
                                    ObjectActivity::Idle | ObjectActivity::Running { .. } => {
                                        unreachable!("replacement was just installed")
                                    }
                                })
                                .expect("object actor exists"),
                            footprint,
                        });
                    }
                    Err(intent) => self.start_or_queue(*intent),
                }
            }
            ObjectDecision::Complete(response) => Self::reply_intent(intent, Ok(response)),
            ObjectDecision::Reject { reason } => {
                Self::reply_intent(intent, Err(RuntimeError::Rejected(reason.to_string())))
            }
        }
    }

    fn publish_join_or_reject(
        &self,
        operation_id: Result<crate::OperationId, Vec<Reply<ResponseOf<S>>>>,
        footprint: crate::Footprint,
    ) {
        match operation_id {
            Ok(operation_id) => {
                self.observation.publish(ObservationEvent::RequestJoined {
                    service_id: self.service_id.clone(),
                    operation_id,
                    footprint,
                });
            }
            Err(waiters) => Self::reply_all(
                waiters,
                Err(RuntimeError::Internal(
                    "domain selected a join target that is not present".into(),
                )),
            ),
        }
    }

    fn start_or_queue(&mut self, intent: Intent<S>) {
        if self.active_count() < self.config.max_active_workflows {
            self.launch_managed(intent);
        } else {
            self.queue_intent(intent);
        }
    }

    fn queue_intent(&mut self, intent: Intent<S>) {
        let key = intent.meta.key.clone();
        self.publish_queued(intent.meta.footprint.clone());
        self.actors
            .entry(key.clone())
            .or_insert_with(|| ObjectActor::new(key))
            .queue(intent);
    }

    fn publish_queued(&self, footprint: crate::Footprint) {
        self.observation.publish(ObservationEvent::RequestQueued {
            service_id: self.service_id.clone(),
            footprint,
        });
    }

    fn launch_inline(&mut self, envelope: BusinessEnvelope<S::Request>) {
        let service = Arc::clone(&self.service);
        let BusinessEnvelope {
            context,
            request,
            reply,
        } = envelope;
        let context = context.with_cancellation_owner(&self.cancellation);
        self.running.push(
            async move {
                let result = service.handle(request, context).await;
                Completion {
                    kind: CompletionKind::Inline { reply },
                    result,
                }
            }
            .boxed(),
        );
    }

    fn launch_managed(&mut self, intent: Intent<S>) {
        let service = Arc::clone(&self.service);
        let Intent {
            context,
            request,
            meta,
            waiters,
        } = intent;
        let key = meta.key.clone();
        let task_attempt_id = TaskAttemptId::next();
        let parent_task_attempt_id = context.parent_task_attempt_id();
        let context = context
            .with_cancellation_owner(&self.cancellation)
            .with_task_attempt(task_attempt_id);
        let operation_id = context.operation_id();
        let call_id = context.call_id();
        self.actors
            .entry(key.clone())
            .or_insert_with(|| ObjectActor::new(key.clone()))
            .activate(meta.clone(), task_attempt_id, context.clone(), waiters)
            .expect("admission launches only an idle object actor");
        self.observation.publish(ObservationEvent::TaskStarted {
            service_id: self.service_id.clone(),
            task_attempt_id,
            parent_task_attempt_id,
            operation_id,
            call_id,
            footprint: meta.footprint,
            kind: format!("{:?}", meta.kind).into(),
        });
        self.running.push(
            async move {
                let result = service.handle(request, context.clone()).await;
                Completion {
                    kind: CompletionKind::Managed {
                        key,
                        task_attempt_id,
                        context,
                    },
                    result,
                }
            }
            .boxed(),
        );
    }

    fn handle_completion(&mut self, completion: Completion<ResponseOf<S>>) {
        match completion.kind {
            CompletionKind::Inline { reply } => {
                let _ = reply.send(completion.result);
            }
            CompletionKind::Managed {
                key,
                task_attempt_id,
                context,
            } => {
                let Some(settled) = self
                    .actors
                    .get_mut(&key)
                    .and_then(|actor| actor.settle(task_attempt_id))
                else {
                    return;
                };
                let outcome = match &completion.result {
                    Ok(_) => TaskOutcome::Completed,
                    Err(RuntimeError::Cancelled) => TaskOutcome::Cancelled,
                    Err(error) => TaskOutcome::Failed(error.to_string().into()),
                };
                self.observation.publish(ObservationEvent::TaskFinished {
                    service_id: self.service_id.clone(),
                    task_attempt_id,
                    operation_id: context.operation_id(),
                    outcome,
                });
                Self::reply_all(settled.waiters, completion.result);
            }
        }
    }

    fn launch_pending(&mut self) {
        if matches!(
            self.lifecycle,
            ServiceLifecycle::Stopping | ServiceLifecycle::Stopped
        ) {
            return;
        }
        let budget = self.pending_count();
        for _ in 0..budget {
            if self.active_count() >= self.config.max_active_workflows {
                break;
            }
            let key = self
                .actors
                .iter()
                .find_map(|(key, actor)| actor.has_ready().then(|| key.clone()));
            let Some(key) = key else {
                break;
            };
            let intent = self
                .actors
                .get_mut(&key)
                .and_then(ObjectActor::take_ready)
                .expect("ready actor owns a pending intent");
            self.admit_intent(intent);
        }
    }

    fn handle_control(&mut self, control: ControlRequest) {
        match control {
            ControlRequest::Pause(reply) => {
                if self.lifecycle == ServiceLifecycle::Running {
                    self.set_lifecycle(ServiceLifecycle::Paused);
                }
                let _ = reply.send(Ok(()));
            }
            ControlRequest::Resume(reply) => {
                if self.lifecycle == ServiceLifecycle::Paused {
                    self.set_lifecycle(ServiceLifecycle::Running);
                }
                let _ = reply.send(Ok(()));
            }
            ControlRequest::Drain(reply) => {
                if matches!(
                    self.lifecycle,
                    ServiceLifecycle::Stopping | ServiceLifecycle::Stopped
                ) {
                    let _ = reply.send(Err(RuntimeError::ServiceStopping(self.service_id.clone())));
                } else {
                    self.set_lifecycle(ServiceLifecycle::Draining);
                    self.drain_waiters.push(reply);
                }
            }
            ControlRequest::Stop(reply) => {
                self.set_lifecycle(ServiceLifecycle::Stopping);
                self.stop_waiters.push(reply);
                self.cancellation
                    .request(CancelCause::new("service stopping"));
                self.cancel_active(CancelCause::new("service stopping"));
                self.reject_pending(RuntimeError::ServiceStopping(self.service_id.clone()));
            }
        }
    }

    fn cancel_active(&self, cause: CancelCause) {
        for actor in self.actors.values() {
            if let Some(notice) = actor.request_cancel(cause.clone()) {
                self.observation
                    .publish(ObservationEvent::TaskCancelRequested {
                        service_id: self.service_id.clone(),
                        task_attempt_id: notice.task_attempt_id,
                        operation_id: notice.operation_id,
                        cause: cause.clone(),
                    });
            }
        }
    }

    fn reject_pending(&mut self, error: RuntimeError) {
        for actor in self.actors.values_mut() {
            Self::reply_all(actor.reject_pending(), Err(error.clone()));
        }
    }

    fn active_count(&self) -> usize {
        self.actors
            .values()
            .filter(|actor| actor.is_active())
            .count()
    }

    fn pending_count(&self) -> usize {
        self.actors.values().map(ObjectActor::pending_count).sum()
    }

    fn finish_lifecycle_if_ready(&mut self) {
        let settled =
            self.running.is_empty() && self.active_count() == 0 && self.pending_count() == 0;
        if !settled {
            return;
        }
        match self.lifecycle {
            ServiceLifecycle::Draining => {
                self.set_lifecycle(ServiceLifecycle::Paused);
                for waiter in self.drain_waiters.drain(..) {
                    let _ = waiter.send(Ok(()));
                }
            }
            ServiceLifecycle::Stopping => {
                self.set_lifecycle(ServiceLifecycle::Stopped);
                for waiter in self.stop_waiters.drain(..) {
                    let _ = waiter.send(Ok(()));
                }
            }
            _ => {}
        }
        self.publish_snapshot();
    }

    fn set_lifecycle(&mut self, lifecycle: ServiceLifecycle) {
        self.lifecycle = lifecycle;
        self.observation
            .publish(ObservationEvent::LifecycleChanged {
                service_id: self.service_id.clone(),
                lifecycle,
            });
    }

    fn publish_snapshot(&mut self) {
        let activity = if self.running.is_empty() && self.pending_count() == 0 {
            Activity::Idle
        } else {
            Activity::Busy
        };
        if activity != self.last_snapshot.activity {
            self.observation.publish(ObservationEvent::ActivityChanged {
                service_id: self.service_id.clone(),
                activity,
            });
        }
        let snapshot = ServiceSnapshot {
            service_id: self.service_id.clone(),
            lifecycle: self.lifecycle,
            activity,
            running_futures: self.running.len(),
            active_tasks: self.active_count(),
            queued_requests: self.pending_count(),
        };
        self.observation.publish_snapshot(snapshot.clone());
        self.last_snapshot = snapshot;
    }

    fn reply_intent(intent: Intent<S>, result: RuntimeResult<ResponseOf<S>>) {
        Self::reply_all(intent.waiters, result);
    }

    fn reply_all(waiters: Vec<Reply<ResponseOf<S>>>, result: RuntimeResult<ResponseOf<S>>) {
        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }
}
