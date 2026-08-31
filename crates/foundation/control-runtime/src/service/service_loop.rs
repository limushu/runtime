use super::container::{
    ControlHandle, ControlRequest, ManagedService, Reply, ResponseOf, RuntimeConfig,
};
use super::object_slot::{Intent, ObjectSlot, ReplaceOutcome, Subscriber, SubscriberGroup};
use crate::context::CancellationScope;
use crate::observation::{ObservationHub, TaskOutcome};
use crate::router::{BusinessEnvelope, BusinessHandle};
use crate::{
    Activity, Admission, CancelCause, ObjectActivity, ObjectKey, ObservationEvent, OrphanPolicy,
    RequestRoute, Router, RuntimeError, RuntimeResult, Service, ServiceId, ServiceLifecycle,
    ServiceSnapshot, TaskAttemptId, WorkflowContext,
};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;
use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use tokio::sync::mpsc;

enum CompletionKind<R> {
    Untracked {
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

pub(super) struct ServiceLoop<S: Service> {
    service: Arc<S>,
    service_id: ServiceId,
    config: RuntimeConfig,
    lifecycle: ServiceLifecycle,
    business_rx: mpsc::Receiver<BusinessEnvelope<S::Request>>,
    control_rx: mpsc::Receiver<ControlRequest>,
    running: FuturesUnordered<BoxFuture<'static, Completion<ResponseOf<S>>>>,
    slots: HashMap<ObjectKey, ObjectSlot<S>>,
    ready: VecDeque<ObjectKey>,
    ready_set: HashSet<ObjectKey>,
    untracked_in_flight: usize,
    drain_waiters: Vec<Reply<()>>,
    stop_waiters: Vec<Reply<()>>,
    observation: ObservationHub,
    cancellation: CancellationScope,
    last_snapshot: ServiceSnapshot,
}

impl<S: Service> ServiceLoop<S> {
    pub(super) fn spawn(
        service: Arc<S>,
        router: &Router,
        mut config: RuntimeConfig,
    ) -> ManagedService {
        config.max_active_workflows = config.max_active_workflows.max(1);
        config.max_untracked_requests = config.max_untracked_requests.max(1);
        config.max_pending_requests = config.max_pending_requests.max(1);
        config.max_pending_per_object = config.max_pending_per_object.max(1);

        let service_id = service.id();
        let (business_tx, business_rx) = mpsc::channel(config.business_capacity.max(1));
        let (control_tx, control_rx) = mpsc::channel(config.control_capacity.max(1));
        let (observation, observer) = ObservationHub::new(service_id.clone());
        let cancellation = CancellationScope::root();
        router.register(
            BusinessHandle::new(service_id.clone(), business_tx),
            observation.clone(),
        );
        let shutdown_timeout = config.shutdown_timeout;
        let runtime = Self {
            service,
            service_id: service_id.clone(),
            config,
            lifecycle: ServiceLifecycle::Running,
            business_rx,
            control_rx,
            running: FuturesUnordered::new(),
            slots: HashMap::new(),
            ready: VecDeque::new(),
            ready_set: HashSet::new(),
            untracked_in_flight: 0,
            drain_waiters: Vec::new(),
            stop_waiters: Vec::new(),
            observation,
            cancellation: cancellation.clone(),
            last_snapshot: ServiceSnapshot::initial(service_id.clone()),
        };
        let join = tokio::spawn(runtime.run());

        ManagedService::new(
            service_id.clone(),
            ControlHandle::new(service_id, control_tx),
            observer,
            shutdown_timeout,
            cancellation,
            join,
        )
    }

    async fn run(mut self) {
        loop {
            self.finish_lifecycle_if_ready();
            if self.lifecycle == ServiceLifecycle::Stopped {
                return;
            }

            let can_receive_business = self.can_receive_business();
            tokio::select! {
                biased;
                Some(control) = self.control_rx.recv() => self.handle_control(control),
                Some(completion) = self.running.next(), if !self.running.is_empty() => {
                    self.handle_completion(completion);
                }
                Some(envelope) = self.business_rx.recv(), if can_receive_business => {
                    self.handle_business(envelope);
                }
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

    fn can_receive_business(&self) -> bool {
        self.pending_count() < self.config.max_pending_requests
            && self.untracked_in_flight < self.config.max_untracked_requests
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

        let route = catch_unwind(AssertUnwindSafe(|| self.service.route(&envelope.request)));
        match route {
            Ok(RequestRoute::Untracked) => self.launch_untracked(envelope),
            Ok(RequestRoute::Workflow(meta)) => self.admit_intent(Intent::new(envelope, meta)),
            Err(panic) => {
                let _ = envelope.reply.send(Err(panic_error(panic)));
            }
        }
    }

    fn admit_intent(&mut self, intent: Intent<S>) {
        let activity = self
            .slots
            .get(&intent.meta.key)
            .map(ObjectSlot::activity)
            .unwrap_or(ObjectActivity::Idle);
        let decision = catch_unwind(AssertUnwindSafe(|| {
            self.service
                .admit(&intent.origin, &intent.request, &activity)
        }));
        match decision {
            Ok(Ok(decision)) => self.apply_admission(intent, decision),
            Ok(Err(error)) => Self::reply_intent(intent, Err(error)),
            Err(panic) => Self::reply_intent(intent, Err(panic_error(panic))),
        }
    }

    fn apply_admission(&mut self, intent: Intent<S>, decision: Admission<ResponseOf<S>>) {
        let key = intent.meta.key.clone();
        match decision {
            Admission::Start => {
                let busy = self.slots.get(&key).is_some_and(|slot| !slot.is_idle());
                if busy {
                    Self::reply_intent(
                        intent,
                        Err(RuntimeError::Rejected(
                            "domain returned Start for a busy object slot".into(),
                        )),
                    );
                } else {
                    self.start_or_queue(intent);
                }
            }
            Admission::Join => self.join_intent(intent),
            Admission::Queue => {
                if self.slots.get(&key).is_none_or(ObjectSlot::is_idle) {
                    Self::reply_intent(
                        intent,
                        Err(RuntimeError::Rejected(
                            "domain returned Queue for an idle object slot".into(),
                        )),
                    );
                } else {
                    self.queue_intent(intent, true);
                }
            }
            Admission::Replace { cause } => self.replace_intent(intent, cause),
            Admission::Complete(response) => Self::reply_intent(intent, Ok(response)),
            Admission::Reject { reason } => {
                Self::reply_intent(intent, Err(RuntimeError::Rejected(reason.to_string())))
            }
        }
    }

    fn join_intent(&mut self, intent: Intent<S>) {
        let key = intent.meta.key.clone();
        let joined = match self.slots.get_mut(&key) {
            Some(slot) => slot.join(intent),
            None => Err(Box::new(intent)),
        };
        match joined {
            Ok(notice) => {
                for joined_operation_id in notice.joined_operation_ids {
                    self.observation.publish(ObservationEvent::RequestJoined {
                        service_id: self.service_id.clone(),
                        active_operation_id: notice.active_operation_id,
                        joined_operation_id,
                        object: key.clone(),
                    });
                }
            }
            Err(intent) => Self::reply_intent(
                *intent,
                Err(RuntimeError::Internal(
                    "domain selected Join but no matching intent exists".into(),
                )),
            ),
        }
    }

    fn replace_intent(&mut self, intent: Intent<S>, cause: CancelCause) {
        let operation_id = intent.operation_id();
        let object = intent.meta.key.clone();
        let Some(slot) = self.slots.get_mut(&object) else {
            self.start_or_queue(intent);
            return;
        };
        match slot.replace(intent, cause.clone()) {
            Ok(ReplaceOutcome::CancelRequested(notice)) => {
                self.publish_queued(operation_id, object);
                self.observation
                    .publish(ObservationEvent::TaskCancelRequested {
                        service_id: self.service_id.clone(),
                        task_attempt_id: notice.task_attempt_id,
                        operation_id: notice.operation_id,
                        cause,
                    });
            }
            Ok(ReplaceOutcome::ReplacementSuperseded(subscribers)) => {
                self.publish_queued(operation_id, object.clone());
                Self::reply_all(
                    subscribers,
                    Err(RuntimeError::Superseded(
                        "a newer object intent replaced the pending intent".into(),
                    )),
                );
                self.observation.publish(ObservationEvent::RequestMerged {
                    service_id: self.service_id.clone(),
                    operation_id,
                    object,
                });
            }
            Err(intent) => self.start_or_queue(*intent),
        }
    }

    fn start_or_queue(&mut self, intent: Intent<S>) {
        if self.active_count() < self.config.max_active_workflows {
            self.launch_managed(intent);
        } else {
            self.queue_intent(intent, false);
        }
    }

    fn queue_intent(&mut self, intent: Intent<S>, reconsider: bool) {
        let key = intent.meta.key.clone();
        let object_pending = self
            .slots
            .get(&key)
            .map(ObjectSlot::pending_count)
            .unwrap_or_default();
        if self.pending_count() >= self.config.max_pending_requests
            || object_pending >= self.config.max_pending_per_object
        {
            Self::reply_intent(
                intent,
                Err(RuntimeError::Rejected("service queue is full".into())),
            );
            return;
        }

        let operation_id = intent.operation_id();
        let ready = self
            .slots
            .entry(key.clone())
            .or_insert_with(|| ObjectSlot::new(key.clone()))
            .queue(intent, reconsider);
        self.publish_queued(operation_id, key.clone());
        if ready {
            self.mark_ready(key);
        }
    }

    fn publish_queued(&self, operation_id: crate::OperationId, object: ObjectKey) {
        self.observation.publish(ObservationEvent::RequestQueued {
            service_id: self.service_id.clone(),
            operation_id,
            object,
        });
    }

    fn launch_untracked(&mut self, envelope: BusinessEnvelope<S::Request>) {
        let service = Arc::clone(&self.service);
        let BusinessEnvelope {
            context,
            request,
            reply,
        } = envelope;
        let context = context.with_cancellation_owner(&self.cancellation);
        self.untracked_in_flight += 1;
        self.running.push(
            async move {
                let result = AssertUnwindSafe(service.handle(request, context))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|panic| Err(panic_error(panic)));
                Completion {
                    kind: CompletionKind::Untracked { reply },
                    result,
                }
            }
            .boxed(),
        );
    }

    fn launch_managed(&mut self, intent: Intent<S>) {
        let service = Arc::clone(&self.service);
        let Intent {
            origin,
            request,
            meta,
            subscribers,
        } = intent;
        let key = meta.key.clone();
        let task_attempt_id = TaskAttemptId::next();
        let parent_task_attempt_id = origin.parent_task_attempt_id();
        let task_cancellation = CancellationScope::root().linked_to(&self.cancellation);
        let context = origin
            .with_cancellation(task_cancellation)
            .with_task_attempt(task_attempt_id);
        let operation_id = context.operation_id();
        let call_id = context.call_id();
        let orphan_policy = meta.orphan_policy;
        let subscriber_group = self
            .slots
            .entry(key.clone())
            .or_insert_with(|| ObjectSlot::new(key.clone()))
            .activate(meta.clone(), task_attempt_id, context.clone(), subscribers)
            .expect("admission launches only an idle object slot");

        self.observation.publish(ObservationEvent::TaskStarted {
            service_id: self.service_id.clone(),
            task_attempt_id,
            parent_task_attempt_id,
            operation_id,
            call_id,
            object: key.clone(),
            kind: format!("{:?}", meta.kind).into(),
        });

        self.running.push(
            managed_future(
                service,
                request,
                context,
                subscriber_group,
                orphan_policy,
                key,
                task_attempt_id,
            )
            .boxed(),
        );
    }

    fn handle_completion(&mut self, completion: Completion<ResponseOf<S>>) {
        match completion.kind {
            CompletionKind::Untracked { reply } => {
                self.untracked_in_flight = self.untracked_in_flight.saturating_sub(1);
                let _ = reply.send(completion.result);
            }
            CompletionKind::Managed {
                key,
                task_attempt_id,
                context,
            } => {
                let Some((settled, empty)) = self.slots.get_mut(&key).and_then(|slot| {
                    slot.settle(task_attempt_id)
                        .map(|settled| (settled, slot.is_empty()))
                }) else {
                    return;
                };

                if settled.ready {
                    self.mark_ready(key.clone());
                }
                if empty {
                    self.slots.remove(&key);
                }

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
                Self::reply_all(settled.subscribers, completion.result);
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

        let budget = self.ready.len();
        for _ in 0..budget {
            if self.active_count() >= self.config.max_active_workflows {
                break;
            }
            let Some(key) = self.ready.pop_front() else {
                break;
            };
            self.ready_set.remove(&key);
            let Some(pending) = self.slots.get_mut(&key).and_then(ObjectSlot::take_ready) else {
                continue;
            };

            if pending.reconsider {
                self.admit_intent(pending.intent);
            } else {
                self.launch_managed(pending.intent);
            }
        }
    }

    fn mark_ready(&mut self, key: ObjectKey) {
        if self.ready_set.insert(key.clone()) {
            self.ready.push_back(key);
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
        for slot in self.slots.values() {
            if let Some(notice) = slot.request_cancel(cause.clone()) {
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
        for slot in self.slots.values_mut() {
            Self::reply_all(slot.reject_pending(), Err(error.clone()));
        }
        self.ready.clear();
        self.ready_set.clear();
        self.slots.retain(|_, slot| !slot.is_empty());
    }

    fn active_count(&self) -> usize {
        self.slots.values().filter(|slot| slot.is_active()).count()
    }

    fn pending_count(&self) -> usize {
        self.slots.values().map(ObjectSlot::pending_count).sum()
    }

    fn finish_lifecycle_if_ready(&mut self) {
        if !self.running.is_empty() || self.pending_count() > 0 {
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
        Self::reply_all(intent.subscribers, result);
    }

    fn reply_all(
        subscribers: Vec<Subscriber<ResponseOf<S>>>,
        result: RuntimeResult<ResponseOf<S>>,
    ) {
        for subscriber in subscribers {
            let _ = subscriber.reply.send(result.clone());
        }
    }
}

async fn managed_future<S: Service>(
    service: Arc<S>,
    request: S::Request,
    context: WorkflowContext,
    subscriber_group: SubscriberGroup,
    orphan_policy: OrphanPolicy,
    key: ObjectKey,
    task_attempt_id: TaskAttemptId,
) -> Completion<ResponseOf<S>> {
    let future = AssertUnwindSafe(service.handle(request, context.clone())).catch_unwind();
    tokio::pin!(future);

    let result = match orphan_policy {
        OrphanPolicy::Continue => future.await,
        OrphanPolicy::Cancel => {
            tokio::select! {
                result = &mut future => result,
                _ = subscriber_group.all_cancelled() => {
                    context
                        .cancellation()
                        .request(CancelCause::new("all workflow callers dropped"));
                    future.await
                }
            }
        }
    }
    .unwrap_or_else(|panic| Err(panic_error(panic)));

    Completion {
        kind: CompletionKind::Managed {
            key,
            task_attempt_id,
            context,
        },
        result,
    }
}

fn panic_error(panic: Box<dyn Any + Send>) -> RuntimeError {
    let message = panic
        .downcast_ref::<&str>()
        .map(|message| (*message).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "non-string panic payload".to_owned());
    RuntimeError::WorkflowPanicked(message)
}
