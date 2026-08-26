use std::{collections::HashMap, sync::Arc};

use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    RuntimeError, Service, ServiceActivity, ServiceKey, ServiceLifecycle, ServiceObserver,
    ServiceRef, ServiceSnapshot, ShutdownMode, Submission, TaskExit,
    service::{
        Accepted, RequestEnvelope, RuntimeControl, ServiceTaskGuard, SpawnedService, client,
        control, lifecycle_snapshot,
    },
    task::HandlerOutcome,
};

use super::handler_set::{FinishedHandler, HandlerExit, HandlerSet};

type ManagedCompletion<S> =
    oneshot::Sender<TaskExit<<S as Service>::Response, <S as Service>::Error>>;

struct ServiceRuntime<K, S>
where
    K: ServiceKey,
    S: Service<Key = K>,
{
    key: K,
    service: Arc<S>,
    requests: mpsc::Receiver<RequestEnvelope<S>>,
    controls: mpsc::Receiver<RuntimeControl>,
    control_handle: crate::ControlHandle,
    handlers: HandlerSet<S::Response, S::Error>,
    pending_submissions: HashMap<crate::RequestId, oneshot::Sender<Accepted<S>>>,
    managed_completions: HashMap<crate::RequestId, ManagedCompletion<S>>,
    task_updates: watch::Receiver<Vec<crate::TaskSnapshot<K>>>,
    lifecycle: ServiceLifecycle,
    lifecycle_tx: watch::Sender<ServiceLifecycle>,
    status_tx: watch::Sender<ServiceSnapshot<K>>,
    activity: ServiceActivity,
    drain_reply: Option<oneshot::Sender<()>>,
    shutdown_reply: Option<oneshot::Sender<()>>,
    shutdown_after_drain: bool,
}

impl<K, S> ServiceRuntime<K, S>
where
    K: ServiceKey,
    S: Service<Key = K>,
{
    async fn run(mut self) {
        self.lifecycle = ServiceLifecycle::Running;
        self.publish();

        loop {
            self.finish_drain_if_ready();
            if self.lifecycle == ServiceLifecycle::Stopping {
                self.stop().await;
                break;
            }

            let receive_requests = matches!(
                self.lifecycle,
                ServiceLifecycle::Running | ServiceLifecycle::Draining
            );

            tokio::select! {
                biased;
                control = self.controls.recv() => {
                    if let Some(control) = control {
                        self.handle_control(control);
                    }
                }
                finished = self.handlers.next_finished(), if self.handlers.has_running() => {
                    if let Some(finished) = finished {
                        self.finish_handler(finished);
                    }
                }
                changed = self.task_updates.changed() => {
                    let _ = changed;
                }
                envelope = self.requests.recv(), if receive_requests => {
                    if let Some(envelope) = envelope {
                        self.start_handler(envelope);
                    }
                }
                else => break,
            }
            self.acknowledge_managed_requests();
            self.publish();
        }
    }

    fn start_handler(&mut self, envelope: RequestEnvelope<S>) {
        let request_id = envelope.context.request_id;
        self.pending_submissions
            .insert(request_id, envelope.accepted);
        self.handlers
            .start(self.service.clone(), envelope.request, envelope.context);
    }

    fn acknowledge_managed_requests(&mut self) {
        let managed: Vec<_> = self
            .pending_submissions
            .keys()
            .filter_map(|request_id| {
                self.service
                    .task_manager()
                    .task_for_request(*request_id)
                    .map(|task_id| (*request_id, task_id))
            })
            .collect();

        for (request_id, task_id) in managed {
            let Some(accepted) = self.pending_submissions.remove(&request_id) else {
                continue;
            };
            let (completion, ticket) = oneshot::channel();
            let submission = Submission::Task(crate::TaskTicket::new(
                task_id,
                self.control_handle.clone(),
                ticket,
            ));
            if accepted.send(Ok(submission)).is_ok() {
                self.managed_completions.insert(request_id, completion);
            }
        }
    }

    fn finish_handler(&mut self, finished: FinishedHandler<S::Response, S::Error>) {
        self.acknowledge_one(finished.request_id);
        let cancellation = self
            .service
            .task_manager()
            .cancellation_for_request(finished.request_id);
        let outcome = match &finished.exit {
            HandlerExit::Finished(Ok(_)) => HandlerOutcome::Completed,
            HandlerExit::Finished(Err(_)) => HandlerOutcome::Failed,
            HandlerExit::Aborted => HandlerOutcome::Aborted,
        };
        let cancellation = self
            .service
            .task_manager()
            .finish_request(finished.request_id, outcome)
            .or(cancellation);

        if let Some(completion) = self.managed_completions.remove(&finished.request_id) {
            let exit = match finished.exit {
                HandlerExit::Aborted => TaskExit::Aborted,
                HandlerExit::Finished(Ok(_)) if cancellation.is_some() => {
                    TaskExit::Cancelled(cancellation.expect("cancellation exists"))
                }
                HandlerExit::Finished(Err(_)) if cancellation.is_some() => {
                    TaskExit::Cancelled(cancellation.expect("cancellation exists"))
                }
                HandlerExit::Finished(Ok(value)) => TaskExit::Completed(value),
                HandlerExit::Finished(Err(error)) => TaskExit::Failed(error),
            };
            let _ = completion.send(exit);
            return;
        }

        if let Some(accepted) = self.pending_submissions.remove(&finished.request_id) {
            let result = match finished.exit {
                HandlerExit::Finished(result) => Ok(Submission::Reply(result)),
                HandlerExit::Aborted => Err(RuntimeError::ResponseDropped),
            };
            let _ = accepted.send(result);
        }
    }

    fn acknowledge_one(&mut self, request_id: crate::RequestId) {
        let Some(task_id) = self.service.task_manager().task_for_request(request_id) else {
            return;
        };
        let Some(accepted) = self.pending_submissions.remove(&request_id) else {
            return;
        };
        let (completion, ticket) = oneshot::channel();
        let submission = Submission::Task(crate::TaskTicket::new(
            task_id,
            self.control_handle.clone(),
            ticket,
        ));
        if accepted.send(Ok(submission)).is_ok() {
            self.managed_completions.insert(request_id, completion);
        }
    }

    fn handle_control(&mut self, control: RuntimeControl) {
        match control {
            RuntimeControl::Pause(reply) => {
                if self.lifecycle == ServiceLifecycle::Running {
                    self.lifecycle = ServiceLifecycle::Paused;
                }
                let _ = reply.send(());
            }
            RuntimeControl::Resume(reply) => {
                if self.lifecycle == ServiceLifecycle::Paused {
                    self.lifecycle = ServiceLifecycle::Running;
                }
                let _ = reply.send(());
            }
            RuntimeControl::Drain(reply) => {
                self.lifecycle = ServiceLifecycle::Draining;
                self.drain_reply = Some(reply);
                self.shutdown_after_drain = false;
            }
            RuntimeControl::Shutdown(ShutdownMode::Graceful, reply) => {
                self.lifecycle = ServiceLifecycle::Draining;
                self.shutdown_reply = Some(reply);
                self.shutdown_after_drain = true;
            }
            RuntimeControl::Shutdown(ShutdownMode::Immediate, reply) => {
                self.shutdown_reply = Some(reply);
                self.lifecycle = ServiceLifecycle::Stopping;
            }
            RuntimeControl::CancelTask {
                task_id,
                reason,
                reply,
            } => {
                let _ = reply.send(self.service.task_manager().cancel(task_id, reason));
            }
        }
    }

    fn finish_drain_if_ready(&mut self) {
        if self.lifecycle != ServiceLifecycle::Draining
            || !self.requests.is_empty()
            || !self.handlers.is_empty()
        {
            return;
        }
        if self.shutdown_after_drain {
            self.lifecycle = ServiceLifecycle::Stopping;
        } else {
            self.lifecycle = ServiceLifecycle::Paused;
            if let Some(reply) = self.drain_reply.take() {
                let _ = reply.send(());
            }
        }
        self.publish();
    }

    async fn stop(&mut self) {
        self.lifecycle = ServiceLifecycle::Stopping;
        self.handlers.abort_all();
        self.service.task_manager().abort_all();
        self.publish();
        while self.handlers.has_running() {
            if let Some(finished) = self.handlers.next_finished().await {
                self.finish_handler(finished);
                self.publish();
            }
        }
        self.service.on_shutdown();
        self.lifecycle = ServiceLifecycle::Stopped;
        self.publish();
        if let Some(reply) = self.drain_reply.take() {
            let _ = reply.send(());
        }
        if let Some(reply) = self.shutdown_reply.take() {
            let _ = reply.send(());
        }
    }

    fn publish(&mut self) {
        let activity = if self.requests.is_empty() && self.handlers.is_empty() {
            ServiceActivity::Idle
        } else {
            ServiceActivity::Busy
        };
        if activity != self.activity {
            self.activity = activity;
            self.service.on_activity(activity);
        }
        self.lifecycle_tx.send_replace(self.lifecycle);
        self.status_tx.send_replace(ServiceSnapshot {
            service: self.key.clone(),
            lifecycle: self.lifecycle,
            activity,
            queued_requests: self.requests.len(),
            inflight_requests: self.handlers.len(),
            managed_tasks: self.service.task_manager().len(),
        });
    }
}

pub(crate) async fn spawn_service<K, S>(
    key: K,
    name: Arc<str>,
    service: Arc<S>,
    queue_capacity: usize,
) -> Result<SpawnedService<K, S>, RuntimeError>
where
    K: ServiceKey,
    S: Service<Key = K>,
{
    let (request_tx, request_rx) = mpsc::channel(queue_capacity);
    let (control_tx, control_rx) = mpsc::channel(32);
    let (lifecycle_tx, lifecycle_rx) = watch::channel(ServiceLifecycle::Initializing);
    let (status_tx, status_rx) = watch::channel(lifecycle_snapshot(
        key.clone(),
        ServiceLifecycle::Initializing,
    ));
    let task_updates = service.task_manager().watch_snapshots();
    let client = client(name.clone(), request_tx, lifecycle_rx.clone());
    let control = control(name, control_tx);
    let observer = ServiceObserver::new(
        status_rx,
        task_updates.clone(),
        service.task_manager().events(),
    );
    let runtime = ServiceRuntime {
        key: key.clone(),
        service,
        requests: request_rx,
        controls: control_rx,
        control_handle: control.clone(),
        handlers: HandlerSet::new(),
        pending_submissions: HashMap::new(),
        managed_completions: HashMap::new(),
        task_updates,
        lifecycle: ServiceLifecycle::Initializing,
        lifecycle_tx,
        status_tx,
        activity: ServiceActivity::Idle,
        drain_reply: None,
        shutdown_reply: None,
        shutdown_after_drain: false,
    };
    let join = tokio::spawn(runtime.run());
    let mut ready = lifecycle_rx;
    while *ready.borrow() == ServiceLifecycle::Initializing {
        ready
            .changed()
            .await
            .map_err(|_| RuntimeError::ChannelClosed("service startup".into()))?;
    }
    Ok(SpawnedService {
        reference: ServiceRef {
            key,
            client,
            control,
            observer,
        },
        guard: ServiceTaskGuard::new(join.abort_handle()),
    })
}
