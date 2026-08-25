use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, oneshot, watch};

use crate::{
    CancelReason, HandleResult, RuntimeError, Service, ServiceActivity, ServiceKey,
    ServiceLifecycle, ServiceObserver, ServiceRef, ServiceSnapshot, ShutdownMode, TaskSnapshot,
    service::{
        RequestEnvelope, RuntimeControl, ServiceTaskGuard, SpawnedService, client, control,
        lifecycle_snapshot,
    },
};

use super::task_set::TaskSet;

struct ServiceRuntime<K, S>
where
    K: ServiceKey,
    S: Service,
{
    key: K,
    service: Arc<S>,
    requests: mpsc::Receiver<RequestEnvelope<S>>,
    controls: mpsc::Receiver<RuntimeControl>,
    control_handle: crate::ControlHandle,
    tasks: TaskSet<K, S::Response, S::Error>,
    lifecycle: ServiceLifecycle,
    lifecycle_tx: watch::Sender<ServiceLifecycle>,
    status_tx: watch::Sender<ServiceSnapshot<K>>,
    tasks_tx: watch::Sender<Vec<TaskSnapshot<K>>>,
    activity: ServiceActivity,
    drain_reply: Option<oneshot::Sender<()>>,
    shutdown_reply: Option<oneshot::Sender<()>>,
    shutdown_after_drain: bool,
}

impl<K, S> ServiceRuntime<K, S>
where
    K: ServiceKey,
    S: Service,
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
                finished = self.tasks.next_finished(), if self.tasks.has_running() => {
                    let _ = finished;
                }
                envelope = self.requests.recv(), if receive_requests => {
                    if let Some(envelope) = envelope {
                        self.handle_request(envelope);
                    }
                }
                else => break,
            }
            self.publish();
        }
    }

    fn handle_request(&mut self, envelope: RequestEnvelope<S>) {
        let result = self
            .service
            .clone()
            .handle(envelope.request, envelope.context.clone());
        match result {
            HandleResult::Reply(reply) => {
                let _ = envelope.accepted.send(Ok(crate::Submission::Reply(reply)));
            }
            HandleResult::Task(task) => {
                let submission = self
                    .tasks
                    .submit(task, envelope.context, self.control_handle.clone())
                    .map(crate::Submission::Task);
                let _ = envelope.accepted.send(submission);
            }
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
                let _ = reply.send(self.tasks.request_cancel(task_id, reason, false));
            }
        }
    }

    fn finish_drain_if_ready(&mut self) {
        if self.lifecycle != ServiceLifecycle::Draining
            || !self.requests.is_empty()
            || !self.tasks.is_empty()
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
        self.tasks.cancel_all(CancelReason::ServiceStopping, true);
        self.publish();
        while self.tasks.has_running() {
            let _ = self.tasks.next_finished().await;
            self.publish();
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
        let activity = if self.requests.is_empty() && self.tasks.is_empty() {
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
            managed_tasks: self.tasks.len(),
        });
        self.tasks_tx.send_replace(self.tasks.snapshots());
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
    S: Service,
{
    let (request_tx, request_rx) = mpsc::channel(queue_capacity);
    let (control_tx, control_rx) = mpsc::channel(32);
    let (lifecycle_tx, lifecycle_rx) = watch::channel(ServiceLifecycle::Initializing);
    let (status_tx, status_rx) = watch::channel(lifecycle_snapshot(
        key.clone(),
        ServiceLifecycle::Initializing,
    ));
    let (tasks_tx, tasks_rx) = watch::channel(Vec::new());
    let (events, _) = broadcast::channel(256);
    let client = client(name.clone(), request_tx, lifecycle_rx.clone());
    let control = control(name, control_tx);
    let observer = ServiceObserver::new(status_rx, tasks_rx, events.clone());
    let runtime = ServiceRuntime {
        key: key.clone(),
        service,
        requests: request_rx,
        controls: control_rx,
        control_handle: control.clone(),
        tasks: TaskSet::new(key.clone(), events),
        lifecycle: ServiceLifecycle::Initializing,
        lifecycle_tx,
        status_tx,
        tasks_tx,
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
