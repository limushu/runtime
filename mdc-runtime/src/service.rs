use std::{collections::HashMap, fmt::Debug, future::Future, hash::Hash};

use tokio::{
    sync::{mpsc, oneshot, watch},
    task::AbortHandle,
};

use crate::task::TaskSet;
use crate::{
    CancelMode, CancelReason, HandlerRegistry, Message, MessageContext, MessagePayload, Router,
    RunOutcome, RuntimeError, ServiceActivity, ServiceLifecycle, ServiceObserver, ServiceSnapshot,
    TaskContext, TaskKey, TaskOutcome, TaskVisibility,
};

pub trait ServiceKey: Clone + Debug + Eq + Hash + Send + Sync + 'static {}

impl<T> ServiceKey for T where T: Clone + Debug + Eq + Hash + Send + Sync + 'static {}

struct Envelope<M>
where
    M: Message,
{
    message: M,
    context: MessageContext,
}

pub struct CommandHandle<K, M>
where
    K: ServiceKey,
    M: Message,
{
    service: K,
    sender: mpsc::Sender<Envelope<M>>,
    status: watch::Receiver<ServiceSnapshot<K>>,
}

impl<K, M> CommandHandle<K, M>
where
    K: ServiceKey,
    M: Message,
{
    pub async fn send(&self, message: M, context: MessageContext) -> Result<(), RuntimeError> {
        let lifecycle = self.status.borrow().lifecycle;
        if lifecycle != ServiceLifecycle::Running {
            return Err(RuntimeError::ServiceUnavailable(format!(
                "{:?} is {lifecycle:?}",
                self.service
            )));
        }
        self.sender
            .send(Envelope { message, context })
            .await
            .map_err(|_| RuntimeError::ChannelClosed(format!("{:?}", self.service)))
    }

    pub async fn send_payload<P>(
        &self,
        payload: P,
        context: MessageContext,
    ) -> Result<(), RuntimeError>
    where
        P: MessagePayload<M>,
    {
        self.send(payload.into_message(), context).await
    }
}

impl<K, M> Clone for CommandHandle<K, M>
where
    K: ServiceKey,
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            sender: self.sender.clone(),
            status: self.status.clone(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceShutdown {
    Graceful,
    Immediate,
}

enum ServiceControl {
    Pause(oneshot::Sender<()>),
    Resume(oneshot::Sender<()>),
    Drain(oneshot::Sender<()>),
    Shutdown(ServiceShutdown, oneshot::Sender<()>),
}

#[derive(Clone)]
pub struct ControlHandle<K>
where
    K: ServiceKey,
{
    service: K,
    sender: mpsc::Sender<ServiceControl>,
}

impl<K> ControlHandle<K>
where
    K: ServiceKey,
{
    pub async fn pause(&self) -> Result<(), RuntimeError> {
        self.request(ServiceControl::Pause).await
    }

    pub async fn resume(&self) -> Result<(), RuntimeError> {
        self.request(ServiceControl::Resume).await
    }

    pub async fn drain(&self) -> Result<(), RuntimeError> {
        self.request(ServiceControl::Drain).await
    }

    pub async fn shutdown(&self, mode: ServiceShutdown) -> Result<(), RuntimeError> {
        let (reply, ticket) = oneshot::channel();
        self.sender
            .send(ServiceControl::Shutdown(mode, reply))
            .await
            .map_err(|_| RuntimeError::ChannelClosed(format!("{:?}", self.service)))?;
        ticket
            .await
            .map_err(|_| RuntimeError::ChannelClosed(format!("{:?}", self.service)))
    }

    async fn request(
        &self,
        build: fn(oneshot::Sender<()>) -> ServiceControl,
    ) -> Result<(), RuntimeError> {
        let (reply, ticket) = oneshot::channel();
        self.sender
            .send(build(reply))
            .await
            .map_err(|_| RuntimeError::ChannelClosed(format!("{:?}", self.service)))?;
        ticket
            .await
            .map_err(|_| RuntimeError::ChannelClosed(format!("{:?}", self.service)))
    }
}

struct ServiceTaskGuard {
    abort: AbortHandle,
}

impl Drop for ServiceTaskGuard {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

pub struct ServiceEntry<K, M>
where
    K: ServiceKey,
    M: Message,
{
    command: CommandHandle<K, M>,
    control: ControlHandle<K>,
    observer: ServiceObserver<K>,
    _task: ServiceTaskGuard,
}

impl<K, M> ServiceEntry<K, M>
where
    K: ServiceKey,
    M: Message,
{
    pub fn reference(&self) -> ServiceRef<K, M> {
        ServiceRef {
            command: self.command.clone(),
            control: self.control.clone(),
            observer: self.observer.clone(),
        }
    }
}

#[derive(Clone)]
pub struct ServiceRef<K, M>
where
    K: ServiceKey,
    M: Message,
{
    pub command: CommandHandle<K, M>,
    pub control: ControlHandle<K>,
    pub observer: ServiceObserver<K>,
}

pub struct ServiceContext<K, M, S>
where
    K: ServiceKey,
    M: Message,
    S: Send + 'static,
{
    key: K,
    state: S,
    current: Option<MessageContext>,
    router: Router<K, M>,
    tasks: TaskSet<K, M>,
}

impl<K, M, S> ServiceContext<K, M, S>
where
    K: ServiceKey,
    M: Message,
    S: Send + 'static,
{
    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn state(&self) -> &S {
        &self.state
    }

    pub fn state_mut(&mut self) -> &mut S {
        &mut self.state
    }

    pub fn message_context(&self) -> MessageContext {
        self.current
            .expect("handlers always have a message context")
    }

    pub fn router(&self) -> Router<K, M> {
        self.router.clone()
    }

    pub fn task_count(&self) -> usize {
        self.tasks.len()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run<T, Fut, Work, Complete>(
        &mut self,
        key: TaskKey,
        label: impl Into<String>,
        visibility: TaskVisibility,
        work: Work,
        complete: Complete,
    ) -> RunOutcome
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, RuntimeError>> + Send + 'static,
        Work: FnOnce(TaskContext<K, M>) -> Fut + Send + 'static,
        Complete: FnOnce(TaskOutcome<T>) -> M + Send + 'static,
    {
        self.tasks.run(
            key,
            label,
            visibility,
            self.message_context(),
            work,
            complete,
        )
    }

    pub fn task_is_running(&self, key: &TaskKey) -> bool {
        self.tasks.contains(key)
    }

    pub fn cancel_task(&mut self, key: &TaskKey, mode: CancelMode, reason: CancelReason) -> bool {
        self.tasks.cancel(key, mode, reason)
    }
}

struct ServiceRuntime<K, M, S>
where
    K: ServiceKey,
    M: Message,
    S: Send + 'static,
{
    registry: HandlerRegistry<K, M, S>,
    context: ServiceContext<K, M, S>,
    command_rx: mpsc::Receiver<Envelope<M>>,
    control_rx: mpsc::Receiver<ServiceControl>,
    status_tx: watch::Sender<ServiceSnapshot<K>>,
    lifecycle: ServiceLifecycle,
    drain_reply: Option<oneshot::Sender<()>>,
    shutdown_reply: Option<oneshot::Sender<()>>,
    shutdown_after_drain: bool,
    last_activity: ServiceActivity,
}

impl<K, M, S> ServiceRuntime<K, M, S>
where
    K: ServiceKey,
    M: Message,
    S: Send + 'static,
{
    async fn run(mut self) {
        self.lifecycle = ServiceLifecycle::Running;
        self.publish();

        loop {
            self.finish_transition_if_ready();
            if self.lifecycle == ServiceLifecycle::Stopping {
                self.stop().await;
                break;
            }

            for _ in 0..8 {
                match self.control_rx.try_recv() {
                    Ok(control) => self.handle_control(control),
                    Err(_) => break,
                }
            }
            self.finish_transition_if_ready();
            self.publish();

            if self.lifecycle == ServiceLifecycle::Stopping {
                continue;
            }

            let receive_commands = matches!(
                self.lifecycle,
                ServiceLifecycle::Running | ServiceLifecycle::Draining
            );
            tokio::select! {
                biased;
                control = self.control_rx.recv() => {
                    if let Some(control) = control {
                        self.handle_control(control);
                    }
                }
                completion = self.context.tasks.next_event(), if !self.context.tasks.is_empty() => {
                    if let Some(completion) = completion {
                        self.dispatch(completion.message, completion.context);
                    }
                }
                envelope = self.command_rx.recv(), if receive_commands => {
                    if let Some(envelope) = envelope {
                        self.dispatch(envelope.message, envelope.context);
                    }
                }
                else => tokio::task::yield_now().await,
            }
        }
    }

    fn dispatch(&mut self, message: M, context: MessageContext) {
        let kind = message.kind();
        self.context.current = Some(context);
        if let Some(handler) = self.registry.handler(kind) {
            let _ = handler(&mut self.context, message);
        }
        self.context.current = None;
    }

    fn handle_control(&mut self, control: ServiceControl) {
        match control {
            ServiceControl::Pause(reply) => {
                if self.lifecycle == ServiceLifecycle::Running {
                    self.lifecycle = ServiceLifecycle::Paused;
                }
                let _ = reply.send(());
            }
            ServiceControl::Resume(reply) => {
                if self.lifecycle == ServiceLifecycle::Paused {
                    self.lifecycle = ServiceLifecycle::Running;
                }
                let _ = reply.send(());
            }
            ServiceControl::Drain(reply) => {
                self.lifecycle = ServiceLifecycle::Draining;
                self.drain_reply = Some(reply);
                self.shutdown_after_drain = false;
            }
            ServiceControl::Shutdown(ServiceShutdown::Graceful, reply) => {
                self.lifecycle = ServiceLifecycle::Draining;
                self.shutdown_reply = Some(reply);
                self.shutdown_after_drain = true;
            }
            ServiceControl::Shutdown(ServiceShutdown::Immediate, reply) => {
                self.shutdown_reply = Some(reply);
                self.lifecycle = ServiceLifecycle::Stopping;
            }
        }
        self.publish();
    }

    fn finish_transition_if_ready(&mut self) {
        if self.lifecycle != ServiceLifecycle::Draining
            || !self.command_rx.is_empty()
            || !self.context.tasks.is_empty()
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
        self.publish();
        self.context
            .tasks
            .cancel_all(CancelMode::Force, CancelReason::ServiceStopping);
        while !self.context.tasks.is_empty() {
            let _ = self.context.tasks.next_event().await;
        }
        self.registry.shutdown(&mut self.context.state);
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
        let queued = self.command_rx.len();
        let running = self.context.tasks.len();
        let activity = if queued == 0 && running == 0 {
            ServiceActivity::Idle
        } else {
            ServiceActivity::Busy
        };
        if activity != self.last_activity {
            self.registry.activity(&mut self.context.state, activity);
            self.last_activity = activity;
        }
        self.status_tx.send_replace(ServiceSnapshot {
            service: self.context.key.clone(),
            lifecycle: self.lifecycle,
            activity,
            queued_messages: queued,
            running_tasks: running,
        });
    }
}

pub struct PoolServices<K, M>
where
    K: ServiceKey,
    M: Message,
{
    router: Router<K, M>,
    entries: HashMap<K, ServiceEntry<K, M>>,
}

impl<K, M> PoolServices<K, M>
where
    K: ServiceKey,
    M: Message,
{
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            entries: HashMap::new(),
        }
    }

    pub fn router(&self) -> Router<K, M> {
        self.router.clone()
    }

    pub async fn spawn<S>(
        &mut self,
        key: K,
        state: S,
        registry: HandlerRegistry<K, M, S>,
        queue_capacity: usize,
    ) -> Result<ServiceRef<K, M>, RuntimeError>
    where
        S: Send + 'static,
    {
        let (command_tx, command_rx) = mpsc::channel(queue_capacity);
        let (control_tx, control_rx) = mpsc::channel(16);
        let initial = ServiceSnapshot {
            service: key.clone(),
            lifecycle: ServiceLifecycle::Initializing,
            activity: ServiceActivity::Idle,
            queued_messages: 0,
            running_tasks: 0,
        };
        let (status_tx, status_rx) = watch::channel(initial);
        let mut ready_rx = status_rx.clone();
        let (task_tx, _) = tokio::sync::broadcast::channel(256);
        let command = CommandHandle {
            service: key.clone(),
            sender: command_tx,
            status: status_rx.clone(),
        };
        let control = ControlHandle {
            service: key.clone(),
            sender: control_tx,
        };
        let observer = ServiceObserver::new(status_rx, task_tx.clone());
        let context = ServiceContext {
            key: key.clone(),
            state,
            current: None,
            router: self.router.clone(),
            tasks: TaskSet::new(key.clone(), self.router.clone(), task_tx),
        };
        let runtime = ServiceRuntime {
            registry,
            context,
            command_rx,
            control_rx,
            status_tx,
            lifecycle: ServiceLifecycle::Initializing,
            drain_reply: None,
            shutdown_reply: None,
            shutdown_after_drain: false,
            last_activity: ServiceActivity::Idle,
        };
        let join = tokio::spawn(runtime.run());
        let entry = ServiceEntry {
            command: command.clone(),
            control: control.clone(),
            observer: observer.clone(),
            _task: ServiceTaskGuard {
                abort: join.abort_handle(),
            },
        };
        let reference = entry.reference();
        self.router.register(key.clone(), command).await;
        self.entries.insert(key, entry);
        while ready_rx.borrow().lifecycle == ServiceLifecycle::Initializing {
            ready_rx
                .changed()
                .await
                .map_err(|_| RuntimeError::ChannelClosed("service startup".into()))?;
        }
        Ok(reference)
    }

    pub fn service(&self, key: &K) -> Option<ServiceRef<K, M>> {
        self.entries.get(key).map(ServiceEntry::reference)
    }

    pub async fn shutdown_all(&self, mode: ServiceShutdown) -> Result<(), RuntimeError> {
        for entry in self.entries.values() {
            entry.control.shutdown(mode).await?;
        }
        Ok(())
    }
}

impl<K, M> Default for PoolServices<K, M>
where
    K: ServiceKey,
    M: Message,
{
    fn default() -> Self {
        Self::new()
    }
}
