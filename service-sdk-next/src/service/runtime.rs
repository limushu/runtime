use super::{Service, TaskSnapshot};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

static NEXT_EXECUTION_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    Initializing,
    Running,
    Paused,
    Draining,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub channel_capacity: usize,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 128,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CallError<E> {
    Unavailable(Lifecycle),
    Stopped,
    Superseded,
    HandlerPanicked(String),
    Protocol(&'static str),
    Business(E),
}

impl<E: fmt::Display> fmt::Display for CallError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(state) => write!(formatter, "service is {state:?}"),
            Self::Stopped => formatter.write_str("service stopped"),
            Self::Superseded => formatter.write_str("command was superseded before execution"),
            Self::HandlerPanicked(message) => write!(formatter, "handler panicked: {message}"),
            Self::Protocol(message) => message.fmt(formatter),
            Self::Business(error) => error.fmt(formatter),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum CommandActivity<'a, C> {
    Idle,
    Running {
        active: &'a C,
        latest: &'a C,
        pending: usize,
        cancelling: bool,
    },
}

impl<'a, C> CommandActivity<'a, C> {
    pub fn latest(self) -> Option<&'a C> {
        match self {
            Self::Idle => None,
            Self::Running { latest, .. } => Some(latest),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandDecision<R> {
    Run,
    Join,
    Queue,
    Replace { cause: String },
    Complete(R),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandReceipt {
    Started { execution: u64 },
    Joined { execution: u64 },
    Queued,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionState {
    Running,
    Cancelling,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionSnapshot {
    pub id: u64,
    pub command_key: Option<String>,
    pub description: String,
    pub state: ExecutionState,
    pub progress: Option<u8>,
    pub milestone: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceActivity {
    Idle,
    Busy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceSnapshot {
    pub revision: u64,
    pub name: &'static str,
    pub lifecycle: Lifecycle,
    pub activity: ServiceActivity,
    pub queued_commands: usize,
    pub active_executions: Vec<ExecutionSnapshot>,
    pub tasks: Vec<TaskSnapshot>,
    pub last_error: Option<String>,
}

#[derive(Clone)]
pub struct CommandContext<K> {
    execution: u64,
    key: Option<K>,
    cancellation: CancellationToken,
    state: Arc<Mutex<RuntimeState>>,
    changed: watch::Sender<u64>,
}

impl<K> CommandContext<K> {
    pub fn execution_id(&self) -> u64 {
        self.execution
    }

    pub fn key(&self) -> Option<&K> {
        self.key.as_ref()
    }

    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub fn progress(&self, progress: u8) {
        self.update(|execution| execution.progress = Some(progress.min(100)));
    }

    pub fn milestone(&self, milestone: impl Into<String>) {
        let milestone = milestone.into();
        self.update(|execution| execution.milestone = Some(milestone));
    }

    fn update(&self, update: impl FnOnce(&mut ExecutionSnapshot)) {
        let revision = {
            let mut state = self.state.lock().expect("service runtime state poisoned");
            let Some(execution) = state.executions.get_mut(&self.execution) else {
                return;
            };
            update(execution);
            state.revision += 1;
            state.revision
        };
        self.changed.send_replace(revision);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlError {
    Stopped,
    InvalidTransition {
        from: Lifecycle,
        operation: &'static str,
    },
}

impl fmt::Display for ControlError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stopped => formatter.write_str("service stopped"),
            Self::InvalidTransition { from, operation } => {
                write!(formatter, "cannot {operation} a service in {from:?}")
            }
        }
    }
}

impl std::error::Error for ControlError {}

enum ControlMessage {
    Pause(oneshot::Sender<Result<(), ControlError>>),
    Resume(oneshot::Sender<Result<(), ControlError>>),
    Drain(oneshot::Sender<Result<(), ControlError>>),
    Stop(oneshot::Sender<Result<(), ControlError>>),
}

#[derive(Clone)]
pub struct ServiceControl {
    sender: mpsc::Sender<ControlMessage>,
}

impl ServiceControl {
    pub async fn pause(&self) -> Result<(), ControlError> {
        self.send(ControlMessage::Pause).await
    }

    pub async fn resume(&self) -> Result<(), ControlError> {
        self.send(ControlMessage::Resume).await
    }

    pub async fn drain(&self) -> Result<(), ControlError> {
        self.send(ControlMessage::Drain).await
    }

    pub async fn stop(&self) -> Result<(), ControlError> {
        self.send(ControlMessage::Stop).await
    }

    async fn send(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<(), ControlError>>) -> ControlMessage,
    ) -> Result<(), ControlError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(make(reply))
            .await
            .map_err(|_| ControlError::Stopped)?;
        response.await.map_err(|_| ControlError::Stopped)?
    }
}

type QueryResult<S> = Result<<S as Service>::QueryReply, CallError<<S as Service>::Error>>;
type CommandResult<S> = Result<<S as Service>::CommandReply, CallError<<S as Service>::Error>>;

struct QueryMessage<S: Service> {
    query: S::Query,
    reply: oneshot::Sender<QueryResult<S>>,
}

enum CommandReply<S: Service> {
    Submit(oneshot::Sender<Result<CommandReceipt, CallError<S::Error>>>),
    Call(oneshot::Sender<CommandResult<S>>),
}

struct CommandMessage<S: Service> {
    command: S::Command,
    reply: CommandReply<S>,
}

pub struct ServiceEndpoint<S: Service> {
    query: mpsc::Sender<QueryMessage<S>>,
    command: mpsc::Sender<CommandMessage<S>>,
}

impl<S: Service> Clone for ServiceEndpoint<S> {
    fn clone(&self) -> Self {
        Self {
            query: self.query.clone(),
            command: self.command.clone(),
        }
    }
}

impl<S: Service> ServiceEndpoint<S> {
    pub async fn query(&self, query: S::Query) -> QueryResult<S> {
        let (reply, response) = oneshot::channel();
        self.query
            .send(QueryMessage { query, reply })
            .await
            .map_err(|_| CallError::Stopped)?;
        response.await.map_err(|_| CallError::Stopped)?
    }

    pub async fn submit(&self, command: S::Command) -> Result<CommandReceipt, CallError<S::Error>> {
        let (reply, response) = oneshot::channel();
        self.command
            .send(CommandMessage {
                command,
                reply: CommandReply::Submit(reply),
            })
            .await
            .map_err(|_| CallError::Stopped)?;
        response.await.map_err(|_| CallError::Stopped)?
    }

    pub async fn call(&self, command: S::Command) -> CommandResult<S> {
        let (reply, response) = oneshot::channel();
        self.command
            .send(CommandMessage {
                command,
                reply: CommandReply::Call(reply),
            })
            .await
            .map_err(|_| CallError::Stopped)?;
        response.await.map_err(|_| CallError::Stopped)?
    }
}

pub struct ServiceObserver<S: Service> {
    service: Arc<S>,
    state: Arc<Mutex<RuntimeState>>,
    changed: watch::Receiver<u64>,
}

impl<S: Service> Clone for ServiceObserver<S> {
    fn clone(&self) -> Self {
        Self {
            service: self.service.clone(),
            state: self.state.clone(),
            changed: self.changed.clone(),
        }
    }
}

impl<S: Service> ServiceObserver<S> {
    pub fn snapshot(&self) -> ServiceSnapshot {
        let (revision, lifecycle, queued_commands, mut active_executions, last_error) = {
            let state = self.state.lock().expect("service runtime state poisoned");
            (
                state.revision,
                state.lifecycle,
                state.queued_commands,
                state.executions.values().cloned().collect::<Vec<_>>(),
                state.last_error.clone(),
            )
        };
        active_executions.sort_by_key(|execution| execution.id);
        ServiceSnapshot {
            revision,
            name: self.service.name(),
            lifecycle,
            activity: if active_executions.is_empty() && queued_commands == 0 {
                ServiceActivity::Idle
            } else {
                ServiceActivity::Busy
            },
            queued_commands,
            active_executions,
            tasks: self.service.task_snapshots(),
            last_error,
        }
    }

    pub async fn changed(&mut self) -> Result<ServiceSnapshot, watch::error::RecvError> {
        self.changed.changed().await?;
        Ok(self.snapshot())
    }

    pub async fn wait_for(
        &mut self,
        mut predicate: impl FnMut(&ServiceSnapshot) -> bool,
    ) -> Result<ServiceSnapshot, watch::error::RecvError> {
        loop {
            let snapshot = self.snapshot();
            if predicate(&snapshot) {
                return Ok(snapshot);
            }
            self.changed.changed().await?;
        }
    }
}

pub struct RunningService<S: Service> {
    pub endpoint: ServiceEndpoint<S>,
    pub control: ServiceControl,
    pub observer: ServiceObserver<S>,
    pub task: JoinHandle<()>,
}

impl<S: Service> RunningService<S> {
    /// Last-resort teardown. Normal callers should prefer `drain` or `stop`.
    pub fn abort(&self) {
        self.task.abort();
    }
}

struct RuntimeState {
    revision: u64,
    lifecycle: Lifecycle,
    queued_commands: usize,
    executions: HashMap<u64, ExecutionSnapshot>,
    last_error: Option<String>,
}

struct CommandWaiter<S: Service> {
    reply: oneshot::Sender<CommandResult<S>>,
}

struct PendingCommand<S: Service> {
    command: S::Command,
    submit: Option<oneshot::Sender<Result<CommandReceipt, CallError<S::Error>>>>,
    waiters: Vec<CommandWaiter<S>>,
}

impl<S: Service> PendingCommand<S> {
    fn new(message: CommandMessage<S>) -> Self {
        let (submit, waiters) = match message.reply {
            CommandReply::Submit(reply) => (Some(reply), Vec::new()),
            CommandReply::Call(reply) => (None, vec![CommandWaiter { reply }]),
        };
        Self {
            command: message.command,
            submit,
            waiters,
        }
    }
}

struct ActiveCommand<S: Service> {
    execution: u64,
    command: S::Command,
    cancellation: CancellationToken,
    waiters: Vec<CommandWaiter<S>>,
}

struct CommandSlot<S: Service> {
    active: ActiveCommand<S>,
    pending: VecDeque<PendingCommand<S>>,
}

enum FutureResult<R, E> {
    Returned(Result<R, E>),
    Panicked(String),
}

enum Finished<S: Service> {
    Query {
        execution: u64,
        reply: oneshot::Sender<QueryResult<S>>,
        result: FutureResult<S::QueryReply, S::Error>,
    },
    Command {
        execution: u64,
        key: Option<S::Key>,
        result: FutureResult<S::CommandReply, S::Error>,
    },
}

type RunningFuture<S> = Pin<Box<dyn Future<Output = Finished<S>> + Send>>;

struct Runtime<S: Service> {
    service: Arc<S>,
    state: Arc<Mutex<RuntimeState>>,
    changed: watch::Sender<u64>,
    running: FuturesUnordered<RunningFuture<S>>,
    slots: HashMap<S::Key, CommandSlot<S>>,
    independent: HashMap<u64, ActiveCommand<S>>,
    shutdown_waiters: Vec<oneshot::Sender<Result<(), ControlError>>>,
}

pub(super) fn start<S: Service>(service: Arc<S>, config: ServiceConfig) -> RunningService<S> {
    assert!(
        config.channel_capacity > 0,
        "channel capacity must be non-zero"
    );
    let (query_tx, query_rx) = mpsc::channel(config.channel_capacity);
    let (command_tx, command_rx) = mpsc::channel(config.channel_capacity);
    let (control_tx, control_rx) = mpsc::channel(16);
    let state = Arc::new(Mutex::new(RuntimeState {
        revision: 0,
        lifecycle: Lifecycle::Initializing,
        queued_commands: 0,
        executions: HashMap::new(),
        last_error: None,
    }));
    let (changed_tx, changed_rx) = watch::channel(0);
    let observer = ServiceObserver {
        service: service.clone(),
        state: state.clone(),
        changed: changed_rx,
    };
    let task = tokio::spawn(run(
        service, state, changed_tx, query_rx, command_rx, control_rx,
    ));

    RunningService {
        endpoint: ServiceEndpoint {
            query: query_tx,
            command: command_tx,
        },
        control: ServiceControl { sender: control_tx },
        observer,
        task,
    }
}

async fn run<S: Service>(
    service: Arc<S>,
    state: Arc<Mutex<RuntimeState>>,
    changed: watch::Sender<u64>,
    mut query_rx: mpsc::Receiver<QueryMessage<S>>,
    mut command_rx: mpsc::Receiver<CommandMessage<S>>,
    mut control_rx: mpsc::Receiver<ControlMessage>,
) {
    let mut runtime = Runtime {
        service,
        state,
        changed,
        running: FuturesUnordered::new(),
        slots: HashMap::new(),
        independent: HashMap::new(),
        shutdown_waiters: Vec::new(),
    };

    match AssertUnwindSafe(runtime.service.initialize())
        .catch_unwind()
        .await
    {
        Ok(Ok(())) => runtime.lifecycle(Lifecycle::Running),
        Ok(Err(error)) => {
            runtime.fail(error.to_string());
            reject_channels(&mut query_rx, &mut command_rx, Lifecycle::Failed).await;
            return;
        }
        Err(panic) => {
            runtime.fail(format!(
                "initialize panicked: {}",
                panic_text(panic.as_ref())
            ));
            reject_channels(&mut query_rx, &mut command_rx, Lifecycle::Failed).await;
            return;
        }
    }

    let mut inputs_open = true;
    loop {
        let lifecycle = runtime.current_lifecycle();
        if matches!(lifecycle, Lifecycle::Draining | Lifecycle::Stopping) && inputs_open {
            inputs_open = false;
            reject_channels(&mut query_rx, &mut command_rx, lifecycle).await;
        }
        if matches!(lifecycle, Lifecycle::Draining | Lifecycle::Stopping)
            && runtime.running.is_empty()
            && runtime.pending_count() == 0
        {
            runtime.finish_shutdown().await;
            return;
        }

        tokio::select! {
            biased;

            Some(control) = control_rx.recv() => runtime.control(control),

            Some(finished) = runtime.running.next(), if !runtime.running.is_empty() => {
                runtime.finished(finished);
            }

            Some(query) = query_rx.recv(), if inputs_open => {
                if matches!(runtime.current_lifecycle(), Lifecycle::Running | Lifecycle::Paused) {
                    runtime.start_query(query);
                } else {
                    let _ = query.reply.send(Err(CallError::Unavailable(runtime.current_lifecycle())));
                }
            }

            Some(command) = command_rx.recv(), if inputs_open => {
                if runtime.current_lifecycle() == Lifecycle::Running {
                    runtime.admit(command);
                } else {
                    send_command_error(command.reply, CallError::Unavailable(runtime.current_lifecycle()));
                }
            }
        }
    }
}

impl<S: Service> Runtime<S> {
    fn admit(&mut self, message: CommandMessage<S>) {
        let key = match std::panic::catch_unwind(AssertUnwindSafe(|| {
            self.service.command_key(&message.command)
        })) {
            Ok(key) => key,
            Err(panic) => {
                send_command_error(
                    message.reply,
                    CallError::HandlerPanicked(panic_text(panic.as_ref()).into()),
                );
                return;
            }
        };
        let decision = match std::panic::catch_unwind(AssertUnwindSafe(|| match key.as_ref() {
            Some(key) => self
                .service
                .admit(&message.command, self.command_activity(key)),
            None => self.service.admit(&message.command, CommandActivity::Idle),
        })) {
            Ok(decision) => decision,
            Err(panic) => {
                send_command_error(
                    message.reply,
                    CallError::HandlerPanicked(panic_text(panic.as_ref()).into()),
                );
                return;
            }
        };
        let decision = match decision {
            Ok(decision) => decision,
            Err(error) => {
                send_command_error(message.reply, CallError::Business(error));
                return;
            }
        };
        let mut incoming = PendingCommand::new(message);

        match (key, decision) {
            (_, CommandDecision::Complete(reply)) => {
                acknowledge(&mut incoming, CommandReceipt::Completed);
                send_waiters(incoming.waiters, Ok(reply));
            }
            (None, CommandDecision::Run) => self.start_command(None, incoming, VecDeque::new()),
            (None, _) => reject_pending(
                incoming,
                CallError::Protocol("independent command must return Run or Complete"),
            ),
            (Some(key), CommandDecision::Run) if !self.slots.contains_key(&key) => {
                self.start_command(Some(key), incoming, VecDeque::new())
            }
            (Some(_), CommandDecision::Run) => reject_pending(
                incoming,
                CallError::Protocol("Run requires an idle command key"),
            ),
            (Some(key), CommandDecision::Join) if self.slots.contains_key(&key) => {
                let slot = self
                    .slots
                    .get_mut(&key)
                    .expect("Join requires an active command");
                let execution = slot.active.execution;
                if let Some(pending) = slot.pending.back_mut() {
                    pending.waiters.append(&mut incoming.waiters);
                } else {
                    slot.active.waiters.append(&mut incoming.waiters);
                }
                acknowledge(&mut incoming, CommandReceipt::Joined { execution });
            }
            (Some(_), CommandDecision::Join) => reject_pending(
                incoming,
                CallError::Protocol("Join requires an active command key"),
            ),
            (Some(key), CommandDecision::Queue) if self.slots.contains_key(&key) => {
                acknowledge(&mut incoming, CommandReceipt::Queued);
                self.slots
                    .get_mut(&key)
                    .expect("Queue requires an active command")
                    .pending
                    .push_back(incoming);
                self.refresh_pending();
            }
            (Some(_), CommandDecision::Queue) => reject_pending(
                incoming,
                CallError::Protocol("Queue requires an active command key"),
            ),
            (Some(key), CommandDecision::Replace { cause }) if self.slots.contains_key(&key) => {
                let (old_pending, active_execution) = {
                    let slot = self
                        .slots
                        .get_mut(&key)
                        .expect("Replace requires an active command");
                    let old = std::mem::take(&mut slot.pending);
                    slot.active.cancellation.cancel();
                    slot.pending.push_back(incoming);
                    (old, slot.active.execution)
                };
                for command in old_pending {
                    reject_pending(command, CallError::Superseded);
                }
                {
                    let slot = self.slots.get_mut(&key).expect("slot still exists");
                    let pending = slot.pending.back_mut().expect("replacement was queued");
                    acknowledge(pending, CommandReceipt::Queued);
                }
                self.mark_cancelling(active_execution, cause);
                self.refresh_pending();
            }
            (Some(_), CommandDecision::Replace { .. }) => reject_pending(
                incoming,
                CallError::Protocol("Replace requires an active command key"),
            ),
        }
    }

    fn command_activity(&self, key: &S::Key) -> CommandActivity<'_, S::Command> {
        let Some(slot) = self.slots.get(key) else {
            return CommandActivity::Idle;
        };
        CommandActivity::Running {
            active: &slot.active.command,
            latest: slot
                .pending
                .back()
                .map(|pending| &pending.command)
                .unwrap_or(&slot.active.command),
            pending: slot.pending.len(),
            cancelling: slot.active.cancellation.is_cancelled(),
        }
    }

    fn start_query(&mut self, message: QueryMessage<S>) {
        let execution = self.start_execution(None, format!("query: {:?}", message.query));
        let service = self.service.clone();
        self.running.push(Box::pin(async move {
            let result = AssertUnwindSafe(service.handle_query(message.query))
                .catch_unwind()
                .await;
            Finished::Query {
                execution,
                reply: message.reply,
                result: match result {
                    Ok(result) => FutureResult::Returned(result),
                    Err(panic) => FutureResult::Panicked(panic_text(panic.as_ref()).to_owned()),
                },
            }
        }));
    }

    fn start_command(
        &mut self,
        key: Option<S::Key>,
        mut pending: PendingCommand<S>,
        remaining: VecDeque<PendingCommand<S>>,
    ) {
        let execution = self.start_execution(
            key.as_ref().map(|key| format!("{key:?}")),
            format!("command: {:?}", pending.command),
        );
        let cancellation = CancellationToken::new();
        let context = CommandContext {
            execution,
            key: key.clone(),
            cancellation: cancellation.clone(),
            state: self.state.clone(),
            changed: self.changed.clone(),
        };
        acknowledge(&mut pending, CommandReceipt::Started { execution });
        let active = ActiveCommand {
            execution,
            command: pending.command.clone(),
            cancellation,
            waiters: pending.waiters,
        };
        match key.as_ref() {
            Some(key) => {
                self.slots.insert(
                    key.clone(),
                    CommandSlot {
                        active,
                        pending: remaining,
                    },
                );
            }
            None => {
                self.independent.insert(execution, active);
            }
        }

        let service = self.service.clone();
        let command = pending.command;
        self.running.push(Box::pin(async move {
            let result = AssertUnwindSafe(service.handle_command(command, context))
                .catch_unwind()
                .await;
            Finished::Command {
                execution,
                key,
                result: match result {
                    Ok(result) => FutureResult::Returned(result),
                    Err(panic) => FutureResult::Panicked(panic_text(panic.as_ref()).to_owned()),
                },
            }
        }));
        self.refresh_pending();
    }

    fn finished(&mut self, finished: Finished<S>) {
        match finished {
            Finished::Query {
                execution,
                reply,
                result,
            } => {
                let mapped = map_result(result);
                self.finish_execution(execution, mapped.as_ref().err().map(ToString::to_string));
                let _ = reply.send(mapped);
            }
            Finished::Command {
                execution,
                key,
                result,
            } => {
                let mapped = map_result(result);
                let waiters = match key.as_ref() {
                    Some(key) => std::mem::take(
                        &mut self
                            .slots
                            .get_mut(key)
                            .expect("completed command owns its slot")
                            .active
                            .waiters,
                    ),
                    None => {
                        self.independent
                            .remove(&execution)
                            .expect("completed independent command is active")
                            .waiters
                    }
                };
                send_waiters(waiters, mapped.clone());
                self.finish_execution(execution, mapped.as_ref().err().map(ToString::to_string));

                if let Some(key) = key {
                    let mut slot = self
                        .slots
                        .remove(&key)
                        .expect("completed command owns its slot");
                    if let Some(next) = slot.pending.pop_front() {
                        self.start_command(Some(key), next, slot.pending);
                    }
                }
                self.refresh_pending();
            }
        }
    }

    fn start_execution(&self, key: Option<String>, description: String) -> u64 {
        let id = NEXT_EXECUTION_ID.fetch_add(1, Ordering::Relaxed);
        self.update(|state| {
            state.executions.insert(
                id,
                ExecutionSnapshot {
                    id,
                    command_key: key,
                    description,
                    state: ExecutionState::Running,
                    progress: None,
                    milestone: None,
                },
            );
        });
        id
    }

    fn finish_execution(&self, execution: u64, error: Option<String>) {
        self.update(|state| {
            state.executions.remove(&execution);
            if error.is_some() {
                state.last_error = error;
            }
        });
    }

    fn mark_cancelling(&self, execution: u64, cause: String) {
        self.update(|state| {
            if let Some(execution) = state.executions.get_mut(&execution) {
                execution.state = ExecutionState::Cancelling;
                execution.milestone = Some(cause);
            }
        });
    }

    fn control(&mut self, message: ControlMessage) {
        match message {
            ControlMessage::Pause(reply) => {
                let result = if self.current_lifecycle() == Lifecycle::Running {
                    self.lifecycle(Lifecycle::Paused);
                    Ok(())
                } else {
                    Err(self.invalid("pause"))
                };
                let _ = reply.send(result);
            }
            ControlMessage::Resume(reply) => {
                let result = if self.current_lifecycle() == Lifecycle::Paused {
                    self.lifecycle(Lifecycle::Running);
                    Ok(())
                } else {
                    Err(self.invalid("resume"))
                };
                let _ = reply.send(result);
            }
            ControlMessage::Drain(reply) => {
                let lifecycle = self.current_lifecycle();
                if matches!(
                    lifecycle,
                    Lifecycle::Running | Lifecycle::Paused | Lifecycle::Draining
                ) {
                    if lifecycle != Lifecycle::Draining {
                        self.lifecycle(Lifecycle::Draining);
                    }
                    self.shutdown_waiters.push(reply);
                } else {
                    let _ = reply.send(Err(self.invalid("drain")));
                }
            }
            ControlMessage::Stop(reply) => {
                let lifecycle = self.current_lifecycle();
                if matches!(
                    lifecycle,
                    Lifecycle::Running
                        | Lifecycle::Paused
                        | Lifecycle::Draining
                        | Lifecycle::Stopping
                ) {
                    if lifecycle != Lifecycle::Stopping {
                        self.lifecycle(Lifecycle::Stopping);
                        self.cancel_all();
                        self.reject_all_pending();
                    }
                    self.shutdown_waiters.push(reply);
                } else {
                    let _ = reply.send(Err(self.invalid("stop")));
                }
            }
        }
    }

    fn cancel_all(&mut self) {
        let executions: Vec<_> = self
            .slots
            .values()
            .map(|slot| {
                slot.active.cancellation.cancel();
                slot.active.execution
            })
            .chain(self.independent.values().map(|active| {
                active.cancellation.cancel();
                active.execution
            }))
            .collect();
        for execution in executions {
            self.mark_cancelling(execution, "service stopping".into());
        }
    }

    fn reject_all_pending(&mut self) {
        for slot in self.slots.values_mut() {
            for pending in std::mem::take(&mut slot.pending) {
                reject_pending(pending, CallError::Unavailable(Lifecycle::Stopping));
            }
        }
        self.refresh_pending();
    }

    async fn finish_shutdown(&mut self) {
        match AssertUnwindSafe(self.service.shutdown())
            .catch_unwind()
            .await
        {
            Ok(Ok(())) => {
                self.lifecycle(Lifecycle::Stopped);
                for waiter in self.shutdown_waiters.drain(..) {
                    let _ = waiter.send(Ok(()));
                }
            }
            Ok(Err(error)) => {
                self.fail(error.to_string());
                self.fail_shutdown_waiters();
            }
            Err(panic) => {
                self.fail(format!("shutdown panicked: {}", panic_text(panic.as_ref())));
                self.fail_shutdown_waiters();
            }
        }
    }

    fn fail_shutdown_waiters(&mut self) {
        for waiter in self.shutdown_waiters.drain(..) {
            let _ = waiter.send(Err(ControlError::Stopped));
        }
    }

    fn pending_count(&self) -> usize {
        self.slots.values().map(|slot| slot.pending.len()).sum()
    }

    fn refresh_pending(&self) {
        let pending = self.pending_count();
        self.update(|state| state.queued_commands = pending);
    }

    fn current_lifecycle(&self) -> Lifecycle {
        self.state
            .lock()
            .expect("service runtime state poisoned")
            .lifecycle
    }

    fn lifecycle(&self, lifecycle: Lifecycle) {
        self.update(|state| state.lifecycle = lifecycle);
    }

    fn fail(&self, error: String) {
        self.update(|state| {
            state.lifecycle = Lifecycle::Failed;
            state.last_error = Some(error);
        });
    }

    fn invalid(&self, operation: &'static str) -> ControlError {
        ControlError::InvalidTransition {
            from: self.current_lifecycle(),
            operation,
        }
    }

    fn update(&self, update: impl FnOnce(&mut RuntimeState)) {
        let revision = {
            let mut state = self.state.lock().expect("service runtime state poisoned");
            update(&mut state);
            state.revision += 1;
            state.revision
        };
        self.changed.send_replace(revision);
    }
}

fn acknowledge<S: Service>(pending: &mut PendingCommand<S>, receipt: CommandReceipt) {
    if let Some(reply) = pending.submit.take() {
        let _ = reply.send(Ok(receipt));
    }
}

fn send_waiters<S: Service>(waiters: Vec<CommandWaiter<S>>, result: CommandResult<S>) {
    for waiter in waiters {
        let _ = waiter.reply.send(result.clone());
    }
}

fn reject_pending<S: Service>(mut pending: PendingCommand<S>, error: CallError<S::Error>) {
    if let Some(reply) = pending.submit.take() {
        let _ = reply.send(Err(error.clone()));
    }
    send_waiters(pending.waiters, Err(error));
}

fn send_command_error<S: Service>(reply: CommandReply<S>, error: CallError<S::Error>) {
    match reply {
        CommandReply::Submit(reply) => {
            let _ = reply.send(Err(error));
        }
        CommandReply::Call(reply) => {
            let _ = reply.send(Err(error));
        }
    }
}

fn map_result<R, E: Clone>(result: FutureResult<R, E>) -> Result<R, CallError<E>> {
    match result {
        FutureResult::Returned(Ok(reply)) => Ok(reply),
        FutureResult::Returned(Err(error)) => Err(CallError::Business(error)),
        FutureResult::Panicked(message) => Err(CallError::HandlerPanicked(message)),
    }
}

async fn reject_channels<S: Service>(
    query_rx: &mut mpsc::Receiver<QueryMessage<S>>,
    command_rx: &mut mpsc::Receiver<CommandMessage<S>>,
    lifecycle: Lifecycle,
) {
    query_rx.close();
    command_rx.close();
    while let Some(query) = query_rx.recv().await {
        let _ = query.reply.send(Err(CallError::Unavailable(lifecycle)));
    }
    while let Some(command) = command_rx.recv().await {
        send_command_error(command.reply, CallError::Unavailable(lifecycle));
    }
}

fn panic_text(panic: &(dyn std::any::Any + Send)) -> &str {
    panic
        .downcast_ref::<&'static str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic")
}
