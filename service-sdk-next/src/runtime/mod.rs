mod api;
mod lifecycle;
mod runs;

pub use api::{
    CallError, CommandId, CommandReply, ControlError, IncomingCommand, RuntimeSnapshot,
    ServiceClient, ServiceControl, ServiceObserver,
};
pub use lifecycle::{Activity, Lifecycle, ServiceUnavailable};
pub use runs::NewRuns;

use api::{ControlCommand, ControlEnvelope, ServiceCommand, ServiceQuery};
use futures_util::StreamExt;
use runs::RunDriver;
use std::fmt::Debug;
use std::future::pending;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::{JoinError, JoinHandle},
};

/// Business hooks invoked by the fixed service runtime.
///
/// The service owns all domain state and request queues. The runtime only
/// orders these hooks and polls the futures produced by `start_ready`.
pub trait Service: Send + 'static {
    type Command: Send + 'static;
    type CommandReply: Send + 'static;
    type Query: Send + 'static;
    type QueryReply: Send + 'static;
    type Completion: Send + 'static;
    type Error: Debug + Send + 'static;
    type Snapshot: Clone + Debug + Send + Sync + 'static;

    fn query(&self, query: Self::Query) -> Result<Self::QueryReply, Self::Error>;

    fn receive_command(
        &mut self,
        command: IncomingCommand<Self::Command, Self::CommandReply, Self::Error>,
    );

    fn admit(&mut self);

    fn start_ready(&mut self, runs: &mut NewRuns<Self::Completion>);

    fn finish_running(&mut self, completion: Self::Completion);

    fn cancel_all(&mut self);

    fn is_idle(&self) -> bool;

    /// Earliest business retry deadline. The default service has no timers.
    fn next_wakeup(&self) -> Option<tokio::time::Instant> {
        None
    }

    fn snapshot(&self) -> Self::Snapshot;
}

#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub channel_capacity: usize,
    pub commands_per_round: usize,
    pub queries_per_round: usize,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            channel_capacity: 128,
            commands_per_round: 64,
            queries_per_round: 64,
        }
    }
}

pub struct ServiceInstance<S: Service> {
    pub client: ServiceClient<S>,
    pub control: ServiceControl,
    pub observer: ServiceObserver<S::Snapshot>,
    /// Ownership handle for the service's single root Tokio task.
    /// Dropping or aborting it forcefully drops every future owned by the service.
    pub task: ServiceTask,
}

impl<S: Service> ServiceInstance<S> {
    pub async fn join(mut self) -> Result<(), JoinError> {
        self.task.join().await
    }
}

pub struct ServiceTask {
    handle: Option<JoinHandle<()>>,
}

impl ServiceTask {
    pub async fn join(&mut self) -> Result<(), JoinError> {
        match self.handle.take() {
            Some(handle) => handle.await,
            None => Ok(()),
        }
    }

    pub fn abort(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

impl Drop for ServiceTask {
    fn drop(&mut self) {
        self.abort();
    }
}

pub fn spawn_service<S: Service>(service: S, config: ServiceConfig) -> ServiceInstance<S> {
    let (command_tx, command_rx) = mpsc::channel(config.channel_capacity);
    let (query_tx, query_rx) = mpsc::channel(config.channel_capacity);
    let (control_tx, control_rx) = mpsc::channel(config.channel_capacity);

    let initial = RuntimeSnapshot {
        lifecycle: Lifecycle::Running,
        activity: if service.is_idle() {
            Activity::Idle
        } else {
            Activity::Busy
        },
        running_futures: 0,
        service: service.snapshot(),
    };
    let (snapshot_tx, snapshot_rx) = watch::channel(initial);

    let task = tokio::spawn(run_service(
        service,
        config,
        command_rx,
        query_rx,
        control_rx,
        snapshot_tx,
    ));

    ServiceInstance {
        client: ServiceClient::new(command_tx, query_tx),
        control: ServiceControl::new(control_tx),
        observer: ServiceObserver::new(snapshot_rx),
        task: ServiceTask { handle: Some(task) },
    }
}

async fn run_service<S: Service>(
    mut service: S,
    config: ServiceConfig,
    mut command_rx: mpsc::Receiver<ServiceCommand<S>>,
    mut query_rx: mpsc::Receiver<ServiceQuery<S>>,
    mut control_rx: mpsc::Receiver<ControlEnvelope>,
    snapshot_tx: watch::Sender<RuntimeSnapshot<S::Snapshot>>,
) {
    let mut lifecycle = Lifecycle::Running;
    let mut driver = RunDriver::default();
    let mut shutdown_waiters: Vec<oneshot::Sender<Result<(), ControlError>>> = Vec::new();

    loop {
        // Control is checked before bounded business draining as well as first
        // in the biased select below. A full command channel cannot starve an
        // operator request.
        if let Ok(control) = control_rx.try_recv() {
            apply_control(control, &mut lifecycle, &mut service, &mut shutdown_waiters);
        }

        drain_queries(&service, &mut query_rx, config.queries_per_round);
        drain_commands(
            &mut service,
            &mut command_rx,
            lifecycle,
            config.commands_per_round,
        );

        if lifecycle.schedules_work() {
            service.admit();

            let mut new_runs = NewRuns::default();
            service.start_ready(&mut new_runs);
            driver.extend(&mut new_runs);
        }

        publish_snapshot(&snapshot_tx, lifecycle, driver.len(), &service);

        if matches!(lifecycle, Lifecycle::Draining | Lifecycle::Stopping)
            && service.is_idle()
            && driver.is_empty()
        {
            lifecycle = Lifecycle::Stopped;
            reject_queued_commands::<S>(&mut command_rx, lifecycle);
            publish_snapshot(&snapshot_tx, lifecycle, 0, &service);
            for waiter in shutdown_waiters.drain(..) {
                let _ = waiter.send(Ok(()));
            }
            break;
        }

        let wakeup = service.next_wakeup();

        tokio::select! {
            biased;

            control = control_rx.recv() => {
                let Some(control) = control else {
                    if lifecycle != Lifecycle::Stopping {
                        lifecycle = Lifecycle::Draining;
                    }
                    continue;
                };
                apply_control(
                    control,
                    &mut lifecycle,
                    &mut service,
                    &mut shutdown_waiters,
                );
            }

            completion = driver.next(), if !driver.is_empty() => {
                if let Some(completion) = completion {
                    service.finish_running(completion);
                }
            }

            query = query_rx.recv() => {
                if let Some(query) = query {
                    answer_query(&service, query);
                }
            }

            command = command_rx.recv() => {
                if let Some(command) = command {
                    receive_or_reject(&mut service, lifecycle, command);
                }
            }

            _ = async {
                match wakeup {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => pending::<()>().await,
                }
            } => {}

            else => {
                lifecycle = Lifecycle::Stopping;
                service.cancel_all();
            }
        }
    }
}

fn drain_queries<S: Service>(
    service: &S,
    receiver: &mut mpsc::Receiver<ServiceQuery<S>>,
    limit: usize,
) {
    for _ in 0..limit {
        let Ok(query) = receiver.try_recv() else {
            break;
        };
        answer_query(service, query);
    }
}

fn answer_query<S: Service>(service: &S, query: ServiceQuery<S>) {
    let result = service.query(query.query).map_err(CallError::Business);
    let _ = query.reply.send(result);
}

fn drain_commands<S: Service>(
    service: &mut S,
    receiver: &mut mpsc::Receiver<ServiceCommand<S>>,
    lifecycle: Lifecycle,
    limit: usize,
) {
    for _ in 0..limit {
        let Ok(command) = receiver.try_recv() else {
            break;
        };
        receive_or_reject(service, lifecycle, command);
    }
}

fn receive_or_reject<S: Service>(
    service: &mut S,
    lifecycle: Lifecycle,
    mut command: ServiceCommand<S>,
) {
    if lifecycle.accepts_commands() {
        service.receive_command(command);
    } else {
        command
            .reply
            .reject(CallError::Unavailable(lifecycle.into()));
    }
}

fn reject_queued_commands<S: Service>(
    receiver: &mut mpsc::Receiver<ServiceCommand<S>>,
    lifecycle: Lifecycle,
) {
    receiver.close();
    while let Ok(mut command) = receiver.try_recv() {
        command
            .reply
            .reject(CallError::Unavailable(lifecycle.into()));
    }
}

fn apply_control<S: Service>(
    control: ControlEnvelope,
    lifecycle: &mut Lifecycle,
    service: &mut S,
    shutdown_waiters: &mut Vec<oneshot::Sender<Result<(), ControlError>>>,
) {
    let ControlEnvelope { command, reply } = control;

    match command {
        ControlCommand::Pause if *lifecycle == Lifecycle::Running => {
            *lifecycle = Lifecycle::Paused;
            let _ = reply.send(Ok(()));
        }
        ControlCommand::Resume if *lifecycle == Lifecycle::Paused => {
            *lifecycle = Lifecycle::Running;
            let _ = reply.send(Ok(()));
        }
        ControlCommand::Drain if matches!(*lifecycle, Lifecycle::Running | Lifecycle::Paused) => {
            *lifecycle = Lifecycle::Draining;
            shutdown_waiters.push(reply);
        }
        ControlCommand::Drain if *lifecycle == Lifecycle::Draining => {
            shutdown_waiters.push(reply);
        }
        ControlCommand::Stop if *lifecycle != Lifecycle::Stopped => {
            if *lifecycle != Lifecycle::Stopping {
                *lifecycle = Lifecycle::Stopping;
                service.cancel_all();
            }
            shutdown_waiters.push(reply);
        }
        command => {
            let command = match command {
                ControlCommand::Pause => "pause",
                ControlCommand::Resume => "resume",
                ControlCommand::Drain => "drain",
                ControlCommand::Stop => "stop",
            };
            let _ = reply.send(Err(ControlError::InvalidTransition {
                from: *lifecycle,
                command,
            }));
        }
    }
}

fn publish_snapshot<S: Service>(
    sender: &watch::Sender<RuntimeSnapshot<S::Snapshot>>,
    lifecycle: Lifecycle,
    running_futures: usize,
    service: &S,
) {
    sender.send_replace(RuntimeSnapshot {
        lifecycle,
        activity: if service.is_idle() && running_futures == 0 {
            Activity::Idle
        } else {
            Activity::Busy
        },
        running_futures,
        service: service.snapshot(),
    });
}
