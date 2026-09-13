use super::{Activity, Lifecycle, Service, ServiceUnavailable};
use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot, watch};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CommandId(pub u64);

#[derive(Debug)]
pub enum CallError<E> {
    Unavailable(ServiceUnavailable),
    Business(E),
    Aborted,
}

impl<E: fmt::Display> fmt::Display for CallError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable(reason) => write!(formatter, "service is {reason:?}"),
            Self::Business(error) => error.fmt(formatter),
            Self::Aborted => formatter.write_str("service dropped the command before replying"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for CallError<E> {}

/// Reply contract selected by the caller of a command.
///
/// `submit` waits only for acceptance. `call` waits for the final business
/// result. The sender lives in the command record, never in child requests.
pub enum CommandReply<R, E> {
    Accepted(Option<oneshot::Sender<Result<CommandId, CallError<E>>>>),
    Completed(Option<oneshot::Sender<Result<R, CallError<E>>>>),
}

impl<R, E> CommandReply<R, E> {
    pub fn accepted(&mut self, command_id: CommandId) {
        if let Self::Accepted(sender) = self
            && let Some(sender) = sender.take()
        {
            let _ = sender.send(Ok(command_id));
        }
    }

    pub fn complete(&mut self, result: Result<R, E>) {
        if let Self::Completed(sender) = self
            && let Some(sender) = sender.take()
        {
            let _ = sender.send(result.map_err(CallError::Business));
        }
    }

    pub fn reject(&mut self, error: CallError<E>) {
        match self {
            Self::Accepted(sender) => {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Err(error));
                }
            }
            Self::Completed(sender) => {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Err(error));
                }
            }
        }
    }
}

impl<R, E> Drop for CommandReply<R, E> {
    fn drop(&mut self) {
        match self {
            Self::Accepted(sender) => {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Err(CallError::Aborted));
                }
            }
            Self::Completed(sender) => {
                if let Some(sender) = sender.take() {
                    let _ = sender.send(Err(CallError::Aborted));
                }
            }
        }
    }
}

pub struct IncomingCommand<C, R, E> {
    pub id: CommandId,
    pub payload: C,
    pub reply: CommandReply<R, E>,
}

pub(crate) struct QueryEnvelope<Q, R, E> {
    pub query: Q,
    pub reply: oneshot::Sender<Result<R, CallError<E>>>,
}

pub(crate) type ServiceCommand<S> =
    IncomingCommand<<S as Service>::Command, <S as Service>::CommandReply, <S as Service>::Error>;

pub(crate) type ServiceQuery<S> =
    QueryEnvelope<<S as Service>::Query, <S as Service>::QueryReply, <S as Service>::Error>;

pub struct ServiceClient<S: Service> {
    next_command_id: Arc<AtomicU64>,
    command_tx: mpsc::Sender<ServiceCommand<S>>,
    query_tx: mpsc::Sender<ServiceQuery<S>>,
}

impl<S: Service> Clone for ServiceClient<S> {
    fn clone(&self) -> Self {
        Self {
            next_command_id: self.next_command_id.clone(),
            command_tx: self.command_tx.clone(),
            query_tx: self.query_tx.clone(),
        }
    }
}

impl<S: Service> ServiceClient<S> {
    pub(crate) fn new(
        command_tx: mpsc::Sender<ServiceCommand<S>>,
        query_tx: mpsc::Sender<ServiceQuery<S>>,
    ) -> Self {
        Self {
            next_command_id: Arc::new(AtomicU64::new(1)),
            command_tx,
            query_tx,
        }
    }

    pub async fn query(&self, query: S::Query) -> Result<S::QueryReply, CallError<S::Error>> {
        let (reply, response) = oneshot::channel();
        self.query_tx
            .send(QueryEnvelope { query, reply })
            .await
            .map_err(|_| CallError::Unavailable(ServiceUnavailable::Stopped))?;
        response.await.unwrap_or(Err(CallError::Aborted))
    }

    pub async fn submit(&self, command: S::Command) -> Result<CommandId, CallError<S::Error>> {
        let id = self.next_id();
        let (reply, response) = oneshot::channel();
        let command = IncomingCommand {
            id,
            payload: command,
            reply: CommandReply::Accepted(Some(reply)),
        };
        self.command_tx
            .send(command)
            .await
            .map_err(|_| CallError::Unavailable(ServiceUnavailable::Stopped))?;
        response.await.unwrap_or(Err(CallError::Aborted))
    }

    pub async fn call(&self, command: S::Command) -> Result<S::CommandReply, CallError<S::Error>> {
        let id = self.next_id();
        let (reply, response) = oneshot::channel();
        let command = IncomingCommand {
            id,
            payload: command,
            reply: CommandReply::Completed(Some(reply)),
        };
        self.command_tx
            .send(command)
            .await
            .map_err(|_| CallError::Unavailable(ServiceUnavailable::Stopped))?;
        response.await.unwrap_or(Err(CallError::Aborted))
    }

    fn next_id(&self) -> CommandId {
        CommandId(self.next_command_id.fetch_add(1, Ordering::Relaxed))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ControlError {
    Stopped,
    InvalidTransition {
        from: Lifecycle,
        command: &'static str,
    },
}

pub(crate) enum ControlCommand {
    Pause,
    Resume,
    Drain,
    Stop,
}

pub(crate) struct ControlEnvelope {
    pub command: ControlCommand,
    pub reply: oneshot::Sender<Result<(), ControlError>>,
}

#[derive(Clone)]
pub struct ServiceControl {
    tx: mpsc::Sender<ControlEnvelope>,
}

impl ServiceControl {
    pub(crate) fn new(tx: mpsc::Sender<ControlEnvelope>) -> Self {
        Self { tx }
    }

    pub async fn pause(&self) -> Result<(), ControlError> {
        self.send(ControlCommand::Pause).await
    }

    pub async fn resume(&self) -> Result<(), ControlError> {
        self.send(ControlCommand::Resume).await
    }

    pub async fn drain(&self) -> Result<(), ControlError> {
        self.send(ControlCommand::Drain).await
    }

    pub async fn stop(&self) -> Result<(), ControlError> {
        self.send(ControlCommand::Stop).await
    }

    async fn send(&self, command: ControlCommand) -> Result<(), ControlError> {
        let (reply, response) = oneshot::channel();
        self.tx
            .send(ControlEnvelope { command, reply })
            .await
            .map_err(|_| ControlError::Stopped)?;
        response.await.unwrap_or(Err(ControlError::Stopped))
    }
}

#[derive(Debug, Clone)]
pub struct RuntimeSnapshot<S> {
    pub lifecycle: Lifecycle,
    pub activity: Activity,
    pub running_futures: usize,
    pub service: S,
}

pub struct ServiceObserver<S: Clone> {
    state: watch::Receiver<RuntimeSnapshot<S>>,
}

impl<S: Clone> Clone for ServiceObserver<S> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<S: Clone> ServiceObserver<S> {
    pub(crate) fn new(state: watch::Receiver<RuntimeSnapshot<S>>) -> Self {
        Self { state }
    }

    pub fn snapshot(&self) -> RuntimeSnapshot<S> {
        self.state.borrow().clone()
    }

    pub async fn changed(&mut self) -> Result<RuntimeSnapshot<S>, ControlError> {
        self.state
            .changed()
            .await
            .map_err(|_| ControlError::Stopped)?;
        Ok(self.snapshot())
    }
}
