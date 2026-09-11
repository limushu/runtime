mod runtime;

pub use runtime::{
    CallError, CommandActivity, CommandDecision, CommandReceipt, ControlError, ExecutionContext,
    ExecutionSnapshot, ExecutionState, Lifecycle, RunningService, ServiceActivity, ServiceConfig,
    ServiceControl, ServiceEndpoint, ServiceObserver, ServiceSnapshot,
};

use async_trait::async_trait;
use std::{fmt, hash::Hash, sync::Arc};

/// A business task as understood by the domain, not a Future invented by the runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskSnapshot {
    pub id: String,
    pub kind: String,
    pub subject: String,
    pub state: TaskState,
    pub progress: Option<u8>,
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Running,
    Paused,
    Cancelling,
    Failed,
}

/// The contract implemented by every in-process Pool service.
///
/// Business data remains private in the concrete service. The SDK supplies
/// lifecycle, channels, command slots, Future polling, cancellation and
/// runtime observation through [`Service::start`].
#[async_trait]
pub trait Service: Send + Sync + 'static {
    type Query: fmt::Debug + Send + 'static;
    type QueryReply: Send + 'static;
    type Command: Clone + fmt::Debug + Send + 'static;
    type CommandReply: Clone + Send + 'static;
    type Key: Clone + Eq + Hash + fmt::Debug + Send + Sync + 'static;
    type Error: Clone + fmt::Display + Send + Sync + 'static;

    fn name(&self) -> &'static str;

    /// `Some(key)` opts a command into per-object serialization.
    /// Independent commands return `None`.
    fn command_key(&self, command: &Self::Command) -> Option<Self::Key>;

    /// Supplies domain conflict policy; the SDK applies the returned mechanics.
    fn admit(
        &self,
        command: &Self::Command,
        activity: CommandActivity<'_, Self::Command>,
    ) -> Result<CommandDecision<Self::CommandReply>, Self::Error>;

    async fn handle_query(&self, query: Self::Query) -> Result<Self::QueryReply, Self::Error>;

    /// Implement this as ordinary async business code. Stateful domains may
    /// repeatedly execute one explicit state transition until stable.
    async fn handle_command(
        self: Arc<Self>,
        command: Self::Command,
        context: ExecutionContext<Self::Key>,
    ) -> Result<Self::CommandReply, Self::Error>;

    /// Returns domain-owned tasks in one SDK-wide projection.
    /// The runtime never creates these tasks on the service's behalf.
    fn task_snapshots(&self) -> Vec<TaskSnapshot> {
        Vec::new()
    }

    async fn initialize(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    async fn shutdown(&self) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Starts the SDK-provided service runtime. Business implementations do
    /// not write their own select loop or lifecycle state machine.
    fn start(self, config: ServiceConfig) -> RunningService<Self>
    where
        Self: Sized,
    {
        runtime::start(Arc::new(self), config)
    }
}
