//! A domain's independent mailbox and Future driver. No object scheduler here.
use super::*;
use crate::process_manager::WorkflowFailure;
use crate::process_manager::{
    ActivityLease, ExecutionScope, ObjectClaim, WorkflowContext, WorkflowError,
};
use async_trait::async_trait;
use futures_util::FutureExt;
use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Debug,
    panic::AssertUnwindSafe,
    sync::Arc,
};
use tokio_util::sync::CancellationToken;

#[async_trait]
pub trait ActivityHandler: Send + Sync + 'static {
    type Request: Send + 'static;
    type Reply: Send + 'static;
    type Query: Send + 'static;
    type QueryReply: Send + 'static;
    type Error: WorkflowFailure + From<WorkflowError> + Debug;
    /// Required access is declared by the capability, not supplied by its caller.
    fn claims(request: &Self::Request) -> Vec<ObjectClaim>;
    fn name(request: &Self::Request) -> &'static str;
    async fn execute(
        &self,
        request: Self::Request,
        cancel: CancellationToken,
    ) -> Result<Self::Reply, Self::Error>;
    fn query(&self, query: Self::Query) -> Result<Self::QueryReply, Self::Error>;
}
pub struct ActivityRequest<H: ActivityHandler> {
    request: H::Request,
    cancel: CancellationToken,
    lease: ActivityLease,
}
pub struct ActivityCompletion<H: ActivityHandler> {
    id: CommandId,
    reply: CommandReply<H::Reply, H::Error>,
    result: Result<H::Reply, H::Error>,
}
#[derive(Debug, Clone, Default)]
pub struct ActivitySnapshot {
    pub queued: usize,
    pub running: usize,
    pub completed: u64,
}

pub struct ActivityService<H: ActivityHandler> {
    handler: Arc<H>,
    queue: VecDeque<IncomingCommand<ActivityRequest<H>, H::Reply, H::Error>>,
    active: BTreeMap<CommandId, CancellationToken>,
    completed: u64,
}
impl<H: ActivityHandler> ActivityService<H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler: Arc::new(handler),
            queue: VecDeque::new(),
            active: BTreeMap::new(),
            completed: 0,
        }
    }
}

pub struct ActivityClient<H: ActivityHandler> {
    client: ServiceClient<ActivityService<H>>,
    scope: ExecutionScope,
}
impl<H: ActivityHandler> Clone for ActivityClient<H> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            scope: self.scope.clone(),
        }
    }
}
impl<H: ActivityHandler> ActivityClient<H> {
    pub fn new(client: ServiceClient<ActivityService<H>>, scope: ExecutionScope) -> Self {
        Self { client, scope }
    }
    pub async fn call(
        &self,
        context: &mut WorkflowContext,
        request: H::Request,
    ) -> Result<H::Reply, H::Error> {
        let lease = context
            .authorize(&self.scope, &H::claims(&request), H::name(&request))
            .await
            .map_err(H::Error::from)?;
        let request = ActivityRequest {
            request,
            cancel: context.cancellation().child_token(),
            lease,
        };
        let result = self.client.call(request).await.map_err(|e| match e {
            CallError::Business(e) => e,
            CallError::Unavailable(e) => H::Error::from(WorkflowError::Failed(format!(
                "activity service unavailable: {e:?}"
            ))),
            CallError::Aborted => H::Error::recovery("activity service aborted"),
        });
        context.activity_finished();
        result
    }
    pub async fn query(&self, query: H::Query) -> Result<H::QueryReply, CallError<H::Error>> {
        self.client.query(query).await
    }
}
impl<H: ActivityHandler> Service for ActivityService<H> {
    type Command = ActivityRequest<H>;
    type CommandReply = H::Reply;
    type Query = H::Query;
    type QueryReply = H::QueryReply;
    type Error = H::Error;
    type Completion = ActivityCompletion<H>;
    type Snapshot = ActivitySnapshot;
    fn query(&self, query: H::Query) -> Result<H::QueryReply, H::Error> {
        self.handler.query(query)
    }
    fn receive_command(&mut self, mut command: IncomingCommand<Self::Command, H::Reply, H::Error>) {
        command.reply.accepted(command.id);
        self.queue.push_back(command);
    }
    fn admit(&mut self) {}
    fn start_ready(&mut self, runs: &mut NewRuns<Self::Completion>) {
        while let Some(command) = self.queue.pop_front() {
            let IncomingCommand { id, payload, reply } = command;
            let ActivityRequest {
                request,
                cancel,
                mut lease,
            } = payload;
            self.active.insert(id, cancel.clone());
            let handler = self.handler.clone();
            runs.push(async move {
                let result = if cancel.is_cancelled() {
                    Err(H::Error::cancelled())
                } else {
                    lease.started();
                    AssertUnwindSafe(handler.execute(request, cancel))
                        .catch_unwind()
                        .await
                        .unwrap_or_else(|_| Err(H::Error::recovery("activity panicked")))
                };
                lease.settled(&result);
                drop(lease);
                ActivityCompletion { id, reply, result }
            });
        }
    }
    fn finish_running(&mut self, mut completion: Self::Completion) {
        self.active.remove(&completion.id);
        self.completed += 1;
        completion.reply.complete(completion.result);
    }
    fn cancel_all(&mut self) {
        for mut command in self.queue.drain(..) {
            command.reply.complete(Err(H::Error::cancelled()));
        }
        for cancel in self.active.values() {
            cancel.cancel();
        }
    }
    fn is_idle(&self) -> bool {
        self.queue.is_empty() && self.active.is_empty()
    }
    fn snapshot(&self) -> ActivitySnapshot {
        ActivitySnapshot {
            queued: self.queue.len(),
            running: self.active.len(),
            completed: self.completed,
        }
    }
}
