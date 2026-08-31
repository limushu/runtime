use super::protocol::{MemberDiskIoAvailability, PoolNodeCommand, PoolNodeResponse};
use crate::kernel::{MemberDiskId, PoolId};
use async_trait::async_trait;
use control_runtime::{
    spawn_service, ManagedService, RequestRoute, RuntimeConfig, RuntimeError, RuntimeResult,
    Service, ServiceClient, ServiceId, StateCell, WorkflowContext,
};
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct PoolNodeService {
    client: ServiceClient<PoolNodeCommand>,
}

impl PoolNodeService {
    pub(crate) fn spawn(pool: &PoolId, config: RuntimeConfig) -> (Self, ManagedService) {
        let worker = Arc::new(PoolNodeWorker::new(pool));
        let (client, managed) = spawn_service(worker, config);
        (Self { client }, managed)
    }

    pub async fn publish_member_disk(
        &self,
        context: &WorkflowContext,
        disk: MemberDiskId,
        availability: MemberDiskIoAvailability,
    ) -> RuntimeResult<()> {
        match self
            .client
            .call(
                context,
                format!("publish {disk} as {availability:?}"),
                PoolNodeCommand::PublishMemberDisk { disk, availability },
            )
            .await?
        {
            PoolNodeResponse::Published => Ok(()),
            PoolNodeResponse::PublishedCount(_) => Err(RuntimeError::Internal(
                "PoolNode returned a query response to publish_member_disk".into(),
            )),
        }
    }

    pub async fn published_count(&self) -> RuntimeResult<usize> {
        match self
            .client
            .call_root(
                "query published member disk facts",
                PoolNodeCommand::PublishedCount,
            )
            .await?
        {
            PoolNodeResponse::PublishedCount(count) => Ok(count),
            PoolNodeResponse::Published => Err(RuntimeError::Internal(
                "PoolNode returned Published to published_count".into(),
            )),
        }
    }
}

#[derive(Debug, Default)]
struct PoolNodeState {
    published: Vec<(MemberDiskId, MemberDiskIoAvailability)>,
}

struct PoolNodeWorker {
    id: ServiceId,
    state: StateCell<PoolNodeState>,
}

impl PoolNodeWorker {
    fn new(pool: &PoolId) -> Self {
        Self {
            id: ServiceId::new(format!("pool/{pool}/node")),
            state: StateCell::new(PoolNodeState::default()),
        }
    }
}

#[async_trait]
impl Service for PoolNodeWorker {
    type Request = PoolNodeCommand;
    type WorkflowKind = Infallible;

    fn id(&self) -> ServiceId {
        self.id.clone()
    }

    fn route(&self, _request: &Self::Request) -> RequestRoute<Self::WorkflowKind> {
        RequestRoute::Untracked
    }

    async fn handle(
        &self,
        request: Self::Request,
        _context: WorkflowContext,
    ) -> RuntimeResult<PoolNodeResponse> {
        match request {
            PoolNodeCommand::PublishMemberDisk { disk, availability } => {
                tokio::time::sleep(Duration::from_millis(5)).await;
                self.state
                    .update(|state| state.published.push((disk, availability)));
                Ok(PoolNodeResponse::Published)
            }
            PoolNodeCommand::PublishedCount => Ok(PoolNodeResponse::PublishedCount(
                self.state.read(|state| state.published.len()),
            )),
        }
    }
}
