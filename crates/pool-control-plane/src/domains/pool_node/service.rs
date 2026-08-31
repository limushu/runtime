use super::protocol::{MemberDiskIoAvailability, PoolNodeReply, PoolNodeRequest};
use crate::kernel::MemberDiskId;
use async_trait::async_trait;
use control_runtime::{RequestRoute, RuntimeResult, Service, StateCell, WorkflowContext};
use std::convert::Infallible;
use std::time::Duration;

#[derive(Debug, Default)]
struct PoolNodeState {
    published: Vec<(MemberDiskId, MemberDiskIoAvailability)>,
}

#[derive(Debug, Default)]
pub struct PoolNodeService {
    state: StateCell<PoolNodeState>,
}

impl PoolNodeService {
    pub fn new() -> Self {
        Self::default()
    }

    async fn publish_member_disk(
        &self,
        disk: MemberDiskId,
        availability: MemberDiskIoAvailability,
    ) -> RuntimeResult<PoolNodeReply> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        self.state
            .update(|state| state.published.push((disk, availability)));
        Ok(PoolNodeReply::Published)
    }
}

#[async_trait]
impl Service for PoolNodeService {
    type Request = PoolNodeRequest;
    type WorkflowKind = Infallible;

    fn route(&self, _request: &Self::Request) -> RequestRoute<Self::WorkflowKind> {
        RequestRoute::Untracked
    }

    async fn handle(
        &self,
        request: Self::Request,
        _context: WorkflowContext,
    ) -> RuntimeResult<PoolNodeReply> {
        match request {
            PoolNodeRequest::PublishMemberDisk { disk, availability } => {
                self.publish_member_disk(disk, availability).await
            }
            PoolNodeRequest::PublishedCount => Ok(PoolNodeReply::PublishedCount(
                self.state.read(|state| state.published.len()),
            )),
        }
    }
}
