//! Independently hosted VD service; actual BG scheduling remains a provider concern.
use crate::{
    member_disk::*,
    process_manager::{ExecutionScope, ObjectClaim, ObjectKey, WorkflowContext},
    runtime::activity::*,
};
use async_trait::async_trait;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub struct VdService {
    scope: ExecutionScope,
    provider: Arc<dyn VirtualDisks>,
}
impl VdService {
    pub(crate) fn new(scope: ExecutionScope, provider: Arc<dyn VirtualDisks>) -> Self {
        Self { scope, provider }
    }
}
#[async_trait]
impl ActivityHandler for VdService {
    type Request = DrainTicket;
    type Reply = DrainProof;
    type Query = ();
    type QueryReply = ();
    type Error = MemberDiskError;
    fn claims(ticket: &DrainTicket) -> Vec<ObjectClaim> {
        vec![ObjectClaim::exclusive(ObjectKey::new(
            "disk",
            ticket.disk.to_string(),
        ))]
    }
    fn name(_: &DrainTicket) -> &'static str {
        "vd.evacuate"
    }
    fn query(&self, _: ()) -> Result<(), MemberDiskError> {
        Ok(())
    }
    async fn execute(
        &self,
        ticket: DrainTicket,
        cancel: CancellationToken,
    ) -> Result<DrainProof, MemberDiskError> {
        if !ticket.scope.same(&self.scope) {
            return Err(MemberDiskError::InvalidRequest("foreign drain ticket"));
        }
        let result = self.provider.evacuate(&ticket.disk, &cancel).await;
        // An uncertain provider error must not be hidden by a simultaneous cancellation.
        result.map_err(|e| MemberDiskError::NeedsRecovery(e.0))?;
        if cancel.is_cancelled() {
            return Err(MemberDiskError::Cancelled);
        }
        Ok(DrainProof { ticket })
    }
}
#[derive(Clone)]
pub struct VdClient(pub(crate) ActivityClient<VdService>);
impl VdClient {
    pub async fn evacuate(
        &self,
        ctx: &mut WorkflowContext,
        ticket: DrainTicket,
    ) -> Result<DrainProof, MemberDiskError> {
        self.0.call(ctx, ticket).await
    }
}
