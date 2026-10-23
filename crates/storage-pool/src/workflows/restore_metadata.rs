use super::*;
use crate::task_center::{OperationKind, PoolOperation, PoolWorkflow};
use async_trait::async_trait;
use workflow_foundation::{Workflow, WorkflowContext};
pub(crate) struct RestoreMetadataWorkflow {
    pub domains: Domains,
}
impl PoolWorkflow for RestoreMetadataWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::LoadPool,
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for RestoreMetadataWorkflow {
    type Output = ();
    const NAME: &'static str = "storage.restore-metadata";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("pool", "lifecycle")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.activity(
            &self.domains.membership,
            crate::pool::MembershipCommand::Restore,
        )
        .await
        .map(|_| ())
    }
}
