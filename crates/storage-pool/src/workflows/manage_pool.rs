use crate::{
    pool::{AdminClient, AdminCommand},
    support::object,
    task_center::{OperationKind, PoolOperation, PoolWorkflow},
    *,
};
use async_trait::async_trait;
use workflow_foundation::{ObjectKey, Workflow, WorkflowContext};
pub(crate) struct RenamePoolWorkflow {
    pub client: AdminClient,
    pub name: PoolName,
    pub revision: u64,
}
impl PoolWorkflow for RenamePoolWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::RenamePool,
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for RenamePoolWorkflow {
    type Output = PoolDescriptor;
    const NAME: &'static str = "storage.rename-pool";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("pool", "directory")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<PoolDescriptor> {
        ctx.activity(
            &self.client,
            AdminCommand::Rename {
                name: self.name,
                expected_revision: self.revision,
            },
        )
        .await
    }
}
pub(crate) struct DeletePoolWorkflow {
    pub client: AdminClient,
}
impl PoolWorkflow for DeletePoolWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::DeletePool,
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for DeletePoolWorkflow {
    type Output = ();
    const NAME: &'static str = "storage.delete-pool";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("pool", "directory")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        ctx.activity(&self.client, AdminCommand::CheckEmpty).await?;
        Ok(())
    }
}
