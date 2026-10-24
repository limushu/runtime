use super::*;
use crate::{
    support::{object, reject},
    task_center::{OperationKind, PoolOperation, PoolWorkflow},
    workflows::Domains,
};
use async_trait::async_trait;
use workflow_foundation::{
    ActivityClient, ActivityContext, ObjectKey, ResourceAccess, ServiceHandler, Workflow,
    WorkflowContext,
};

pub(crate) enum AdminCommand {
    Rename {
        name: PoolName,
        expected_revision: u64,
    },
    CheckEmpty,
}
pub(crate) type AdminClient = ActivityClient<AdminCommand, PoolDescriptor, (), PoolDescriptor>;
pub(crate) struct AdminHost {
    pub shared: Arc<PoolShared>,
    pub domains: Domains,
}
#[async_trait]
impl ServiceHandler for AdminHost {
    type Request = AdminCommand;
    type Reply = PoolDescriptor;
    type Query = ();
    type View = PoolDescriptor;
    fn activity_name(request: &AdminCommand) -> &'static str {
        match request {
            AdminCommand::Rename { .. } => "pool.rename",
            AdminCommand::CheckEmpty => "pool.check-empty",
        }
    }
    fn resources(_: &AdminCommand) -> Vec<ResourceAccess> {
        vec![ResourceAccess::write(object("pool", "directory"))]
    }
    async fn execute(&self, request: AdminCommand, _: ActivityContext) -> Outcome<PoolDescriptor> {
        match request {
            AdminCommand::Rename {
                name,
                expected_revision,
            } => {
                let registry = self.shared.registry.upgrade().ok_or(Failure::Unavailable)?;
                let mut registry = registry.lock().unwrap();
                let mut descriptor = self.shared.descriptor.lock().unwrap();
                descriptor.active()?;
                if descriptor.revision != expected_revision {
                    return Err(reject("stale pool revision"));
                }
                if registry
                    .names
                    .get(&name)
                    .is_some_and(|id| id != &descriptor.id)
                {
                    return Err(reject("pool name already used"));
                }
                registry.names.remove(&descriptor.name);
                registry.names.insert(name.clone(), descriptor.id.clone());
                descriptor.name = name;
                descriptor.revision += 1;
                Ok(descriptor.clone())
            }
            AdminCommand::CheckEmpty => {
                let (nodes, disks, volumes, tiers) = tokio::try_join!(
                    self.domains.node.query(None),
                    self.domains.disks(),
                    self.domains.volumes(),
                    self.domains.tier.query(None)
                )?;
                if !nodes.is_empty()
                    || !disks.is_empty()
                    || !volumes.is_empty()
                    || !tiers.is_empty()
                    || !self.shared.nodes.lock().unwrap().is_empty()
                {
                    return Err(reject("pool still has children or pending members"));
                }
                let uuid = self.shared.descriptor.lock().unwrap().uuid;
                if let Some(registry) = self.shared.registry.upgrade()
                    && registry
                        .lock()
                        .unwrap()
                        .disks
                        .values()
                        .any(|entry| entry.owner == uuid)
                {
                    return Err(reject("pool has outstanding disk reservations"));
                }
                Ok(self.shared.descriptor.lock().unwrap().clone())
            }
        }
    }
    fn query(&self, _: ()) -> Outcome<PoolDescriptor> {
        Ok(self.shared.descriptor.lock().unwrap().clone())
    }
}

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
