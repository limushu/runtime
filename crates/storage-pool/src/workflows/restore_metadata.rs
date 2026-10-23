use super::*;
use crate::{
    manager::PoolShared,
    task_center::{OperationKind, PoolOperation, PoolWorkflow},
};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use workflow_foundation::{Workflow, WorkflowContext};

pub(crate) struct RestoreMetadataWorkflow {
    pub domains: Domains,
    pub shared: Arc<PoolShared>,
    pub routing: Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
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
        ctx.activity(&self.domains.tier, TierCommand::Refresh)
            .await?;
        let disks = batch_disks(
            ctx.activity(&self.domains.disk, DiskCommand::Refresh)
                .await?,
        )?;
        ctx.activity(&self.domains.vd, VdCommand::Refresh).await?;
        crate::manager::restore_disk_directory(&self.shared, &self.routing, disks)
    }
}
impl PoolHandle {
    /// Call after acquiring Pool ownership and before recover_tasks. Connectivity/load
    /// observations still come from the real topology source; this never invents them.
    pub async fn restore_metadata(&self) -> Outcome<()> {
        self.descriptor().active()?;
        let mut run = self
            .task_center
            .submit_admin(RestoreMetadataWorkflow {
                domains: self.domains.clone(),
                shared: self.shared.clone(),
                routing: self.task_center.disk_directory(),
            })
            .await?;
        run.result().await
    }
    pub async fn disk(&self, id: DiskId) -> Outcome<MemberDisk> {
        if self
            .authoritative_reads
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return self
                .metadata
                .snapshot(&self.id)
                .await?
                .disks
                .into_iter()
                .find(|d| d.id == id)
                .ok_or_else(|| crate::support::reject("unknown disk"));
        }
        self.domains
            .disk
            .query(Some(id))
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| crate::support::reject("unknown disk"))
    }
}
