use super::{Domains, disk_result};
use crate::domains::disk::service::{DiskCommand, DiskReply};
use crate::domains::node::service::NodeCommand;
use crate::domains::vd::service::{VdCommand, VdReply};
use crate::support::{object, reject};
use crate::task_center::{OperationKind, PoolOperation, PoolWorkflow};
use crate::*;
use async_trait::async_trait;
use workflow_foundation::{ObjectKey, Workflow, WorkflowContext};

pub struct CreateVolumeWorkflow {
    pub(crate) domains: Domains,
    pub(crate) request: CreateVolume,
    pub(crate) nodes: Vec<NodeId>,
}

impl PoolWorkflow for CreateVolumeWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::CreateVolume(self.request.id.clone()),
            objects: self.objects(),
        }
    }
}

#[async_trait]
impl Workflow for CreateVolumeWorkflow {
    type Output = VirtualDisk;
    const NAME: &'static str = "storage.create-volume";
    fn objects(&self) -> Vec<ObjectKey> {
        let mut keys = vec![object("volume", self.request.id.as_str())];
        keys.extend(
            self.request
                .disks
                .iter()
                .map(|id| object("disk", id.as_str())),
        );
        keys.extend(self.nodes.iter().map(|id| object("node", id.as_str())));
        keys
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<VirtualDisk> {
        ctx.report_progress(Self::NAME, 0, None);
        self.request.validate()?;
        ctx.activity(
            &self.domains.vd,
            VdCommand::RequireAbsent(self.request.id.clone()),
        )
        .await?;
        // Recheck current states after workflow admission, not just on submission.
        for id in &self.nodes {
            if !ctx
                .activity(&self.domains.node, NodeCommand::Inspect(id.clone()))
                .await?
                .serving()
            {
                return Err(reject("replica node is not serving"));
            }
        }
        for id in &self.request.disks {
            let disk = disk_result(
                ctx.activity(&self.domains.disk, DiskCommand::Inspect(id.clone()))
                    .await?,
            )?;
            if !disk.allocatable() || disk.allocated.len() as u64 >= u64::from(disk.capacity_blks) {
                return Err(reject("replica disk unavailable or full"));
            }
        }
        let mut allocations = Vec::new();
        for disk in self.request.disks {
            match ctx
                .activity(
                    &self.domains.disk,
                    DiskCommand::Allocate(disk, self.request.id.clone()),
                )
                .await?
            {
                DiskReply::Allocated(a) => allocations.push(a),
                _ => return Err(Failure::Uncertain("unexpected allocation reply".into())),
            }
        }
        match ctx
            .activity(
                &self.domains.vd,
                VdCommand::Create {
                    id: self.request.id,
                    allocations,
                },
            )
            .await?
        {
            VdReply::Created(v) => Ok(v),
            _ => Err(Failure::Uncertain("unexpected VD reply".into())),
        }
    }
}
