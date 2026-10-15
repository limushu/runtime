//! Named application workflows. No domain state lives in these structs.
//! Construct through PoolHandle so callers cannot omit the required conflict set.
use crate::{
    app::coordination::{OperationKind, PoolOperation, PoolWorkflow},
    domain::disk::{DiskCommand, DiskReply, DiskService},
    domain::node::{NodeCommand, NodeService},
    domain::vd::{VdCommand, VdReply, VdService},
    domain::{object, reject},
    *,
};
use async_trait::async_trait;
use workflow_foundation::{ObjectKey, ServiceClient, Workflow, WorkflowContext};

#[derive(Clone)]
pub(crate) struct Domains {
    pub disk: ServiceClient<DiskService>,
    pub node: ServiceClient<NodeService>,
    pub vd: ServiceClient<VdService>,
}
fn disk_result(reply: DiskReply) -> Outcome<MemberDisk> {
    match reply {
        DiskReply::Disk(disk) => Ok(disk),
        _ => Err(Failure::Uncertain("unexpected disk reply".into())),
    }
}

impl PoolWorkflow for ObserveNodeWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::ObserveNode {
                id: self.id.clone(),
                sequence: self.sequence,
                connected: self.connected,
            },
            objects: self.objects(),
        }
    }
}
impl PoolWorkflow for LoadNodeWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::LoadNode(self.id.clone()),
            objects: self.objects(),
        }
    }
}
impl PoolWorkflow for OnlineDiskWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::OnlineDisk(self.spec.id.clone()),
            objects: self.objects(),
        }
    }
}
impl PoolWorkflow for OfflineDiskWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::OfflineDisk(self.id.clone()),
            objects: self.objects(),
        }
    }
}
impl PoolWorkflow for CreateVolumeWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::CreateVolume(self.request.id.clone()),
            objects: self.objects(),
        }
    }
}

pub struct ObserveNodeWorkflow {
    pub(crate) domains: Domains,
    pub(crate) id: NodeId,
    pub(crate) sequence: u64,
    pub(crate) connected: bool,
}
#[async_trait]
impl Workflow for ObserveNodeWorkflow {
    type Output = PoolNode;
    const NAME: &'static str = "storage.observe-node";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("node", self.id.as_str())]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<PoolNode> {
        ctx.activity(
            &self.domains.node,
            NodeCommand::Observe {
                id: self.id,
                sequence: self.sequence,
                connected: self.connected,
            },
        )
        .await
    }
}

pub struct LoadNodeWorkflow {
    pub(crate) domains: Domains,
    pub(crate) id: NodeId,
}
#[async_trait]
impl Workflow for LoadNodeWorkflow {
    type Output = PoolNode;
    const NAME: &'static str = "storage.load-node";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("node", self.id.as_str())]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<PoolNode> {
        ctx.activity(&self.domains.node, NodeCommand::Load(self.id))
            .await
    }
}

pub struct OnlineDiskWorkflow {
    pub(crate) domains: Domains,
    pub(crate) spec: DiskSpec,
}
#[async_trait]
impl Workflow for OnlineDiskWorkflow {
    type Output = MemberDisk;
    const NAME: &'static str = "storage.online-disk";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![
            object("disk", self.spec.id.as_str()),
            object("node", self.spec.node.as_str()),
        ]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<MemberDisk> {
        let node = ctx
            .activity(
                &self.domains.node,
                NodeCommand::Inspect(self.spec.node.clone()),
            )
            .await?;
        if !node.serving() {
            return Err(reject("disk online requires a loaded, connected pool node"));
        }
        let id = self.spec.id.clone();
        // These are steps of ONE business operation, never separate public runs.
        // Ensure membership is idempotent; Open alone does not publish IO Up.
        ctx.activity(&self.domains.disk, DiskCommand::Add(self.spec))
            .await?;
        ctx.activity(&self.domains.disk, DiskCommand::Open(id.clone()))
            .await?;
        disk_result(
            ctx.activity(&self.domains.disk, DiskCommand::SetIo(id, IoState::Up))
                .await?,
        )
    }
}

pub struct OfflineDiskWorkflow {
    pub(crate) domains: Domains,
    pub(crate) id: DiskId,
}
#[async_trait]
impl Workflow for OfflineDiskWorkflow {
    type Output = MemberDisk;
    const NAME: &'static str = "storage.offline-disk";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("disk", self.id.as_str())]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<MemberDisk> {
        ctx.activity(&self.domains.disk, DiskCommand::Inspect(self.id.clone()))
            .await?;
        // Conservative validity transition first: cancellation may leave an Up
        // disk with Invalid data, never a fake successful repair or freed BLKs.
        ctx.activity(&self.domains.vd, VdCommand::Invalidate(self.id.clone()))
            .await?;
        disk_result(
            ctx.activity(
                &self.domains.disk,
                DiskCommand::SetIo(self.id, IoState::Down),
            )
            .await?,
        )
    }
}

pub struct CreateVolumeWorkflow {
    pub(crate) domains: Domains,
    pub(crate) request: CreateVolume,
    pub(crate) nodes: Vec<NodeId>,
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
