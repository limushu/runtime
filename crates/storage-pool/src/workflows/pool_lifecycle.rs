use super::*;
use crate::{
    support::reject,
    task_center::{OperationKind, PoolOperation, PoolWorkflow},
};
use async_trait::async_trait;
use std::collections::BTreeSet;
use workflow_foundation::{Workflow, WorkflowContext};
pub(crate) struct UnloadPoolWorkflow {
    pub domains: Domains,
    pub nodes: Vec<NodeId>,
}
impl PoolWorkflow for UnloadPoolWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::UnloadPool,
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for UnloadPoolWorkflow {
    type Output = ();
    const NAME: &'static str = "storage.unload-pool";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("pool", "lifecycle")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        for node in self.nodes {
            ctx.activity(&self.domains.node, NodeCommand::Unload(node))
                .await?;
        }
        Ok(())
    }
}
pub(crate) struct LoadPoolWorkflow {
    pub domains: Domains,
    pub snapshot: Vec<TopologyEvent>,
    pub disks: Vec<MemberDisk>,
    pub attach_nodes: bool,
}
impl PoolWorkflow for LoadPoolWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::LoadPool,
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for LoadPoolWorkflow {
    type Output = ();
    const NAME: &'static str = "storage.load-pool";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("pool", "lifecycle")]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        let mut connected = BTreeSet::new();
        for event in self.snapshot {
            let TopologyChange::NodeConnectivity {
                node,
                connected: is_connected,
            } = &event.change
            else {
                return Err(reject("load requires node connectivity snapshots"));
            };
            ctx.activity(
                &self.domains.membership,
                if self.attach_nodes {
                    crate::pool::MembershipCommand::RestoreObservation(event.clone())
                } else {
                    crate::pool::MembershipCommand::ApplyObservation(event.clone())
                },
            )
            .await?;
            if *is_connected {
                ctx.activity(&self.domains.node, NodeCommand::Load(node.clone()))
                    .await?;
                connected.insert(node.clone());
            }
        }
        for disk in self.disks {
            if self.attach_nodes && disk.io == IoState::Up && !connected.contains(&disk.node) {
                ctx.activity(
                    &self.domains.disk,
                    DiskCommand::SetIo(disk.id, IoState::Down),
                )
                .await?;
                continue;
            }
            if disk.io == IoState::Up && connected.contains(&disk.node) {
                ctx.activity(&self.domains.disk, DiskCommand::Open(disk.id.clone()))
                    .await?;
                ctx.activity(&self.domains.disk, DiskCommand::SetIo(disk.id, IoState::Up))
                    .await?;
            }
        }
        Ok(())
    }
}
