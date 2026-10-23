//! A whole-disk business process. BG planning/copy/remap stays in the balancing domain.
use super::{Domains, disk_result};
use crate::{
    domains::{balance::*, disk::service::DiskCommand, vd::service::VdCommand},
    support::object,
    *,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use workflow_foundation::{ContinuationGuard, ObjectKey, Workflow, WorkflowContext};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ShrinkRequest {
    pub spec: DiskSpec,
    pub job: EvacuationId,
}
pub struct ShrinkDiskWorkflow {
    domains: Domains,
    request: ShrinkRequest,
    guard: Arc<dyn ContinuationGuard>,
}
impl ShrinkDiskWorkflow {
    pub(crate) fn new(
        domains: Domains,
        request: ShrinkRequest,
        guard: Arc<dyn ContinuationGuard>,
    ) -> Self {
        Self {
            domains,
            request,
            guard,
        }
    }
}
#[async_trait]
impl Workflow for ShrinkDiskWorkflow {
    type Output = MemberDisk;
    const NAME: &'static str = "storage.shrink-disk";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![
            object("disk", self.request.spec.id.as_str()),
            object("node", self.request.spec.node.as_str()),
            object("tier", self.request.spec.tier.as_str()),
        ]
    }
    fn continuation_guard(&self) -> Option<Arc<dyn ContinuationGuard>> {
        Some(self.guard.clone())
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<MemberDisk> {
        let id = self.request.spec.id.clone();
        let job = self.request.job;
        ctx.report_progress("preparing shrink", 0, None);
        let current = disk_result(
            ctx.activity(&self.domains.disk, DiskCommand::Inspect(id.clone()))
                .await?,
        )?;
        if current.membership == DiskMembership::Removed {
            ctx.activity(
                &self.domains.membership,
                crate::pool::MembershipCommand::RetireDisk(id.clone()),
            )
            .await?;
            ctx.report_progress("removed", 1, Some(1));
            return Ok(current);
        }

        ctx.activity(
            &self.domains.disk,
            DiskCommand::ShrinkIntent(id.clone(), job),
        )
        .await?;
        ctx.activity(
            &self.domains.disk,
            DiskCommand::DisableAllocation(id.clone(), job),
        )
        .await?;

        // Exactly one pending Activity and one durable job for the entire evacuation.
        // Normal topo pauses never leave this call or create another execution.
        ctx.activity(
            &self.domains.balance,
            BalanceCommand::Evacuate {
                disk: id.clone(),
                id: job,
            },
        )
        .await?;

        ctx.report_progress("verifying committed references and allocations", 0, None);
        ctx.activity(
            &self.domains.vd,
            VdCommand::RequireDiskUnreferenced(id.clone()),
        )
        .await?;
        ctx.activity(&self.domains.disk, DiskCommand::RequireEmpty(id.clone()))
            .await?;

        ctx.report_progress("stopping IO and removing member", 0, None);
        let removed = match ctx
            .activity(
                &self.domains.membership,
                crate::pool::MembershipCommand::FinalizeShrink(id, job),
            )
            .await?
        {
            crate::pool::MembershipReply::Disk(disk) => disk,
            _ => return Err(Failure::Uncertain("unexpected finalization reply".into())),
        };
        ctx.report_progress("removed", 1, Some(1));
        Ok(removed)
    }
}

/// Explicitly abandon a cancelled/failed shrink intent. Keeps allocation disabled and
/// never rolls back remaps. It waits behind any still-running shrink of this disk.
pub struct AbortShrinkDiskWorkflow {
    pub(crate) domains: Domains,
    pub(crate) id: DiskId,
    pub(crate) job: EvacuationId,
}
#[async_trait]
impl Workflow for AbortShrinkDiskWorkflow {
    type Output = MemberDisk;
    const NAME: &'static str = "storage.abort-shrink-disk";
    fn objects(&self) -> Vec<ObjectKey> {
        vec![object("disk", self.id.as_str())]
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<MemberDisk> {
        ctx.activity(
            &self.domains.balance,
            BalanceCommand::Cancel {
                disk: self.id.clone(),
                id: self.job,
            },
        )
        .await?;
        disk_result(
            ctx.activity(
                &self.domains.disk,
                DiskCommand::ClearShrink(self.id, self.job),
            )
            .await?,
        )
    }
}
