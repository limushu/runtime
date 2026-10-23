//! A whole-disk business process. BG planning/copy/remap stays in the balancing domain.
use super::{Domains, disk_result};
use crate::manager::PoolShared;
use crate::{
    domains::{
        balance::{service::BalanceService, *},
        disk::service::DiskCommand,
        vd::service::VdCommand,
    },
    ports::*,
    support::{object, reject},
    *,
};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};
use workflow_foundation::{
    ContinuationGuard, ContinueDecision, ObjectKey, Workflow, WorkflowContext,
};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct ShrinkRequest {
    pub spec: DiskSpec,
    pub job: EvacuationId,
}
#[derive(Serialize, Deserialize)]
enum Checkpoint {
    IntentCommitted,
    AllocationDisabled,
    Evacuated,
    Removed,
}

struct ShrinkGuard {
    request: ShrinkRequest,
    metadata: Arc<dyn MetadataStore>,
    balance: Arc<BalanceService>,
    shared: Arc<PoolShared>,
}
#[async_trait]
impl ContinuationGuard for ShrinkGuard {
    async fn check(&self) -> Outcome<ContinueDecision> {
        if self.shared.descriptor.lock().unwrap().lifecycle != PoolLifecycle::Active {
            return Ok(ContinueDecision::Wait("pool is not active".into()));
        }
        let snapshot = self.metadata.snapshot(&self.balance.pool).await?;
        let disk = snapshot
            .disks
            .iter()
            .find(|d| d.id == self.request.spec.id)
            .ok_or_else(|| reject("shrink disk is absent"))?;
        if disk.pool != self.balance.pool || self.request.job.0.is_nil() {
            return Err(reject("invalid shrink identity or pool"));
        }
        disk.require_identity(&self.request.spec)?;
        if disk.shrink.is_some_and(|id| id != self.request.job) {
            return Err(reject("disk belongs to another shrink intent"));
        }
        if disk.membership == DiskMembership::Removed {
            return if disk.shrink == Some(self.request.job) {
                Ok(ContinueDecision::Ready)
            } else {
                Err(reject("disk identity is removed"))
            };
        }
        if let Some(job) = self.balance.find(self.request.job).await?
            && job.progress().borrow().state == EvacuationState::Cancelled
        {
            return Err(reject(
                "evacuation is cancelled; explicitly abandon the shrink intent before starting again",
            ));
        }
        Ok(match self.balance.check(&disk.id).await? {
            ShrinkReadiness::Ready => ContinueDecision::Ready,
            ShrinkReadiness::Wait(reason) => ContinueDecision::Wait(reason),
        })
    }
}

pub struct ShrinkDiskWorkflow {
    domains: Domains,
    request: ShrinkRequest,
    guard: Arc<ShrinkGuard>,
    routing: Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
    durable: bool,
}
impl ShrinkDiskWorkflow {
    pub(crate) fn new(
        domains: Domains,
        request: ShrinkRequest,
        metadata: Arc<dyn MetadataStore>,
        balance: Arc<BalanceService>,
        shared: Arc<PoolShared>,
        routing: Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
        durable: bool,
    ) -> Self {
        let guard = Arc::new(ShrinkGuard {
            request: request.clone(),
            metadata,
            balance,
            shared,
        });
        Self {
            domains,
            request,
            guard,
            routing,
            durable,
        }
    }
    pub(crate) fn resume_spec(&self) -> Outcome<crate::task_center::persistence::ResumeSpec> {
        Ok(crate::task_center::persistence::ResumeSpec {
            kind: Self::NAME.into(),
            version: 1,
            input: serde_json::to_vec(&self.request).map_err(|e| reject(e.to_string()))?,
            checkpoint: vec![],
        })
    }
    async fn checkpoint(&self, ctx: &WorkflowContext, checkpoint: Checkpoint) -> Outcome<()> {
        if self.durable {
            ctx.save_checkpoint(
                serde_json::to_vec(&checkpoint).map_err(|e| reject(e.to_string()))?,
            )
            .await?;
        }
        Ok(())
    }
    fn retire_directory(&self) -> Outcome<()> {
        crate::manager::retire_disk(&self.guard.shared, &self.routing, &self.request.spec.id)
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
        ctx.activity(&self.domains.disk, DiskCommand::Refresh)
            .await?;
        let current = disk_result(
            ctx.activity(&self.domains.disk, DiskCommand::Inspect(id.clone()))
                .await?,
        )?;
        if current.membership == DiskMembership::Removed {
            self.retire_directory()?;
            ctx.report_progress("removed", 1, Some(1));
            return Ok(current);
        }

        ctx.activity(
            &self.domains.disk,
            DiskCommand::ShrinkIntent(id.clone(), job),
        )
        .await?;
        self.checkpoint(ctx, Checkpoint::IntentCommitted).await?;
        ctx.activity(
            &self.domains.disk,
            DiskCommand::DisableAllocation(id.clone(), job),
        )
        .await?;
        self.checkpoint(ctx, Checkpoint::AllocationDisabled).await?;

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
        self.checkpoint(ctx, Checkpoint::Evacuated).await?;

        ctx.report_progress("verifying committed references and allocations", 0, None);
        ctx.activity(&self.domains.vd, VdCommand::Refresh).await?;
        ctx.activity(&self.domains.disk, DiskCommand::Refresh)
            .await?;
        ctx.activity(
            &self.domains.vd,
            VdCommand::RequireDiskUnreferenced(id.clone()),
        )
        .await?;
        ctx.activity(&self.domains.disk, DiskCommand::RequireEmpty(id.clone()))
            .await?;

        ctx.report_progress("stopping IO and removing member", 0, None);
        let removed = disk_result(
            ctx.activity(&self.domains.disk, DiskCommand::FinalizeShrink(id, job))
                .await?,
        )?;
        self.retire_directory()?;
        self.checkpoint(ctx, Checkpoint::Removed).await?;
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
        ctx.activity(&self.domains.disk, DiskCommand::Refresh)
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

impl PoolHandle {
    pub async fn shrink_disk(&self, id: DiskId) -> Outcome<Run<MemberDisk>> {
        self.descriptor().active()?;
        let snapshot = self.metadata.snapshot(&self.id).await?;
        let disk = snapshot
            .disks
            .into_iter()
            .find(|d| d.id == id)
            .ok_or_else(|| reject("unknown disk"))?;
        if disk.membership == DiskMembership::Removed {
            return Err(reject("disk is removed"));
        }
        let request = ShrinkRequest {
            spec: DiskSpec {
                id,
                node: disk.node,
                tier: disk.tier,
                capacity_blks: disk.capacity_blks,
            },
            job: disk.shrink.unwrap_or_default(),
        };
        let workflow = self.build_shrink(request);
        if self.task_center.has_task_store() {
            let resume = workflow.resume_spec()?;
            Ok(self
                .task_center
                .submit_resumable_shrink(workflow, resume)
                .await?
                .run)
        } else {
            self.task_center.submit_shrink(workflow).await
        }
    }
    pub(crate) fn build_shrink(&self, request: ShrinkRequest) -> ShrinkDiskWorkflow {
        ShrinkDiskWorkflow::new(
            self.domains.clone(),
            request,
            self.metadata.clone(),
            self.balance.clone(),
            self.shared.clone(),
            self.task_center.disk_directory(),
            self.task_center.has_task_store(),
        )
    }
    pub async fn abort_shrink_disk(&self, id: DiskId) -> Outcome<Run<MemberDisk>> {
        self.descriptor().active()?;
        let disk = self
            .metadata
            .snapshot(&self.id)
            .await?
            .disks
            .into_iter()
            .find(|d| d.id == id)
            .ok_or_else(|| reject("unknown disk"))?;
        let job = disk
            .shrink
            .ok_or_else(|| reject("disk has no shrink intent"))?;
        self.task_center
            .submit_task(AbortShrinkDiskWorkflow {
                domains: self.domains.clone(),
                id,
                job,
            })
            .await
    }
}
