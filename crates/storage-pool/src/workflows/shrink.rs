//! Remove a batch of member disks from a pool after evacuating their BGs.
//! VD identities, BG counts and replica counts are preserved; only placement changes.
//! One task and one target batch; domain services own all storage state.
use crate::domains::{balance::*, disk::service::DiskCommand, vd::service::VdCommand};
use crate::task_center::{OperationKind, PoolOperation, PoolWorkflow};
use crate::{manager::DiskRemoval, support::reject, *};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use workflow_foundation::{persistence::SavedTask, *};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShrinkRequest {
    pub disks: Vec<DiskId>,
}
impl ShrinkRequest {
    pub fn new(mut disks: Vec<DiskId>) -> Outcome<Self> {
        if disks.is_empty() || disks.iter().collect::<BTreeSet<_>>().len() != disks.len() {
            return Err(reject("shrink requires a nonempty set of distinct disks"));
        }
        disks.sort();
        Ok(Self { disks })
    }
}
/// Workflow checkpoint only; never a domain command or a disk's business state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ShrinkStage {
    #[default]
    RecordIntent,
    DisableAllocation,
    Migrate,
    VerifyEvacuated,
    DisableIo,
    RemoveMembers,
    Complete,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ShrinkProgress {
    pub stage: ShrinkStage,
    pub migration: MigrationCheckpoint,
}
#[derive(Serialize, Deserialize)]
struct Input {
    pool: PoolId,
    request: ShrinkRequest,
}
struct Conditions {
    domains: Domains,
    disks: Vec<DiskId>,
    stage: Arc<Mutex<ShrinkStage>>,
}
#[async_trait]
impl RunCondition for Conditions {
    async fn check(&self) -> Outcome<Readiness> {
        let stage = *self.stage.lock().unwrap();
        if stage == ShrinkStage::Complete {
            return Ok(Readiness::Ready);
        }
        let state = self.domains.disk_state.authoritative().await?;
        let volumes = self.domains.vd_state.authoritative().await?.volumes;
        for id in &self.disks {
            let Some(disk) = state.disks.iter().find(|d| &d.id == id) else {
                // Removal may have committed immediately before a checkpoint write.
                if stage == ShrinkStage::RemoveMembers && state.retired.contains(id) {
                    continue;
                }
                return Err(reject("shrink target is not a pool member"));
            };
            if stage <= ShrinkStage::Migrate && disk.io != IoState::Up {
                return Ok(Readiness::Blocked(format!(
                    "target {} is not online",
                    id.as_str()
                )));
            }
        }
        if stage > ShrinkStage::Migrate {
            return Ok(Readiness::Ready);
        }
        evacuation_readiness(&state.disks, &volumes, &self.disks)
    }
}

pub struct ShrinkWorkflow {
    request: ShrinkRequest,
    domains: Domains,
    stage: Arc<Mutex<ShrinkStage>>,
    durable: bool,
    membership: Option<DiskRemoval>,
}
impl ShrinkWorkflow {
    pub fn new(request: ShrinkRequest, domains: Domains) -> Outcome<Self> {
        if domains.balance.is_none() {
            return Err(reject("balance backend is not configured"));
        }
        Ok(Self {
            request: ShrinkRequest::new(request.disks)?,
            domains,
            stage: Arc::new(Mutex::new(ShrinkStage::RecordIntent)),
            durable: false,
            membership: None,
        })
    }
    pub fn durable(mut self) -> Self {
        self.durable = true;
        self
    }
    pub(crate) fn with_membership(mut self, membership: DiskRemoval) -> Self {
        self.membership = Some(membership);
        self
    }
    async fn save(&self, ctx: &mut WorkflowContext, progress: &ShrinkProgress) -> Outcome<()> {
        *self.stage.lock().unwrap() = progress.stage;
        ctx.report_details(progress)?;
        if self.durable {
            ctx.save_checkpoint(progress).await.map_err(|e| {
                Failure::Uncertain(format!("shrink checkpoint was not confirmed: {e}"))
            })?;
        }
        Ok(())
    }
    async fn advance(
        &self,
        ctx: &mut WorkflowContext,
        progress: &mut ShrinkProgress,
        next: ShrinkStage,
    ) -> Outcome<()> {
        progress.stage = next;
        ctx.report_progress(
            format!("{:?}", progress.stage),
            progress.stage as u64,
            Some(ShrinkStage::Complete as u64),
        );
        self.save(ctx, progress).await
    }
    async fn execute(&self, ctx: &mut WorkflowContext) -> Outcome<()> {
        use ShrinkStage::*;
        let mut progress: ShrinkProgress = if ctx.restored_checkpoint().is_null() {
            ShrinkProgress::default()
        } else {
            serde_json::from_value(ctx.restored_checkpoint().clone())
                .map_err(|e| reject(e.to_string()))?
        };
        let disks = &self.request.disks;

        if progress.stage == RecordIntent {
            ctx.activity(
                &self.domains.disk,
                DiskCommand::RequestShrink(disks.clone()),
            )
            .await?;
            self.advance(ctx, &mut progress, DisableAllocation).await?;
        }
        if progress.stage == DisableAllocation {
            ctx.activity(
                &self.domains.disk,
                DiskCommand::DisableAllocation(disks.clone()),
            )
            .await?;
            self.advance(ctx, &mut progress, Migrate).await?;
        }
        while progress.stage == Migrate {
            let exit = ctx
                .activity(
                    self.domains
                        .balance
                        .as_ref()
                        .expect("validated at construction"),
                    crate::domains::balance::Migrate {
                        pool: self.domains.pool.clone(),
                        task_id: ctx.id(),
                        disks: disks.clone(),
                        checkpoint: progress.migration.clone(),
                    },
                )
                .await?;
            match exit {
                ActivityExit::Completed(done) => {
                    progress.migration.progress = done;
                    self.advance(ctx, &mut progress, VerifyEvacuated).await?;
                }
                ActivityExit::Suspended(checkpoint) => {
                    // The native invocation has ended. Persist BEFORE acknowledging
                    // suspension and yielding to topology; resume creates a new Activity.
                    progress.migration = checkpoint;
                    self.save(ctx, &progress).await?;
                    ctx.wait_until_runnable().await?;
                }
            }
        }
        if progress.stage == VerifyEvacuated {
            ctx.activity(
                &self.domains.vd,
                VdCommand::RequireDisksUnreferenced(disks.clone()),
            )
            .await?;
            ctx.activity(
                &self.domains.disk,
                DiskCommand::RequireEmptyMany(disks.clone()),
            )
            .await?;
            self.advance(ctx, &mut progress, DisableIo).await?;
        }
        if progress.stage == DisableIo {
            ctx.activity(
                &self.domains.disk,
                DiskCommand::SetIoMany(disks.clone(), IoState::Down),
            )
            .await?;
            self.advance(ctx, &mut progress, RemoveMembers).await?;
        }
        if progress.stage == RemoveMembers {
            ctx.activity(
                &self.domains.disk,
                DiskCommand::RemoveEmptyMany(disks.clone()),
            )
            .await?;
            // No await between confirmed removal and the Pool's index cleanup.
            if let Some(membership) = &self.membership {
                membership.complete();
            }
            self.advance(ctx, &mut progress, Complete).await?;
        }
        Ok(())
    }
}
impl PoolWorkflow for ShrinkWorkflow {
    fn operation(&self) -> PoolOperation {
        PoolOperation {
            kind: OperationKind::Shrink(self.request.disks.clone()),
            objects: self.objects(),
        }
    }
}
#[async_trait]
impl Workflow for ShrinkWorkflow {
    type Output = ();
    const NAME: &'static str = "storage.shrink";
    fn objects(&self) -> Vec<ObjectKey> {
        self.request
            .disks
            .iter()
            .map(|d| ObjectKey::new("disk", d.as_str()))
            .collect()
    }
    fn details(&self) -> Outcome<serde_json::Value> {
        Ok(serde_json::to_value(ShrinkProgress::default()).expect("serializable progress"))
    }
    fn persistence(&self) -> Outcome<Persistence> {
        if self.durable {
            Persistence::durable(
                1,
                &Input {
                    pool: self.domains.pool.clone(),
                    request: self.request.clone(),
                },
            )
        } else {
            Ok(Persistence::Memory)
        }
    }
    fn restore(record: &SavedTask, dependencies: &Dependencies) -> Outcome<Self> {
        if record.version != 1 {
            return Err(reject("unsupported shrink version"));
        }
        let input: Input =
            serde_json::from_value(record.input.clone()).map_err(|e| reject(e.to_string()))?;
        let domains = (*dependencies.get::<Domains>()?).clone();
        if domains.pool != input.pool {
            return Err(reject("shrink recovery belongs to another pool"));
        }
        let workflow = Self::new(input.request, domains)?.durable();
        if !record.checkpoint.is_null() {
            let saved: ShrinkProgress = serde_json::from_value(record.checkpoint.clone())
                .map_err(|e| reject(e.to_string()))?;
            *workflow.stage.lock().unwrap() = saved.stage;
        }
        Ok(workflow)
    }
    fn run_condition(&self) -> Option<Arc<dyn RunCondition>> {
        Some(Arc::new(Conditions {
            domains: self.domains.clone(),
            disks: self.request.disks.clone(),
            stage: self.stage.clone(),
        }))
    }
    async fn run(mut self, ctx: &mut WorkflowContext) -> Outcome<()> {
        if let Some(membership) = &mut self.membership {
            membership.begin();
        }
        let result = self.execute(ctx).await;
        if let Some(membership) = &mut self.membership {
            membership.settled(&result);
        }
        result
    }
}
