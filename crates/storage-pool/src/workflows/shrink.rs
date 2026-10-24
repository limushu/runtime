//! One shrink task, one target batch. Only migration may end an Activity on suspend.
use crate::domains::{balance::*, shrink::*};
use crate::task_center::{OperationKind, PoolOperation, PoolWorkflow};
use crate::{DiskId, PoolId, ShrinkServices};
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
    pub fn new(disks: Vec<DiskId>) -> Outcome<Self> {
        if disks.is_empty() || disks.iter().collect::<BTreeSet<_>>().len() != disks.len() {
            return Err(Failure::Rejected(
                "shrink requires a nonempty set of distinct disks".into(),
            ));
        }
        let mut disks = disks;
        disks.sort();
        Ok(Self { disks })
    }
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
    services: ShrinkServices,
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
        self.services
            .conditions
            .readiness(&self.services.pool, &self.disks, stage)
            .await
    }
}

pub struct ShrinkWorkflow {
    request: ShrinkRequest,
    services: ShrinkServices,
    stage: Arc<Mutex<ShrinkStage>>,
    durable: bool,
}
impl ShrinkWorkflow {
    pub fn new(request: ShrinkRequest, services: ShrinkServices) -> Outcome<Self> {
        Ok(Self {
            request: ShrinkRequest::new(request.disks)?,
            services,
            stage: Arc::new(Mutex::new(ShrinkStage::RecordIntent)),
            durable: false,
        })
    }
    /// Opt into the host's configured TaskStore. This never implies that C Futures
    /// are serialized: every invocation must reconcile from durable business data.
    pub fn durable(mut self) -> Self {
        self.durable = true;
        self
    }

    async fn save(&self, ctx: &mut WorkflowContext, progress: &ShrinkProgress) -> Outcome<()> {
        *self.stage.lock().unwrap() = progress.stage;
        ctx.report_details(progress)?;
        if self.durable {
            ctx.save_checkpoint(progress).await.map_err(|error| {
                Failure::Uncertain(format!("shrink checkpoint was not confirmed: {error}"))
            })?;
        }
        Ok(())
    }
    async fn storage_step(
        &self,
        ctx: &mut WorkflowContext,
        progress: &mut ShrinkProgress,
        step: ShrinkStage,
        next: ShrinkStage,
    ) -> Outcome<()> {
        if progress.stage != step {
            return Ok(());
        }
        ctx.report_progress(
            format!("{step:?}"),
            step as u64,
            Some(ShrinkStage::Complete as u64),
        );
        ctx.activity(
            &self.services.storage,
            ShrinkCommand {
                pool: self.services.pool.clone(),
                disks: self.request.disks.clone(),
                stage: step,
            },
        )
        .await?;
        progress.stage = next;
        self.save(ctx, progress).await
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
        Ok(serde_json::to_value(ShrinkProgress::default()).expect("serializable shrink progress"))
    }
    fn persistence(&self) -> Outcome<Persistence> {
        if self.durable {
            Persistence::durable(
                1,
                &Input {
                    pool: self.services.pool.clone(),
                    request: self.request.clone(),
                },
            )
        } else {
            Ok(Persistence::Memory)
        }
    }
    fn restore(record: &SavedTask, dependencies: &Dependencies) -> Outcome<Self> {
        if record.version != 1 {
            return Err(Failure::Rejected("unsupported shrink version".into()));
        }
        let input: Input = serde_json::from_value(record.input.clone())
            .map_err(|e| Failure::Rejected(e.to_string()))?;
        let services = (*dependencies.get::<ShrinkServices>()?).clone();
        if services.pool != input.pool {
            return Err(Failure::Rejected(
                "shrink recovery belongs to another pool".into(),
            ));
        }
        let workflow = Self::new(input.request, services)?.durable();
        if !record.checkpoint.is_null() {
            let saved: ShrinkProgress = serde_json::from_value(record.checkpoint.clone())
                .map_err(|e| Failure::Rejected(e.to_string()))?;
            *workflow.stage.lock().unwrap() = saved.stage;
        }
        Ok(workflow)
    }
    fn run_condition(&self) -> Option<Arc<dyn RunCondition>> {
        Some(Arc::new(Conditions {
            services: self.services.clone(),
            disks: self.request.disks.clone(),
            stage: self.stage.clone(),
        }))
    }
    async fn run(self, ctx: &mut WorkflowContext) -> Outcome<()> {
        use ShrinkStage::*;
        let mut progress = if ctx.restored_checkpoint().is_null() {
            ShrinkProgress::default()
        } else {
            serde_json::from_value(ctx.restored_checkpoint().clone())
                .map_err(|e| Failure::Rejected(e.to_string()))?
        };

        // The ordered business process. Each storage call receives the whole disk set.
        self.storage_step(ctx, &mut progress, RecordIntent, DisableAllocation)
            .await?;
        self.storage_step(ctx, &mut progress, DisableAllocation, Migrate)
            .await?;

        while progress.stage == Migrate {
            let reply = ctx
                .activity(
                    &self.services.balance,
                    crate::domains::balance::Migrate {
                        pool: self.services.pool.clone(),
                        task_id: ctx.id(),
                        disks: self.request.disks.clone(),
                        checkpoint: progress.migration.clone(),
                    },
                )
                .await?;
            match reply {
                ActivityExit::Completed(done) => {
                    progress.migration.progress = done;
                    progress.stage = VerifyEvacuated;
                    self.save(ctx, &progress).await?;
                }
                ActivityExit::Suspended(checkpoint) => {
                    // Domain invocation is already finished. Save the continuation
                    // BEFORE acknowledging suspension and allowing topology to run.
                    progress.migration = checkpoint;
                    self.save(ctx, &progress).await?;
                    ctx.wait_until_runnable().await?;
                }
            }
        }

        self.storage_step(ctx, &mut progress, VerifyEvacuated, DisableIo)
            .await?;
        self.storage_step(ctx, &mut progress, DisableIo, RemoveMembers)
            .await?;
        self.storage_step(ctx, &mut progress, RemoveMembers, Complete)
            .await?;
        ctx.report_progress("completed", 6, Some(6));
        Ok(())
    }
}
