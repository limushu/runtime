use super::*;
use crate::support::reject;
use crate::workflows::*;
use crate::{domains::balance::*, metadata::*, workflows::ShrinkRequest};
use async_trait::async_trait;
use std::sync::Arc;
use workflow_foundation::{ContinuationGuard, ContinueDecision};

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
        let resume = crate::task_center::persistence::ResumeSpec {
            kind: "storage.shrink-disk".into(),
            version: 1,
            input: serde_json::to_vec(&request).map_err(|e| reject(e.to_string()))?,
            checkpoint: vec![],
        };
        let workflow = self.build_shrink(request);
        if self.task_center.has_task_store() {
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
        build_shrink(
            self.domains.clone(),
            request,
            self.metadata.clone(),
            self.balancer.clone(),
            self.shared.clone(),
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

struct ShrinkGuard {
    request: ShrinkRequest,
    metadata: Arc<dyn MetadataStore>,
    balance: Arc<dyn Balancer>,
    pool: PoolId,
    shared: Arc<PoolShared>,
}
#[async_trait]
impl ContinuationGuard for ShrinkGuard {
    async fn check(&self) -> Outcome<ContinueDecision> {
        if self.shared.descriptor.lock().unwrap().lifecycle != PoolLifecycle::Active {
            return Ok(ContinueDecision::Wait("pool is not active".into()));
        }
        let snapshot = self.metadata.snapshot(&self.pool).await?;
        let disk = snapshot
            .disks
            .iter()
            .find(|d| d.id == self.request.spec.id)
            .ok_or_else(|| reject("shrink disk is absent"))?;
        if disk.pool != self.pool || self.request.job.0.is_nil() {
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
        if let Some(job) = self.balance.find(&self.pool, self.request.job).await?
            && job.progress().borrow().state == EvacuationState::Cancelled
        {
            return Err(reject(
                "evacuation is cancelled; explicitly abandon the shrink intent before starting again",
            ));
        }
        Ok(
            match self.balance.check_shrink(&self.pool, &disk.id).await? {
                ShrinkReadiness::Ready => ContinueDecision::Ready,
                ShrinkReadiness::Wait(reason) => ContinueDecision::Wait(reason),
            },
        )
    }
}

pub(super) fn build_shrink(
    domains: Domains,
    request: ShrinkRequest,
    metadata: Arc<dyn MetadataStore>,
    balance: Arc<dyn Balancer>,
    shared: Arc<PoolShared>,
) -> ShrinkDiskWorkflow {
    let pool = shared.descriptor.lock().unwrap().id.clone();
    let guard = Arc::new(ShrinkGuard {
        request: request.clone(),
        metadata,
        balance,
        pool,
        shared,
    });
    ShrinkDiskWorkflow::new(domains, request, guard)
}
