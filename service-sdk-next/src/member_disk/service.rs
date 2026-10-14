use super::{effects::DiskEffects, *};
use crate::{
    process_manager::{ExecutionScope, ObjectClaim, ObjectKey, WorkflowContext},
    runtime::{CallError, activity::*},
};
use async_trait::async_trait;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct DiskConfig {
    pub retry_delay: Duration,
    pub max_retries: u32,
}

/// Owns the disk catalog and commits. Hosted by its own ActivityService Runtime.
/// Object arbitration happens exclusively in the Pool's ProcessManager.
pub struct DiskService {
    scope: ExecutionScope,
    effects: DiskEffects,
}
pub struct DrainTicket {
    pub(crate) scope: ExecutionScope,
    pub(crate) disk: DiskUuid,
    pub(crate) version: u64,
}
pub struct DrainProof {
    pub(crate) ticket: DrainTicket,
}
pub enum DiskRequest {
    CloseIo(DiskUuid),
    BringOnline(DiskUuid),
    DisableAllocation(DiskUuid),
    PrepareDrain(DiskUuid),
    Remove(DrainProof),
    Allocate(usize),
    Release(Vec<AllocatedBlock>),
}
pub enum DiskReply {
    Done,
    Ticket(DrainTicket),
    Allocated(Vec<AllocatedBlock>),
}

impl DiskService {
    pub(crate) fn new(
        pool: &str,
        scope: ExecutionScope,
        disks: impl IntoIterator<Item = MemberDisk>,
        metadata: Arc<dyn MemberDiskMetadata>,
        nodes: Arc<dyn PoolNodes>,
        config: DiskConfig,
    ) -> Result<Self, MemberDiskError> {
        let mut members = BTreeMap::new();
        for disk in disks {
            if disk.pool() != pool {
                return Err(MemberDiskError::InvalidRequest(
                    "member belongs to another Pool",
                ));
            }
            if members.insert(disk.uuid().clone(), disk).is_some() {
                return Err(MemberDiskError::InvalidRequest("duplicate disk UUID"));
            }
        }
        Ok(Self {
            scope,
            effects: DiskEffects::new(members, metadata, nodes, config),
        })
    }
    async fn space(
        &self,
        plan: Vec<MemberDiskCommit>,
        allocating: bool,
        cancel: &CancellationToken,
    ) -> Result<Vec<AllocatedBlock>, MemberDiskError> {
        let mut committed = Vec::new();
        for commit in plan {
            let result = if cancel.is_cancelled() {
                Err(MemberDiskError::Cancelled)
            } else {
                self.effects
                    .commit(&commit.disk, commit.change.clone(), cancel, false)
                    .await
            };
            if let Err(cause) = result {
                return if committed.is_empty() {
                    Err(cause)
                } else {
                    Err(MemberDiskError::PartialSpace {
                        allocated: allocating,
                        committed,
                        cause: Box::new(cause),
                    })
                };
            }
            let blocks = match commit.change {
                MemberDiskChange::Allocate(b) | MemberDiskChange::Release(b) => b,
                _ => unreachable!(),
            };
            committed.extend(blocks.into_iter().map(|block| AllocatedBlock {
                disk: commit.disk.clone(),
                block,
            }));
        }
        Ok(committed)
    }
}
#[async_trait]
impl ActivityHandler for DiskService {
    type Request = DiskRequest;
    type Reply = DiskReply;
    type Query = Option<DiskUuid>;
    type QueryReply = Vec<MemberDisk>;
    type Error = MemberDiskError;
    fn claims(request: &DiskRequest) -> Vec<ObjectClaim> {
        let object = match request {
            DiskRequest::CloseIo(d)
            | DiskRequest::BringOnline(d)
            | DiskRequest::DisableAllocation(d)
            | DiskRequest::PrepareDrain(d) => ObjectKey::new("disk", d.to_string()),
            DiskRequest::Remove(p) => ObjectKey::new("disk", p.ticket.disk.to_string()),
            DiskRequest::Allocate(_) | DiskRequest::Release(_) => ObjectKey::collection("disk"),
        };
        vec![ObjectClaim::exclusive(object)]
    }
    fn name(request: &DiskRequest) -> &'static str {
        match request {
            DiskRequest::CloseIo(_) => "disk.close-io",
            DiskRequest::BringOnline(_) => "disk.online",
            DiskRequest::DisableAllocation(_) => "disk.disable-allocation",
            DiskRequest::PrepareDrain(_) => "disk.prepare-drain",
            DiskRequest::Remove(_) => "disk.remove",
            DiskRequest::Allocate(_) => "disk.allocate",
            DiskRequest::Release(_) => "disk.release",
        }
    }
    fn query(&self, disk: Option<DiskUuid>) -> Result<Vec<MemberDisk>, MemberDiskError> {
        match disk {
            Some(d) => Ok(vec![self.effects.get(&d)?]),
            None => Ok(self.effects.list()),
        }
    }
    async fn execute(
        &self,
        request: DiskRequest,
        cancel: CancellationToken,
    ) -> Result<DiskReply, MemberDiskError> {
        match request {
            DiskRequest::CloseIo(d) => self.effects.close_io(&d).await?,
            DiskRequest::BringOnline(d) => self.effects.bring_online(&d, &cancel).await?,
            DiskRequest::DisableAllocation(d) => {
                self.effects.disable_allocation(&d, &cancel).await?
            }
            DiskRequest::PrepareDrain(d) => {
                if !matches!(
                    self.effects.get(&d)?.state(),
                    MemberDiskState::UpInactive | MemberDiskState::DownInactive
                ) {
                    return Err(MemberDiskError::InvalidState {
                        disk: d,
                        reason: "drain requires inactive allocation",
                    });
                }
                return Ok(DiskReply::Ticket(DrainTicket {
                    scope: self.scope.clone(),
                    version: self.effects.version(&d),
                    disk: d,
                }));
            }
            DiskRequest::Remove(proof) => {
                let ticket = proof.ticket;
                if !ticket.scope.same(&self.scope) {
                    return Err(MemberDiskError::InvalidRequest("foreign drain proof"));
                }
                if self.effects.version(&ticket.disk) != ticket.version {
                    return Err(MemberDiskError::InvalidState {
                        disk: ticket.disk,
                        reason: "stale drain proof",
                    });
                }
                self.effects.remove(&ticket.disk, &cancel).await?;
            }
            DiskRequest::Allocate(count) => {
                return self
                    .space(self.effects.allocation_plan(count)?, true, &cancel)
                    .await
                    .map(DiskReply::Allocated);
            }
            DiskRequest::Release(blocks) => {
                self.space(self.effects.release_plan(blocks)?, false, &cancel)
                    .await?;
            }
        }
        Ok(DiskReply::Done)
    }
}

#[derive(Clone)]
pub struct DiskClient(pub(crate) ActivityClient<DiskService>);
impl DiskClient {
    pub async fn get(&self, disk: DiskUuid) -> Result<MemberDisk, CallError<MemberDiskError>> {
        Ok(self.0.query(Some(disk)).await?.remove(0))
    }
    pub async fn list(&self) -> Result<Vec<MemberDisk>, CallError<MemberDiskError>> {
        self.0.query(None).await
    }
    pub async fn close_io(
        &self,
        ctx: &mut WorkflowContext,
        disk: DiskUuid,
    ) -> Result<(), MemberDiskError> {
        self.0
            .call(ctx, DiskRequest::CloseIo(disk))
            .await
            .map(|_| ())
    }
    pub async fn bring_online(
        &self,
        ctx: &mut WorkflowContext,
        disk: DiskUuid,
    ) -> Result<(), MemberDiskError> {
        self.0
            .call(ctx, DiskRequest::BringOnline(disk))
            .await
            .map(|_| ())
    }
    pub async fn disable_allocation(
        &self,
        ctx: &mut WorkflowContext,
        disk: DiskUuid,
    ) -> Result<(), MemberDiskError> {
        self.0
            .call(ctx, DiskRequest::DisableAllocation(disk))
            .await
            .map(|_| ())
    }
    pub async fn prepare_drain(
        &self,
        ctx: &mut WorkflowContext,
        disk: DiskUuid,
    ) -> Result<DrainTicket, MemberDiskError> {
        match self.0.call(ctx, DiskRequest::PrepareDrain(disk)).await? {
            DiskReply::Ticket(t) => Ok(t),
            _ => unreachable!(),
        }
    }
    pub async fn remove(
        &self,
        ctx: &mut WorkflowContext,
        proof: DrainProof,
    ) -> Result<(), MemberDiskError> {
        self.0
            .call(ctx, DiskRequest::Remove(proof))
            .await
            .map(|_| ())
    }
    pub async fn allocate(
        &self,
        ctx: &mut WorkflowContext,
        count: usize,
    ) -> Result<Vec<AllocatedBlock>, MemberDiskError> {
        match self.0.call(ctx, DiskRequest::Allocate(count)).await? {
            DiskReply::Allocated(b) => Ok(b),
            _ => unreachable!(),
        }
    }
    pub async fn release(
        &self,
        ctx: &mut WorkflowContext,
        blocks: Vec<AllocatedBlock>,
    ) -> Result<(), MemberDiskError> {
        self.0
            .call(ctx, DiskRequest::Release(blocks))
            .await
            .map(|_| ())
    }
}
