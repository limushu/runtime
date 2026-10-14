use super::{admission::Admission, effects::DiskEffects, *};
use crate::runtime::Lifecycle;
use std::{collections::BTreeMap, future::Future, sync::Arc, time::Duration};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct DiskConfig {
    pub retry_delay: Duration,
    pub max_retries: u32,
}

/// Pool-scoped domain capability. Owns catalog, publication and resource
/// arbitration, but no workflow queue, operation directory or business process.
#[derive(Clone)]
pub struct DiskService {
    pool: Arc<str>,
    identity: Arc<()>,
    effects: DiskEffects,
    admission: Admission,
}

/// Validated inactive member version, to be presented to the VD domain.
pub struct DrainTicket {
    pub(crate) pool: Arc<str>,
    pub(crate) identity: Arc<()>,
    pub(crate) disk: DiskUuid,
    pub(crate) version: u64,
}

/// Only the VD capability can construct a proof after successful evacuation.
pub struct DrainProof {
    pub(crate) ticket: DrainTicket,
}

impl DiskService {
    pub(crate) fn new(
        pool: Arc<str>,
        disks: impl IntoIterator<Item = MemberDisk>,
        metadata: Arc<dyn MemberDiskMetadata>,
        nodes: Arc<dyn PoolNodes>,
        config: DiskConfig,
        changed: Arc<Notify>,
    ) -> Result<Self, MemberDiskError> {
        let mut members = BTreeMap::new();
        for disk in disks {
            if disk.pool() != pool.as_ref() {
                return Err(MemberDiskError::InvalidRequest(
                    "member belongs to another Pool",
                ));
            }
            let id = disk.uuid().clone();
            if members.insert(id.clone(), disk).is_some() {
                return Err(MemberDiskError::InvalidState {
                    disk: id,
                    reason: "duplicate UUID",
                });
            }
        }
        Ok(Self {
            pool,
            identity: Arc::new(()),
            effects: DiskEffects::new(members, metadata, nodes, config),
            admission: Admission::new(changed),
        })
    }

    pub fn pool_id(&self) -> &str {
        &self.pool
    }
    pub fn get(&self, disk: &DiskUuid) -> Result<MemberDisk, MemberDiskError> {
        self.effects.get(disk)
    }
    pub fn list(&self) -> Vec<MemberDisk> {
        self.effects.list()
    }
    pub fn blocked_resources(&self) -> Vec<Option<DiskUuid>> {
        self.admission.blocked()
    }
    pub(crate) fn identity(&self) -> Arc<()> {
        self.identity.clone()
    }
    pub(crate) fn lifecycle(&self, state: Lifecycle) {
        self.admission.lifecycle(state);
    }
    pub(crate) fn stop(&self) {
        self.admission.stop();
    }
    pub(crate) fn is_idle(&self) -> bool {
        self.admission.is_idle()
    }

    async fn execute<T, F, Fut>(
        &self,
        disk: Option<DiskUuid>,
        caller: &CancellationToken,
        action: F,
    ) -> Result<T, MemberDiskError>
    where
        F: FnOnce(CancellationToken) -> Fut,
        Fut: Future<Output = Result<T, MemberDiskError>>,
    {
        let mut reservation = self.admission.acquire(disk, caller).await?;
        let cancel = reservation.cancel.clone();
        let action = action(cancel.clone());
        tokio::pin!(action);
        let result = tokio::select! {
            biased;
            result = &mut action => result,
            _ = caller.cancelled() => { cancel.cancel(); action.await },
        };
        reservation.settled(&result);
        result
    }

    pub async fn close_io(
        &self,
        disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskError> {
        self.execute(Some(disk.clone()), cancel, |_| self.effects.close_io(disk))
            .await
    }
    pub async fn bring_online(
        &self,
        disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskError> {
        self.execute(Some(disk.clone()), cancel, |token| async move {
            self.effects.bring_online(disk, &token).await
        })
        .await
    }
    pub async fn disable_allocation(
        &self,
        disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskError> {
        self.execute(Some(disk.clone()), cancel, |token| async move {
            self.effects.disable_allocation(disk, &token).await
        })
        .await
    }

    pub async fn prepare_drain(
        &self,
        disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<DrainTicket, MemberDiskError> {
        self.execute(Some(disk.clone()), cancel, |_| async {
            // A subsequent domain mutation changes the version and invalidates the
            // ticket. The final validation occurs under the removal reservation.
            let version = self.effects.version(disk);
            if !matches!(
                self.get(disk)?.state(),
                MemberDiskState::UpInactive | MemberDiskState::DownInactive
            ) {
                return Err(MemberDiskError::InvalidState {
                    disk: disk.clone(),
                    reason: "drain requires inactive allocation",
                });
            }
            Ok(DrainTicket {
                pool: self.pool.clone(),
                identity: self.identity.clone(),
                disk: disk.clone(),
                version,
            })
        })
        .await
    }

    pub async fn remove(
        &self,
        proof: DrainProof,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskError> {
        let ticket = proof.ticket;
        if !Arc::ptr_eq(&ticket.identity, &self.identity) || ticket.pool != self.pool {
            return Err(MemberDiskError::InvalidRequest(
                "drain proof belongs to another Pool instance",
            ));
        }
        self.execute(Some(ticket.disk.clone()), cancel, |token| async move {
            if self.effects.version(&ticket.disk) != ticket.version {
                return Err(MemberDiskError::InvalidState {
                    disk: ticket.disk,
                    reason: "stale drain proof",
                });
            }
            self.effects.remove(&ticket.disk, &token).await
        })
        .await
    }

    pub async fn allocate(
        &self,
        count: usize,
        cancel: &CancellationToken,
    ) -> Result<Vec<AllocatedBlock>, MemberDiskError> {
        self.execute(None, cancel, |token| async move {
            self.space(self.effects.allocation_plan(count)?, true, &token)
                .await
        })
        .await
    }
    pub async fn release(
        &self,
        blocks: Vec<AllocatedBlock>,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskError> {
        self.execute(None, cancel, |token| async move {
            self.space(self.effects.release_plan(blocks)?, false, &token)
                .await
                .map(|_| ())
        })
        .await
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
