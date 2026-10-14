//! Domain effects. Validation, SDB confirmation and publication form one
//! activity; neither a workflow nor a query can mutate the committed view.
use super::*;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

type CloseReply = oneshot::Sender<Result<(), MemberDiskError>>;

// A direct domain caller can be dropped independently of the Pool root. Do
// not leave coalesced followers waiting forever if the leader is dropped at
// the initial yield, before it has taken ownership of the queued senders.
struct CloseLeader {
    queue: Arc<Mutex<Vec<(DiskUuid, CloseReply)>>>,
    armed: bool,
}

impl CloseLeader {
    fn take(&mut self) -> Vec<(DiskUuid, CloseReply)> {
        let batch = std::mem::take(&mut *self.queue.lock().unwrap());
        self.armed = false;
        batch
    }
}

impl Drop for CloseLeader {
    fn drop(&mut self) {
        if self.armed {
            for (_, reply) in self.take() {
                let _ = reply.send(Err(MemberDiskError::NeedsRecovery(
                    "close batch leader dropped".into(),
                )));
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct DiskEffects {
    disks: Arc<Mutex<Catalog>>,
    metadata: Arc<dyn MemberDiskMetadata>,
    nodes: Arc<dyn PoolNodes>,
    config: DiskConfig,
    close_batch: Arc<Mutex<Vec<(DiskUuid, CloseReply)>>>,
}

impl DiskEffects {
    pub fn new(
        disks: BTreeMap<DiskUuid, MemberDisk>,
        metadata: Arc<dyn MemberDiskMetadata>,
        nodes: Arc<dyn PoolNodes>,
        config: DiskConfig,
    ) -> Self {
        Self {
            disks: Arc::new(Mutex::new(Catalog {
                members: disks,
                versions: BTreeMap::new(),
            })),
            metadata,
            nodes,
            config,
            close_batch: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub fn get(&self, disk: &DiskUuid) -> Result<MemberDisk, MemberDiskError> {
        self.disks
            .lock()
            .unwrap()
            .members
            .get(disk)
            .cloned()
            .ok_or_else(|| MemberDiskError::UnknownDisk(disk.clone()))
    }
    pub fn list(&self) -> Vec<MemberDisk> {
        self.disks
            .lock()
            .unwrap()
            .members
            .values()
            .cloned()
            .collect()
    }

    /// Coalesce simultaneously polled close activities. This is a provider
    /// optimization, not a Workflow/Action scheduler. All waiters get a result.
    pub async fn close_io(&self, disk: &DiskUuid) -> Result<(), MemberDiskError> {
        self.get(disk)?
            .validate(&MemberDiskChange::SetIo(IoState::Down))?;
        let (reply, response) = oneshot::channel();
        let leader = {
            let mut batch = self.close_batch.lock().unwrap();
            let leader = batch.is_empty();
            batch.push((disk.clone(), reply));
            leader
        };
        if leader {
            let mut leader = CloseLeader {
                queue: self.close_batch.clone(),
                armed: true,
            };
            tokio::task::yield_now().await;
            let batch = leader.take();
            let changes = batch
                .iter()
                .map(|(disk, _)| DiskStateChange {
                    disk: disk.clone(),
                    state: IoState::Down,
                })
                .collect();
            let sent = self.push_down(changes).await;
            let commits: Vec<_> = batch
                .iter()
                .map(|(disk, _)| MemberDiskCommit {
                    disk: disk.clone(),
                    change: MemberDiskChange::SetIo(IoState::Down),
                })
                .collect();
            let results = match sent {
                Ok(()) => self.commit_batch(commits, None, true).await,
                Err(error) => vec![Err(error); batch.len()],
            };
            for ((_, reply), result) in batch.into_iter().zip(results) {
                let _ = reply.send(result);
            }
        }
        response
            .await
            .unwrap_or_else(|_| Err(MemberDiskError::NeedsRecovery("close batch aborted".into())))
    }

    async fn push_down(&self, changes: Vec<DiskStateChange>) -> Result<(), MemberDiskError> {
        loop {
            match self.nodes.push_disk_states(changes.clone()).await {
                Ok(()) => return Ok(()),
                Err(error) if self.nodes.is_retryable(&error) => {
                    tokio::time::sleep(self.config.retry_delay).await
                }
                Err(error) => return Err(MemberDiskError::NeedsRecovery(error.0)),
            }
        }
    }

    pub async fn bring_online(
        &self,
        disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskError> {
        let opened = self
            .nodes
            .open_disks(vec![disk.clone()])
            .await
            .map_err(|e| MemberDiskError::NeedsRecovery(e.0))?;
        if opened.len() != 1 || opened[0].disk != *disk {
            return Err(MemberDiskError::NeedsRecovery(
                "invalid open_disks response".into(),
            ));
        }
        opened
            .into_iter()
            .next()
            .unwrap()
            .result
            .map_err(|e| MemberDiskError::PoolNodes(e.0))?;
        // Once UP is published, finish its metadata boundary even if cancelled.
        self.nodes
            .push_disk_states(vec![DiskStateChange {
                disk: disk.clone(),
                state: IoState::Up,
            }])
            .await
            .map_err(|e| MemberDiskError::NeedsRecovery(e.0))?;
        self.commit(disk, MemberDiskChange::BringOnline, cancel, true)
            .await
    }

    pub async fn disable_allocation(
        &self,
        disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskError> {
        self.commit(disk, MemberDiskChange::DisableAllocation, cancel, false)
            .await
    }

    pub async fn remove(
        &self,
        disk: &DiskUuid,
        cancel: &CancellationToken,
    ) -> Result<(), MemberDiskError> {
        self.get(disk)?.validate(&MemberDiskChange::Remove)?;
        self.push_down(vec![DiskStateChange {
            disk: disk.clone(),
            state: IoState::Down,
        }])
        .await?;
        self.commit(disk, MemberDiskChange::Remove, cancel, true)
            .await
    }

    pub fn allocation_plan(&self, count: usize) -> Result<Vec<MemberDiskCommit>, MemberDiskError> {
        if count == 0 {
            return Err(MemberDiskError::InvalidRequest(
                "allocation count must be greater than zero",
            ));
        }
        let mut remaining = count;
        let mut plan = Vec::new();
        for disk in self.list() {
            let blocks = disk.first_free_blocks(remaining);
            remaining -= blocks.len();
            if !blocks.is_empty() {
                plan.push(MemberDiskCommit {
                    disk: disk.uuid().clone(),
                    change: MemberDiskChange::Allocate(blocks),
                });
            }
            if remaining == 0 {
                return Ok(plan);
            }
        }
        Err(MemberDiskError::NoSpace)
    }

    pub fn release_plan(
        &self,
        blocks: Vec<AllocatedBlock>,
    ) -> Result<Vec<MemberDiskCommit>, MemberDiskError> {
        let mut grouped: BTreeMap<DiskUuid, Vec<BlockId>> = BTreeMap::new();
        for block in blocks {
            let group = grouped.entry(block.disk).or_default();
            if group.contains(&block.block) {
                return Err(MemberDiskError::InvalidRequest("duplicate released block"));
            }
            group.push(block.block);
        }
        let plan: Vec<_> = grouped
            .into_iter()
            .map(|(disk, blocks)| MemberDiskCommit {
                disk,
                change: MemberDiskChange::Release(blocks),
            })
            .collect();
        for commit in &plan {
            self.get(&commit.disk)?.validate(&commit.change)?;
        }
        Ok(plan)
    }

    pub async fn commit(
        &self,
        disk: &DiskUuid,
        change: MemberDiskChange,
        cancel: &CancellationToken,
        mandatory: bool,
    ) -> Result<(), MemberDiskError> {
        self.get(disk)?.validate(&change)?;
        self.commit_batch(
            vec![MemberDiskCommit {
                disk: disk.clone(),
                change,
            }],
            Some(cancel),
            mandatory,
        )
        .await
        .pop()
        .unwrap()
    }

    async fn commit_batch(
        &self,
        commits: Vec<MemberDiskCommit>,
        cancel: Option<&CancellationToken>,
        mandatory: bool,
    ) -> Vec<Result<(), MemberDiskError>> {
        let mut results = vec![Ok(()); commits.len()];
        let mut pending: Vec<_> = (0..commits.len()).collect();
        let mut attempt = 0;
        while !pending.is_empty() {
            let reply = self
                .metadata
                .commit(pending.iter().map(|i| commits[*i].clone()).collect())
                .await;
            if reply.len() != pending.len() {
                for i in pending {
                    results[i] = Err(MemberDiskError::NeedsRecovery(
                        "invalid metadata result count".into(),
                    ));
                }
                break;
            }
            let mut retry = Vec::new();
            for (i, result) in pending.into_iter().zip(reply) {
                match result {
                    Ok(()) => {
                        let mut disks = self.disks.lock().unwrap();
                        let disk = disks
                            .members
                            .get_mut(&commits[i].disk)
                            .expect("validated member");
                        disk.apply_committed(commits[i].change.clone());
                        *disks.versions.entry(commits[i].disk.clone()).or_default() += 1;
                    }
                    Err(error)
                        if self.metadata.is_retryable(&error)
                            && (mandatory || attempt < self.config.max_retries) =>
                    {
                        retry.push(i)
                    }
                    Err(error) if self.metadata.is_retryable(&error) => {
                        results[i] = Err(MemberDiskError::Metadata(error.0))
                    }
                    Err(error) => results[i] = Err(MemberDiskError::NeedsRecovery(error.0)),
                }
            }
            pending = retry;
            if !pending.is_empty() {
                attempt += 1;
                if !mandatory && let Some(cancel) = cancel {
                    if let Err(error) = wait_retry(cancel, self.config.retry_delay).await {
                        for i in pending {
                            results[i] = Err(error.clone());
                        }
                        break;
                    }
                } else {
                    tokio::time::sleep(self.config.retry_delay).await;
                }
            }
        }
        results
    }
}

struct Catalog {
    members: BTreeMap<DiskUuid, MemberDisk>,
    versions: BTreeMap<DiskUuid, u64>,
}

impl DiskEffects {
    pub fn version(&self, disk: &DiskUuid) -> u64 {
        *self.disks.lock().unwrap().versions.get(disk).unwrap_or(&0)
    }
}

async fn wait_retry(cancel: &CancellationToken, delay: Duration) -> Result<(), MemberDiskError> {
    tokio::select! { biased; _ = cancel.cancelled() => Err(MemberDiskError::Cancelled), _ = tokio::time::sleep(delay) => Ok(()) }
}
