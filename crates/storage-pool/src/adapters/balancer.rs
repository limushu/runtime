//! Deterministic balancing simulator. It models committed remaps, not physical data copy.
use super::*;
use crate::domains::balance::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::{Notify, watch};

pub struct SimulatedBalancer {
    metadata: Arc<dyn MetadataStore>,
    delay: Duration,
    jobs: Mutex<Jobs>,
}
type Jobs = BTreeMap<(PoolId, EvacuationId), (DiskId, Arc<Job>)>;
impl SimulatedBalancer {
    pub fn new(metadata: Arc<dyn MetadataStore>, delay: Duration) -> Self {
        Self {
            metadata,
            delay,
            jobs: Mutex::default(),
        }
    }
    pub fn jobs_started(&self) -> usize {
        self.jobs.lock().unwrap().len()
    }
}

fn plan(snapshot: &MetadataSnapshot, source: &DiskId) -> Outcome<Vec<MetadataChange>> {
    let mut disks: BTreeMap<_, _> = snapshot
        .disks
        .iter()
        .cloned()
        .map(|d| (d.id.clone(), d))
        .collect();
    let from = disks
        .get(source)
        .ok_or_else(|| reject("unknown shrink disk"))?;
    let referenced: BTreeSet<_> = snapshot
        .volumes
        .iter()
        .flat_map(|v| &v.groups)
        .flat_map(|g| &g.entries)
        .filter(|e| &e.disk == source)
        .map(|e| e.blk)
        .collect();
    if referenced != from.allocated {
        return Err(reject("source allocations need reconciliation"));
    }
    if !referenced.is_empty() && from.io != IoState::Up {
        return Err(reject("source disk is not serving IO"));
    }
    let mut changes = Vec::new();
    for mut volume in snapshot.volumes.clone() {
        for bg in &mut volume.groups {
            let Some(slot) = bg.entries.iter().position(|e| &e.disk == source) else {
                continue;
            };
            if !bg.entries.iter().any(|e| e.data == DataState::Valid) {
                return Err(reject("block group has no valid copy"));
            }
            let from = disks[source].clone();
            let occupied: BTreeSet<_> = bg
                .entries
                .iter()
                .enumerate()
                .filter(|(i, _)| *i != slot)
                .filter_map(|(_, e)| disks.get(&e.disk).map(|d| d.node.clone()))
                .collect();
            let target = disks
                .values()
                .find(|d| {
                    &d.id != source
                        && d.allocatable()
                        && d.tier == from.tier
                        && !occupied.contains(&d.node)
                        && d.allocated.len() < d.capacity_blks as usize
                })
                .cloned()
                .ok_or_else(|| {
                    reject("insufficient healthy capacity or replica failure domains")
                })?;
            let blk = (0..target.capacity_blks)
                .find(|b| !target.allocated.contains(b))
                .unwrap();
            let entry = BgEntry {
                disk: target.id.clone(),
                blk,
                data: DataState::Valid,
            };
            changes.push(MetadataChange::Remap {
                volume: volume.id.clone(),
                expected_revision: volume.revision,
                group: bg.index,
                source: bg.entries[slot].clone(),
                target: entry.clone(),
                source_revision: from.revision,
                target_revision: target.revision,
            });
            let disk = disks.get_mut(source).unwrap();
            disk.allocated.remove(&bg.entries[slot].blk);
            disk.revision += 1;
            let disk = disks.get_mut(&target.id).unwrap();
            disk.allocated.insert(blk);
            disk.revision += 1;
            bg.entries[slot] = entry;
            volume.revision += 1;
        }
    }
    Ok(changes)
}
struct Job {
    progress: watch::Sender<EvacuationProgress>,
    failure: Mutex<Option<Failure>>,
    gate: tokio::sync::Mutex<()>,
    changed: Notify,
}
impl Job {
    fn state(&self, state: EvacuationState) {
        self.progress.send_modify(|p| p.state = state);
        self.changed.notify_one();
    }
    async fn run(
        self: Arc<Self>,
        pool: PoolId,
        disk: DiskId,
        metadata: Arc<dyn MetadataStore>,
        delay: Duration,
    ) {
        loop {
            let changed = self.changed.notified();
            let guard = self.gate.lock().await;
            let state = self.progress.borrow().state.clone();
            match state {
                EvacuationState::Paused => {
                    drop(guard);
                    changed.await;
                    continue;
                }
                EvacuationState::Running => {}
                _ => return,
            }
            let result: Outcome<bool> = async {
                let snapshot = metadata.snapshot(&pool).await?;
                let changes = plan(&snapshot, &disk)?;
                if let Some(change) = changes.into_iter().next() {
                    // The simulator skips physical copying; only a production balancing
                    // adapter may publish this after confirming a real data copy.
                    metadata.commit(&pool, change).await?;
                    self.progress.send_modify(|p| p.completed += 1);
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
            .await;
            match result {
                Ok(false) => {}
                Ok(true) => {
                    self.state(EvacuationState::Completed);
                    return;
                }
                Err(error) => {
                    *self.failure.lock().unwrap() = Some(error.clone());
                    self.state(EvacuationState::Failed(error.to_string()));
                    return;
                }
            }
            drop(guard);
            tokio::time::sleep(delay).await;
        }
    }
}
#[async_trait]
impl EvacuationJob for Job {
    fn progress(&self) -> watch::Receiver<EvacuationProgress> {
        self.progress.subscribe()
    }
    async fn wait(&self) -> Outcome<()> {
        let mut progress = self.progress.subscribe();
        loop {
            match progress.borrow_and_update().state.clone() {
                EvacuationState::Completed => return Ok(()),
                EvacuationState::Cancelled => return Err(Failure::Cancelled),
                EvacuationState::Failed(message) => {
                    return Err(self
                        .failure
                        .lock()
                        .unwrap()
                        .clone()
                        .unwrap_or(Failure::Failed(message)));
                }
                _ => {}
            }
            progress.changed().await.map_err(|_| Failure::Unavailable)?;
        }
    }
    async fn pause(&self) -> Outcome<()> {
        let _guard = self.gate.lock().await;
        if self.progress.borrow().state == EvacuationState::Running {
            self.state(EvacuationState::Paused);
        }
        Ok(())
    }
    async fn resume(&self) -> Outcome<()> {
        let _guard = self.gate.lock().await;
        if self.progress.borrow().state == EvacuationState::Paused {
            self.state(EvacuationState::Running);
        }
        Ok(())
    }
    async fn cancel(&self) -> Outcome<()> {
        let _guard = self.gate.lock().await;
        if matches!(
            self.progress.borrow().state,
            EvacuationState::Running | EvacuationState::Paused
        ) {
            self.state(EvacuationState::Cancelled);
        }
        Ok(())
    }
}
#[async_trait]
impl Balancer for SimulatedBalancer {
    async fn find(&self, pool: &PoolId, id: EvacuationId) -> Outcome<Option<ArcJob>> {
        Ok(self
            .jobs
            .lock()
            .unwrap()
            .get(&(pool.clone(), id))
            .map(|(_, job)| job.clone() as ArcJob))
    }
    async fn check_shrink(&self, pool: &PoolId, disk: &DiskId) -> Outcome<ShrinkReadiness> {
        let snapshot = self.metadata.snapshot(pool).await?;
        Ok(match plan(&snapshot, disk) {
            Ok(_) => ShrinkReadiness::Ready,
            Err(error) => ShrinkReadiness::Wait(error.to_string()),
        })
    }
    async fn evacuate(&self, pool: &PoolId, disk: &DiskId, id: EvacuationId) -> Outcome<ArcJob> {
        let snapshot = self.metadata.snapshot(pool).await?;
        let source = snapshot
            .disks
            .iter()
            .find(|d| &d.id == disk)
            .ok_or_else(|| reject("unknown source"))?;
        if source.shrink != Some(id) || source.allocation != AllocationState::Inactive {
            return Err(reject(
                "evacuation requires committed intent and disabled allocation",
            ));
        }
        let mut jobs = self.jobs.lock().unwrap();
        if let Some((original, job)) = jobs.get(&(pool.clone(), id)) {
            if original != disk {
                return Err(reject("job ID belongs to another disk"));
            }
            return Ok(job.clone());
        }
        let total = plan(&snapshot, disk)?.len() as u64;
        let (progress, _) = watch::channel(EvacuationProgress {
            disk: disk.clone(),
            completed: 0,
            total: Some(total),
            state: EvacuationState::Running,
        });
        let job = Arc::new(Job {
            progress,
            failure: Mutex::default(),
            gate: tokio::sync::Mutex::new(()),
            changed: Notify::new(),
        });
        jobs.insert((pool.clone(), id), (disk.clone(), job.clone()));
        tokio::spawn(job.clone().run(
            pool.clone(),
            disk.clone(),
            self.metadata.clone(),
            self.delay,
        ));
        Ok(job)
    }
}
