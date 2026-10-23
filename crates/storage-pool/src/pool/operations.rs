use super::*;
use crate::manager::EventDecision;
use crate::{support::reject, workflows::*, *};
use std::sync::atomic::Ordering;
use tokio::sync::watch;

impl PoolHandle {
    pub async fn tiers(&self) -> Outcome<Vec<Tier>> {
        Ok(self.metadata.snapshot(&self.id).await?.tiers)
    }
    pub async fn create_tier(&self, id: TierId) -> Outcome<TaskRun<Tier>> {
        self.task_center
            .submit(CreateTierWorkflow {
                domains: self.domains.clone(),
                id,
            })
            .await
    }
    pub async fn delete_tier(&self, id: TierId) -> Outcome<TaskRun<()>> {
        {
            let registry = self.shared.registry.upgrade().ok_or(Failure::Unavailable)?;
            let _guard = registry.lock().unwrap();
            self.descriptor().active()?;
            if self
                .shared
                .disk_directory()
                .lock()
                .unwrap()
                .values()
                .any(|spec| spec.tier == id)
            {
                return Err(reject("tier has disk memberships or reservations"));
            }
            if !self
                .shared
                .retiring_tiers
                .lock()
                .unwrap()
                .insert(id.clone())
            {
                return Err(reject("tier deletion already requested"));
            }
        }
        self.task_center
            .submit(DeleteTierWorkflow {
                domains: self.domains.clone(),
                id: id.clone(),
                removal: RemovalGuard::tier(self.shared.clone(), id.clone()),
            })
            .await
    }
    pub async fn attach_node(
        &self,
        id: NodeId,
        sequence: u64,
        connected: bool,
    ) -> Outcome<TaskRun<PoolNode>> {
        let reservation = self.reserve_node(&id)?;
        self.shared
            .serial
            .fetch_max(sequence.saturating_add(1), Ordering::SeqCst);
        self.task_center
            .submit(AttachNodeWorkflow {
                domains: self.domains.clone(),
                id,
                sequence,
                connected,
                reservation,
            })
            .await
    }
    pub async fn detach_node(&self, id: NodeId) -> Outcome<TaskRun<()>> {
        {
            let registry = self.shared.registry.upgrade().ok_or(Failure::Unavailable)?;
            let _guard = registry.lock().unwrap();
            self.descriptor().active()?;
            if !self.shared.node_disks(&id).is_empty() {
                return Err(reject("node still has disks or reservations"));
            }
            if !self.shared.detaching.lock().unwrap().insert(id.clone()) {
                return Err(reject("node detach already requested"));
            }
        }
        self.task_center
            .submit(DetachNodeWorkflow {
                domains: self.domains.clone(),
                id: id.clone(),
                removal: RemovalGuard::node(self.shared.clone(), id.clone()),
            })
            .await
    }
    pub async fn remove_disk(&self, id: DiskId) -> Outcome<TaskRun<()>> {
        let spec = {
            let registry = self.shared.registry.upgrade().ok_or(Failure::Unavailable)?;
            let _guard = registry.lock().unwrap();
            self.descriptor().active()?;
            let spec = self
                .shared
                .disk_spec(&id)
                .ok_or_else(|| reject("unknown disk membership"))?;
            if !self
                .shared
                .retiring_disks
                .lock()
                .unwrap()
                .insert(id.clone())
            {
                return Err(reject("disk removal already requested"));
            }
            spec
        };
        self.task_center
            .submit(RemoveDiskWorkflow {
                domains: self.domains.clone(),
                spec,
                removal: RemovalGuard::disk(self.shared.clone(), id),
            })
            .await
    }
    pub async fn delete_volume(&self, id: VolumeId) -> Outcome<TaskRun<()>> {
        let volume = self.metadata.snapshot(&self.id).await?.volume(&id)?;
        let nodes = volume
            .groups
            .iter()
            .flat_map(|group| &group.entries)
            .filter_map(|entry| self.shared.disk_spec(&entry.disk).map(|spec| spec.node))
            .collect();
        self.task_center
            .submit(DeleteVolumeWorkflow {
                domains: self.domains.clone(),
                volume,
                nodes,
            })
            .await
    }
    /// The finalizer is owned by Pool, so dropping this wait does not abandon deletion.
    pub async fn delete(&self, expected_revision: u64) -> Outcome<()> {
        let registry = self.shared.registry.upgrade().ok_or(Failure::Unavailable)?;
        let previous = {
            let mut descriptor = self.shared.descriptor.lock().unwrap();
            if !matches!(
                descriptor.lifecycle,
                PoolLifecycle::Active | PoolLifecycle::Unloaded
            ) || self.task_center.stopped()
            {
                return Err(reject("pool cannot be deleted in its current lifecycle"));
            }
            if descriptor.revision != expected_revision {
                return Err(reject("stale pool revision"));
            }
            let previous = descriptor.lifecycle;
            descriptor.lifecycle = PoolLifecycle::Deleting;
            self.task_center.close_admission();
            previous
        };
        let pool = self.clone();
        tokio::spawn(async move {
            let result = async {
                let mut run = pool
                    .task_center
                    .submit_admin(DeletePoolWorkflow {
                        client: pool.admin.clone(),
                    })
                    .await?;
                run.result().await?;
                pool.task_center.shutdown().await
            }
            .await;
            match &result {
                Ok(()) => {
                    let mut registry = registry.lock().unwrap();
                    let mut descriptor = pool.shared.descriptor.lock().unwrap();
                    descriptor.lifecycle = PoolLifecycle::Deleted;
                    descriptor.revision += 1;
                    registry.names.remove(&descriptor.name);
                    registry
                        .retired
                        .insert(descriptor.id.clone(), descriptor.clone());
                    if let Some(owner) = registry.pools.remove(&descriptor.id) {
                        registry.history.push_back(owner);
                    }
                    while registry.history.len() > registry.history_limit {
                        registry.history.pop_front();
                    }
                }
                Err(Failure::Uncertain(_)) => {
                    pool.shared.descriptor.lock().unwrap().lifecycle =
                        PoolLifecycle::ReconcileRequired
                }
                Err(_) => {
                    pool.shared.descriptor.lock().unwrap().lifecycle = previous;
                    if previous == PoolLifecycle::Active {
                        pool.task_center.open_admission();
                    }
                }
            }
            result
        })
        .await
        .unwrap_or_else(|_| {
            Err(Failure::Uncertain(
                "pool deletion finalizer terminated".into(),
            ))
        })
    }
    pub fn descriptor(&self) -> PoolDescriptor {
        self.shared.descriptor.lock().unwrap().clone()
    }
    pub fn uuid(&self) -> PoolUuid {
        self.descriptor().uuid
    }
    pub fn instance(&self) -> PoolInstanceId {
        self.shared.instance
    }
    pub fn task_ref(&self, task: TaskId) -> TaskRef {
        TaskRef {
            pool: self.uuid(),
            task,
        }
    }
    pub fn task_by_ref(&self, task: TaskRef) -> Outcome<TaskSnapshot> {
        self.check_task_ref(task)?;
        self.task(task.task)
    }
    pub fn observe_task_by_ref(&self, task: TaskRef) -> Outcome<TaskObserver> {
        self.check_task_ref(task)?;
        self.observe_task(task.task)
    }
    pub(crate) fn check_task_ref(&self, task: TaskRef) -> Outcome<()> {
        if task.pool != self.uuid() {
            Err(reject("stale or wrong pool task reference"))
        } else {
            Ok(())
        }
    }
    pub async fn rename(
        &self,
        name: PoolName,
        expected_revision: u64,
    ) -> Outcome<TaskRun<PoolDescriptor>> {
        self.task_center
            .submit(RenamePoolWorkflow {
                client: self.admin.clone(),
                name,
                revision: expected_revision,
            })
            .await
    }
    pub async fn shutdown(&self) -> Outcome<()> {
        {
            let mut descriptor = self.shared.descriptor.lock().unwrap();
            if descriptor.lifecycle == PoolLifecycle::Deleted {
                return Ok(());
            }
            match descriptor.lifecycle {
                PoolLifecycle::Active => descriptor.lifecycle = PoolLifecycle::Unloading,
                PoolLifecycle::Unloaded | PoolLifecycle::Unloading => {}
                _ => return Err(reject("pool lifecycle operation is in progress")),
            }
            self.task_center.close_admission();
        }
        let result = self.task_center.shutdown().await;
        self.shared.descriptor.lock().unwrap().lifecycle = if result.is_ok() {
            PoolLifecycle::Unloaded
        } else {
            PoolLifecycle::ReconcileRequired
        };
        result
    }
    pub async fn add_disk(&self, spec: DiskSpec) -> Outcome<TaskRun<MemberDisk>> {
        let reservation = self.reserve_disk(&spec)?;
        self.task_center
            .submit(OnlineDiskWorkflow {
                domains: self.domains.clone(),
                spec,
                reservation: Some(reservation),
            })
            .await
    }
    pub fn try_notify(&self, event: Arc<TopologyEvent>) -> PoolEventReceipt {
        let descriptor = self.descriptor();
        let immediate = |decision| {
            PoolEventReceipt::immediate(descriptor.uuid, self.instance(), event.id, decision)
        };
        if descriptor.lifecycle != PoolLifecycle::Active || !self.task_center.accepts() {
            return immediate(EventDecision::Rejected(Failure::Unavailable));
        }
        let relevant = match &event.change {
            TopologyChange::NodeConnectivity { node, .. } => {
                if self
                    .shared
                    .node_claims
                    .lock()
                    .unwrap()
                    .get(node)
                    .is_some_and(|claim| !claim.confirmed)
                {
                    return immediate(EventDecision::Rejected(Failure::Unavailable));
                }
                self.shared.nodes.lock().unwrap().contains(node)
            }
            TopologyChange::DiskAvailability { disk, .. } => {
                if let Some(registry) = self.shared.registry.upgrade()
                    && registry
                        .lock()
                        .unwrap()
                        .disks
                        .get(disk)
                        .is_some_and(|entry| entry.owner == descriptor.uuid && !entry.confirmed)
                {
                    return immediate(EventDecision::Rejected(Failure::Unavailable));
                }
                self.shared.has_reservation(disk)
            }
        };
        let mut cache = self.shared.events.lock().unwrap();
        if let Some((previous, receipt)) = cache.records.get(&event.id) {
            if **previous != *event {
                return immediate(EventDecision::Rejected(reject(
                    "event id reused with different content",
                )));
            }
            if receipt.snapshot().status != TargetStatus::Backpressured {
                return receipt.clone();
            }
        }
        if let Some((previous, receipt)) = cache.versions.get(&event.key()) {
            if event.version() < previous.version() {
                return immediate(EventDecision::Rejected(reject("stale topology event")));
            }
            if event.version() == previous.version() {
                if event.change != previous.change {
                    return immediate(EventDecision::Rejected(reject(
                        "conflicting topology version",
                    )));
                }
                if receipt.snapshot().status != TargetStatus::Backpressured {
                    return receipt.clone();
                }
            }
        }
        while cache.records.len() >= self.shared.event_limit {
            let removable = cache.order.iter().position(|id| {
                cache.records.get(id).is_some_and(|(_, receipt)| {
                    !matches!(
                        receipt.snapshot().status,
                        TargetStatus::Queued | TargetStatus::Accepted(_)
                    )
                })
            });
            let Some(position) = removable else {
                return immediate(EventDecision::Rejected(Failure::Busy));
            };
            if let Some(id) = cache.order.remove(position) {
                cache.records.remove(&id);
            }
        }
        let (decision, receiver) = watch::channel(if relevant {
            EventDecision::Pending
        } else {
            EventDecision::NotRelevant
        });
        let receipt = PoolEventReceipt {
            pool: descriptor.uuid,
            event: event.id,
            instance: self.instance(),
            decision: receiver,
        };
        cache.order.retain(|id| *id != event.id);
        cache.order.push_back(event.id);
        cache
            .records
            .insert(event.id, (event.clone(), receipt.clone()));
        if !relevant {
            return receipt;
        }
        cache
            .versions
            .insert(event.key(), (event.clone(), receipt.clone()));
        let reply = decision.clone();
        let callback = move |result: Outcome<TaskRun<PoolNode>>| {
            reply.send_replace(match result {
                Ok(run) => EventDecision::Accepted(run.control),
                Err(error) => EventDecision::Rejected(error),
            });
        };
        let result = match &event.change {
            TopologyChange::NodeConnectivity { node, connected } => {
                let sequence = self
                    .shared
                    .serial
                    .fetch_add(1, Ordering::SeqCst)
                    .max(event.sequence);
                self.shared
                    .serial
                    .fetch_max(sequence.saturating_add(1), Ordering::SeqCst);
                self.task_center.try_submit(
                    ObserveNodeWorkflow {
                        domains: self.domains.clone(),
                        id: node.clone(),
                        sequence,
                        connected: *connected,
                    },
                    Some(event.id),
                    callback,
                )
            }
            TopologyChange::DiskAvailability { disk, online } => {
                match self.shared.disk_spec(disk) {
                    Some(spec) if *online => self.task_center.try_submit(
                        OnlineDiskWorkflow {
                            domains: self.domains.clone(),
                            spec,
                            reservation: None,
                        },
                        Some(event.id),
                        move |result| {
                            let result = result.map(|run| run.control);
                            reply_disk(&decision, result);
                        },
                    ),
                    Some(spec) => self.task_center.try_submit(
                        OfflineDiskWorkflow {
                            domains: self.domains.clone(),
                            id: disk.clone(),
                            node: spec.node,
                        },
                        Some(event.id),
                        move |result| reply_disk(&decision, result.map(|run| run.control)),
                    ),
                    None => Err(reject("unknown disk membership")),
                }
            }
        };
        if let Err(error) = result {
            // A rejected mailbox offer has no task. The cached reply remains retryable for Busy.
            let (sender, receiver) = watch::channel(EventDecision::Rejected(error));
            drop(sender);
            let rejected = PoolEventReceipt {
                decision: receiver,
                ..receipt.clone()
            };
            cache
                .records
                .insert(event.id, (event.clone(), rejected.clone()));
            cache
                .versions
                .insert(event.key(), (event, rejected.clone()));
            return rejected;
        }
        receipt
    }
}
fn reply_disk(sender: &watch::Sender<EventDecision>, result: Outcome<TaskControl>) {
    sender.send_replace(match result {
        Ok(control) => EventDecision::Accepted(control),
        Err(error) => EventDecision::Rejected(error),
    });
}

impl PoolHandle {
    /// Call after acquiring Pool ownership and before recover_tasks. Connectivity/load
    /// observations still come from the real topology source; this never invents them.
    pub async fn restore_metadata(&self) -> Outcome<()> {
        self.descriptor().active()?;
        let mut run = self
            .task_center
            .submit_admin(RestoreMetadataWorkflow {
                domains: self.domains.clone(),
            })
            .await?;
        run.result().await
    }
    pub async fn disk(&self, id: DiskId) -> Outcome<MemberDisk> {
        self.metadata.snapshot(&self.id).await?.disk(&id)
    }
}
