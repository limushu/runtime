//! Application activation after election/fencing. No election algorithm lives here.
use super::*;
use crate::domain_clients::DomainSnapshot;
use std::collections::BTreeSet;
use workflow_foundation::{Dependencies, persistence::TaskStore};

/// Pool catalogue and current, externally confirmed topology for the new leader.
/// Disk/VD state is read from MetadataStore, never inferred from task checkpoints.
#[derive(Debug, Clone)]
pub struct PoolRecovery {
    pub descriptor: PoolDescriptor,
    pub nodes: Vec<PoolNode>,
    pub tiers: Vec<Tier>,
}
impl PoolRecovery {
    /// Bootstrap an empty pool through the same durable activation path.
    pub fn empty(request: CreatePool) -> Self {
        Self {
            descriptor: request.descriptor(),
            nodes: vec![],
            tiers: vec![],
        }
    }
    fn validate(&self, state: &DomainSnapshot) -> Outcome<()> {
        self.descriptor.active()?;
        let pool = &self.descriptor.id;
        let mut nodes = BTreeSet::new();
        for node in &self.nodes {
            if &node.pool != pool || (node.loaded && !node.connected) || !nodes.insert(&node.id) {
                return Err(reject("invalid recovery node snapshot"));
            }
        }
        let mut tiers = BTreeSet::new();
        for tier in &self.tiers {
            if &tier.pool != pool || !tiers.insert(&tier.id) {
                return Err(reject("invalid recovery tier snapshot"));
            }
        }
        let mut disks = BTreeSet::new();
        for disk in &state.disks.disks {
            if &disk.pool != pool
                || !nodes.contains(&disk.node)
                || !tiers.contains(&disk.tier)
                || disk.capacity_blks == 0
                || !disks.insert(&disk.id)
            {
                return Err(reject("disk metadata differs from recovery membership"));
            }
        }
        let mut volumes = BTreeSet::new();
        for volume in &state.volumes.volumes {
            if &volume.pool != pool
                || !volumes.insert(&volume.id)
                || volume
                    .groups
                    .iter()
                    .flat_map(|g| &g.entries)
                    .any(|e| !disks.contains(&e.disk))
            {
                return Err(reject("invalid recovery volume snapshot"));
            }
        }
        Ok(())
    }
}

// Reserve catalogue identities across awaits, without holding a std mutex over IO.
// Dropping/cancelling activation removes the reservation and exposes no Pool handle.
struct Activation {
    registry: Arc<Mutex<Registry>>,
    id: PoolId,
}
impl Drop for Activation {
    fn drop(&mut self) {
        self.registry.lock().unwrap().activating.remove(&self.id);
    }
}

impl PoolManager {
    /// Inject the real TaskStore adapter before activating any durable pools.
    pub fn with_task_store(mut self, store: Arc<dyn TaskStore>) -> Self {
        self.task_store = Some(store);
        self
    }

    /// Call only after the election authority has granted a newer epoch and fenced
    /// the old leader's external effects. Supply a complete current catalogue/topology.
    /// The returned handle is published only after domain and Task recovery succeed.
    pub async fn activate(&self, recovery: PoolRecovery) -> Outcome<PoolHandle> {
        let store = self
            .task_store
            .clone()
            .ok_or_else(|| reject("TaskStore is not configured"))?;
        if self.task_ids.epoch() == 0 || !self.task_ids.active() {
            return Err(reject("activation requires an active elected leader epoch"));
        }
        let descriptor = recovery.descriptor.clone();
        let _activation = {
            let mut registry = self.registry.lock().unwrap();
            if registry.stopped {
                return Err(Failure::Unavailable);
            }
            if registry.pools.contains_key(&descriptor.id)
                || registry.retired.contains_key(&descriptor.id)
                || registry.uuids.contains_key(&descriptor.uuid)
                || registry.names.contains_key(&descriptor.name)
                || registry.activating.values().any(|p| {
                    p.id == descriptor.id || p.uuid == descriptor.uuid || p.name == descriptor.name
                })
            {
                return Err(reject("pool identity already active or activating"));
            }
            registry
                .activating
                .insert(descriptor.id.clone(), descriptor.clone());
            Activation {
                registry: self.registry.clone(),
                id: descriptor.id.clone(),
            }
        };
        let snapshot = DomainSnapshot {
            nodes: recovery.nodes.clone(),
            tiers: recovery.tiers.clone(),
            disks: self
                .metadata
                .disks(&descriptor.id)
                .await?
                .ok_or_else(|| reject("activation requires authoritative disk metadata"))?,
            volumes: self
                .metadata
                .volumes(&descriptor.id)
                .await?
                .ok_or_else(|| reject("activation requires authoritative VD metadata"))?,
        };
        recovery.validate(&snapshot)?;
        let disk_ids: Vec<_> = snapshot.disks.disks.iter().map(|d| d.id.clone()).collect();
        let retired = snapshot.disks.retired.iter().cloned().collect();
        let mut pool = Pool::prepare(descriptor.clone(), self, Some(snapshot))?;
        pool.driver.define::<ShrinkWorkflow>(
            Dependencies::default()
                .with(pool.domains.clone())
                .with(RemovalRecovery {
                    shared: pool.shared.clone(),
                    routing: pool.routing.clone(),
                    retired,
                }),
        )?;
        pool.driver.enable_persistence(store).await?;
        let restored = pool.driver.restore().await?;

        // No await after this point. Publish membership before a restored workflow
        // can acquire this registry lock to complete removal or accept a new request.
        let handle = {
            let mut registry = self.registry.lock().unwrap();
            if registry.stopped || !self.task_ids.active() {
                return Err(Failure::Unavailable);
            }
            // Other catalogue operations (for example rename) may have run during IO.
            if registry.pools.contains_key(&descriptor.id)
                || registry.retired.contains_key(&descriptor.id)
                || registry.uuids.contains_key(&descriptor.uuid)
                || registry.names.contains_key(&descriptor.name)
            {
                return Err(reject("pool identity changed during activation"));
            }
            if disk_ids.iter().any(|id| registry.disks.contains_key(id)) {
                return Err(reject("recovered physical disk belongs to another pool"));
            }
            let pool = pool.finish(Some(restored))?;
            let handle = pool.handle();
            for id in disk_ids {
                registry.disks.insert(
                    id,
                    ownership::DiskOwnership {
                        owner: descriptor.uuid,
                        claims: 0,
                        confirmed: true,
                        uncertain: false,
                    },
                );
            }
            registry
                .uuids
                .insert(descriptor.uuid, descriptor.id.clone());
            registry
                .names
                .insert(descriptor.name, descriptor.id.clone());
            registry.pools.insert(descriptor.id, pool);
            handle
        };
        Ok(handle)
    }

    /// Revoke this process's epoch and wait for actual activity settlement.
    /// Durable tasks remain recoverable; this is not a user's task cancellation.
    /// A new manager with a newer elected epoch performs the next activation.
    pub async fn demote(&self) -> Outcome<()> {
        self.task_ids.revoke();
        let pools: Vec<_> = {
            let mut registry = self.registry.lock().unwrap();
            registry.stopped = true;
            registry.pools.values().map(Pool::handle).collect()
        };
        // Leadership loss must stop hosts even during load/delete transitions,
        // rather than being rejected by the user-facing pool lifecycle guards.
        for pool in &pools {
            pool.task_center.close_admission();
            pool.task_center.stop();
        }
        let mut closing: FuturesUnordered<_> = pools
            .into_iter()
            .map(|pool| async move {
                let result = pool.task_center.shutdown().await;
                let mut descriptor = pool.shared.descriptor.lock().unwrap();
                if descriptor.lifecycle != PoolLifecycle::Deleted {
                    descriptor.lifecycle = if result.is_ok() {
                        PoolLifecycle::Unloaded
                    } else {
                        PoolLifecycle::ReconcileRequired
                    };
                }
                result
            })
            .collect();
        let mut error = None;
        while let Some(result) = closing.next().await {
            if let Err(failure) = result {
                error.get_or_insert(failure);
            }
        }
        error.map_or(Ok(()), Err)
    }
}

impl PoolHandle {
    /// Convenience for a controlled handoff/demo. This is not a persistent catalogue
    /// or an atomic live snapshot: quiesce topology first. After a crash, obtain the
    /// catalogue and fresh observations from the external metadata/topology authority.
    pub async fn recovery_snapshot(&self) -> Outcome<PoolRecovery> {
        Ok(PoolRecovery {
            descriptor: self.descriptor(),
            nodes: self.nodes().await?,
            tiers: self.tiers().await?,
        })
    }
}
