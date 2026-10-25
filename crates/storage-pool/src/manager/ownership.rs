use super::*;
use std::{collections::BTreeSet, sync::Weak};

enum RemovalKind {
    Node(NodeId),
    Tier(TierId),
    Disk(DiskId),
}
/// Releases a deletion gate if admission is rejected or a queued task is cancelled.
/// Once an external effect may have started it stays closed until an explicit result.
pub(crate) struct RemovalGuard {
    shared: Arc<PoolShared>,
    kind: RemovalKind,
    retain: bool,
}
impl RemovalGuard {
    pub fn node(shared: Arc<PoolShared>, id: NodeId) -> Self {
        Self {
            shared,
            kind: RemovalKind::Node(id),
            retain: false,
        }
    }
    pub fn tier(shared: Arc<PoolShared>, id: TierId) -> Self {
        Self {
            shared,
            kind: RemovalKind::Tier(id),
            retain: false,
        }
    }
    pub fn disk(shared: Arc<PoolShared>, id: DiskId) -> Self {
        Self {
            shared,
            kind: RemovalKind::Disk(id),
            retain: false,
        }
    }
    pub fn begin(&mut self) {
        self.retain = true;
    }
    pub fn settled<T>(&mut self, result: &Outcome<T>) {
        self.retain = result.is_ok() || matches!(result, Err(Failure::Uncertain(_)));
    }
}
impl Drop for RemovalGuard {
    fn drop(&mut self) {
        if !self.retain {
            match &self.kind {
                RemovalKind::Node(id) => {
                    self.shared.detaching.lock().unwrap().remove(id);
                }
                RemovalKind::Tier(id) => {
                    self.shared.retiring_tiers.lock().unwrap().remove(id);
                }
                RemovalKind::Disk(id) => {
                    self.shared.retiring_disks.lock().unwrap().remove(id);
                }
            }
        }
    }
}

#[derive(Default)]
pub(crate) struct NodeClaim {
    pub count: usize,
    pub confirmed: bool,
    pub uncertain: bool,
}
pub(crate) struct NodeReservation {
    shared: Arc<PoolShared>,
    id: NodeId,
    effect_pending: bool,
}
impl NodeReservation {
    pub fn begin(&mut self) {
        self.effect_pending = true;
    }
    pub fn settled<T>(&mut self, result: &Outcome<T>) {
        self.effect_pending = false;
        if let Some(claim) = self.shared.node_claims.lock().unwrap().get_mut(&self.id) {
            claim.confirmed |= result.is_ok();
            claim.uncertain |= matches!(result, Err(Failure::Uncertain(_)));
        }
    }
}
impl Drop for NodeReservation {
    fn drop(&mut self) {
        let mut claims = self.shared.node_claims.lock().unwrap();
        if let Some(claim) = claims.get_mut(&self.id) {
            claim.count -= 1;
            claim.uncertain |= self.effect_pending;
            if claim.count == 0 && !claim.confirmed && !claim.uncertain {
                claims.remove(&self.id);
                self.shared.nodes.lock().unwrap().remove(&self.id);
            }
        }
    }
}

pub(crate) struct DiskOwnership {
    pub owner: PoolUuid,
    pub claims: usize,
    pub confirmed: bool,
    pub uncertain: bool,
}
/// A not-yet-admitted call owns its reservation too. Known pre-Add failures release only
/// the last claim; a committed membership or ambiguous Add keeps global ownership.
pub(crate) struct DiskReservation {
    pub disk: DiskId,
    owner: PoolUuid,
    registry: Weak<Mutex<Registry>>,
    routing: Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
    effect_pending: bool,
}
impl DiskReservation {
    pub fn begin(&mut self) {
        self.effect_pending = true;
    }
    pub fn settled<T>(&mut self, result: &Outcome<T>) {
        self.effect_pending = false;
        if let Some(registry) = self.registry.upgrade() {
            let mut registry = registry.lock().unwrap();
            if let Some(entry) = registry
                .disks
                .get_mut(&self.disk)
                .filter(|entry| entry.owner == self.owner)
            {
                entry.confirmed |= result.is_ok();
                entry.uncertain |= matches!(result, Err(Failure::Uncertain(_)));
            }
        }
    }
}
impl Drop for DiskReservation {
    fn drop(&mut self) {
        if let Some(registry) = self.registry.upgrade() {
            let mut registry = registry.lock().unwrap();
            if let Some(entry) = registry
                .disks
                .get_mut(&self.disk)
                .filter(|entry| entry.owner == self.owner)
            {
                entry.claims -= 1;
                entry.uncertain |= self.effect_pending;
                if entry.claims == 0 && !entry.confirmed && !entry.uncertain {
                    registry.disks.remove(&self.disk);
                    self.routing.lock().unwrap().remove(&self.disk);
                }
            }
        }
    }
}
impl PoolHandle {
    pub(crate) fn reserve_node(&self, id: &NodeId) -> Outcome<NodeReservation> {
        self.descriptor().active()?;
        let mut claims = self.shared.node_claims.lock().unwrap();
        if self.shared.detaching.lock().unwrap().contains(id) {
            return Err(reject("node membership is retired or detaching"));
        }
        claims.entry(id.clone()).or_default().count += 1;
        self.shared.nodes.lock().unwrap().insert(id.clone());
        Ok(NodeReservation {
            shared: self.shared.clone(),
            id: id.clone(),
            effect_pending: false,
        })
    }
    pub(crate) fn reserve_disk(&self, spec: &DiskSpec) -> Outcome<DiskReservation> {
        if spec.capacity_blks == 0 {
            return Err(reject("disk capacity must be positive"));
        }
        let registry = self.shared.registry.upgrade().ok_or(Failure::Unavailable)?;
        let mut registry = registry.lock().unwrap();
        if registry.stopped {
            return Err(Failure::Unavailable);
        }
        let descriptor = self.shared.descriptor.lock().unwrap();
        descriptor.active()?;
        if self.shared.detaching.lock().unwrap().contains(&spec.node)
            || self
                .shared
                .retiring_tiers
                .lock()
                .unwrap()
                .contains(&spec.tier)
            || self
                .shared
                .retiring_disks
                .lock()
                .unwrap()
                .contains(&spec.id)
        {
            return Err(reject("disk, tier, or node membership is being removed"));
        }
        if registry
            .disks
            .get(&spec.id)
            .is_some_and(|entry| entry.owner != descriptor.uuid)
        {
            return Err(reject("physical disk belongs to another pool"));
        }
        self.task_center.register_disk(spec)?;
        let entry = registry
            .disks
            .entry(spec.id.clone())
            .or_insert(DiskOwnership {
                owner: descriptor.uuid,
                claims: 0,
                confirmed: false,
                uncertain: false,
            });
        entry.claims += 1;
        Ok(DiskReservation {
            disk: spec.id.clone(),
            owner: descriptor.uuid,
            registry: self.shared.registry.clone(),
            routing: self.task_center.disk_directory(),
            effect_pending: false,
        })
    }
}

/// Membership indexes belong to Pool, not to Disk metadata or a native adapter.
pub(crate) struct DiskRemoval {
    shared: Arc<PoolShared>,
    routing: Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
    disks: Vec<DiskId>,
    guards: Vec<RemovalGuard>,
}
impl DiskRemoval {
    pub fn begin(&mut self) {
        for gate in &mut self.guards {
            gate.begin();
        }
    }
    pub fn settled(&mut self, result: &Outcome<()>) {
        for gate in &mut self.guards {
            gate.settled(result);
        }
    }
    pub fn complete(&self) {
        let owner = self.shared.descriptor.lock().unwrap().uuid;
        if let Some(registry) = self.shared.registry.upgrade() {
            let mut registry = registry.lock().unwrap();
            for id in &self.disks {
                if registry
                    .disks
                    .get(id)
                    .is_some_and(|entry| entry.owner == owner)
                {
                    registry.disks.remove(id);
                }
            }
        }
        let mut routing = self.routing.lock().unwrap();
        for id in &self.disks {
            routing.remove(id);
        }
    }
}
impl PoolHandle {
    pub(crate) fn reserve_removal(&self, disks: &[DiskId]) -> Outcome<DiskRemoval> {
        let registry = self.shared.registry.upgrade().ok_or(Failure::Unavailable)?;
        let _registry = registry.lock().unwrap();
        self.descriptor().active()?;
        let mut retiring = self.shared.retiring_disks.lock().unwrap();
        for id in disks {
            if self.task_center.disk_spec(id).is_none() {
                return Err(reject("unknown disk membership"));
            }
            if retiring.contains(id) {
                return Err(reject("disk removal already requested"));
            }
        }
        retiring.extend(disks.iter().cloned());
        Ok(DiskRemoval {
            shared: self.shared.clone(),
            routing: self.task_center.disk_directory(),
            disks: disks.to_vec(),
            guards: disks
                .iter()
                .map(|id| RemovalGuard::disk(self.shared.clone(), id.clone()))
                .collect(),
        })
    }
}

/// Local Pool indexes needed by a reconstructed shrink execution.
#[derive(Clone)]
pub(crate) struct RemovalRecovery {
    pub shared: Arc<PoolShared>,
    pub routing: Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
    pub retired: BTreeSet<DiskId>,
}
impl RemovalRecovery {
    pub fn reserve(&self, disks: &[DiskId]) -> Outcome<DiskRemoval> {
        let routing = self.routing.lock().unwrap();
        let mut retiring = self.shared.retiring_disks.lock().unwrap();
        for disk in disks {
            if (!routing.contains_key(disk) && !self.retired.contains(disk))
                || retiring.contains(disk)
            {
                return Err(reject("invalid or duplicate recovered disk removal"));
            }
        }
        retiring.extend(disks.iter().cloned());
        Ok(DiskRemoval {
            shared: self.shared.clone(),
            routing: self.routing.clone(),
            disks: disks.to_vec(),
            guards: disks
                .iter()
                .map(|id| RemovalGuard::disk(self.shared.clone(), id.clone()))
                .collect(),
        })
    }
}
