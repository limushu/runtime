use crate::{
    manager::{PoolEventReceipt, Registry},
    *,
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex, Weak, atomic::AtomicU64},
};
pub(crate) struct PoolShared {
    pub takeover: tokio::sync::Mutex<bool>,
    pub recovery_report: Mutex<Option<crate::task_center::RecoveryReport>>,
    pub routing: Arc<Mutex<BTreeMap<DiskId, DiskSpec>>>,
    pub descriptor: Mutex<PoolDescriptor>,
    pub registry: Weak<Mutex<Registry>>,
    pub instance: PoolInstanceId,
    pub nodes: Mutex<BTreeSet<NodeId>>,
    pub node_claims: Mutex<BTreeMap<NodeId, super::ownership::NodeClaim>>,
    pub detaching: Mutex<BTreeSet<NodeId>>,
    pub retiring_tiers: Mutex<BTreeSet<TierId>>,
    pub retiring_disks: Mutex<BTreeSet<DiskId>>,
    pub(crate) events: Mutex<EventCache>,
    pub(crate) serial: AtomicU64,
    pub(super) event_limit: usize,
}
#[derive(Default)]
pub(crate) struct EventCache {
    pub records: BTreeMap<uuid::Uuid, (Arc<TopologyEvent>, PoolEventReceipt)>,
    pub order: VecDeque<uuid::Uuid>,
    pub versions: BTreeMap<workflow_foundation::ObjectKey, (Arc<TopologyEvent>, PoolEventReceipt)>,
}
impl PoolShared {
    pub fn new(
        descriptor: PoolDescriptor,
        registry: Weak<Mutex<Registry>>,
        config: &Config,
    ) -> Self {
        Self {
            takeover: tokio::sync::Mutex::new(false),
            recovery_report: Mutex::new(None),
            routing: Arc::default(),
            descriptor: Mutex::new(descriptor),
            registry,
            instance: PoolInstanceId::new(),
            nodes: Mutex::default(),
            node_claims: Mutex::default(),
            detaching: Mutex::default(),
            retiring_tiers: Mutex::default(),
            retiring_disks: Mutex::default(),
            events: Mutex::default(),
            serial: AtomicU64::new(1),
            event_limit: config
                .max_workflows
                .saturating_add(config.capacity)
                .saturating_add(config.task_history)
                .max(1),
        }
    }
}

impl PoolShared {
    pub(crate) fn has_reservation(&self, id: &DiskId) -> bool {
        self.routing.lock().unwrap().contains_key(id)
    }
    pub(crate) fn disk_spec(&self, id: &DiskId) -> Option<DiskSpec> {
        self.routing.lock().unwrap().get(id).cloned()
    }
    pub(crate) fn disk_directory(&self) -> Arc<Mutex<BTreeMap<DiskId, DiskSpec>>> {
        self.routing.clone()
    }
    pub(crate) fn register_disk(&self, spec: &DiskSpec) -> Outcome<()> {
        let mut routing = self.routing.lock().unwrap();
        if let Some(old) = routing.get(&spec.id)
            && (old.node != spec.node
                || old.tier != spec.tier
                || old.capacity_blks != spec.capacity_blks)
        {
            return Err(crate::support::reject("disk routing identity is immutable"));
        }
        routing.insert(spec.id.clone(), spec.clone());
        Ok(())
    }
    pub(crate) fn node_disks(&self, node: &NodeId) -> Vec<DiskSpec> {
        self.routing
            .lock()
            .unwrap()
            .values()
            .filter(|s| &s.node == node)
            .cloned()
            .collect()
    }
}
