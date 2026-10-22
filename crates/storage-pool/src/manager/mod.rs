//! Shared handle registry. No manager actor, domain scheduler, or global workflow host.
pub(crate) mod admin;
mod fanout;
mod identity;
mod lifecycle;
mod ownership;
mod pool_ops;
pub use fanout::*;
pub use identity::*;
pub(crate) use ownership::{DiskReservation, NodeReservation, RemovalGuard};
pub(crate) use pool_ops::PoolShared;

use crate::{ports::*, support::reject, *};
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Default)]
pub(crate) struct Registry {
    pub pools: BTreeMap<PoolId, Pool>,
    pub uuids: BTreeMap<PoolUuid, PoolId>,
    pub names: BTreeMap<PoolName, PoolId>,
    pub disks: BTreeMap<DiskId, ownership::DiskOwnership>,
    pub stopped: bool,
    pub retired: BTreeMap<PoolId, PoolDescriptor>,
    pub history: VecDeque<Pool>,
    pub history_limit: usize,
}

/// Clones share registry ownership. Pool handles remain bound to their original UUID.
#[derive(Clone)]
pub struct PoolManager {
    pub(crate) registry: Arc<Mutex<Registry>>,
    config: Config,
    metadata: Arc<dyn MetadataStore>,
    data_plane: Arc<dyn DataPlane>,
    pub(crate) next_fanout: Arc<AtomicUsize>,
}
impl PoolManager {
    pub fn new(
        config: Config,
        metadata: Arc<dyn MetadataStore>,
        data_plane: Arc<dyn DataPlane>,
    ) -> Self {
        Self {
            registry: Arc::new(Mutex::new(Registry {
                history_limit: config.task_history,
                ..Registry::default()
            })),
            config,
            metadata,
            data_plane,
            next_fanout: Arc::new(AtomicUsize::new(0)),
        }
    }
    pub fn create(&self, request: CreatePool) -> Outcome<PoolHandle> {
        // Construction is synchronous and does no external IO. Publish all indexes together.
        let mut registry = self.registry.lock().unwrap();
        if registry.stopped {
            return Err(Failure::Unavailable);
        }
        if registry.pools.contains_key(&request.id)
            || registry.retired.contains_key(&request.id)
            || registry.uuids.contains_key(&request.uuid)
            || registry.names.contains_key(&request.name)
        {
            return Err(reject("pool id, UUID, or name already used"));
        }
        let descriptor = request.descriptor();
        let pool = Pool::start(
            descriptor.clone(),
            self.config.clone(),
            self.metadata.clone(),
            self.data_plane.clone(),
            Arc::downgrade(&self.registry),
        )?;
        let handle = pool.handle();
        registry
            .uuids
            .insert(descriptor.uuid, descriptor.id.clone());
        registry
            .names
            .insert(descriptor.name, descriptor.id.clone());
        registry.pools.insert(descriptor.id, pool);
        Ok(handle)
    }
    /// Compatibility shorthand; name is initially equal to the explicitly supplied ID.
    pub fn create_pool(&self, id: PoolId) -> Outcome<PoolHandle> {
        let name = PoolName::new(id.as_str())?;
        self.create(CreatePool::new(id, name))
    }
    pub fn get(&self, selector: impl Into<PoolSelector>) -> Outcome<PoolHandle> {
        let registry = self.registry.lock().unwrap();
        let selector = selector.into();
        let id = match &selector {
            PoolSelector::Id(id) => Some(id),
            PoolSelector::Uuid(uuid) => registry.uuids.get(uuid),
            PoolSelector::Name(name) => registry.names.get(name),
        }
        .ok_or_else(|| reject("unknown pool"))?;
        let pool = registry
            .pools
            .get(id)
            .ok_or_else(|| reject("unknown pool"))?;
        let handle = pool.handle();
        if handle.descriptor().lifecycle == PoolLifecycle::Deleted {
            return Err(reject("pool is deleted"));
        }
        Ok(handle)
    }
    pub fn pool(&self, id: &PoolId) -> Outcome<PoolHandle> {
        self.get(id)
    }
    pub fn list(&self) -> Vec<PoolDescriptor> {
        let registry = self.registry.lock().unwrap();
        let mut descriptors: Vec<_> = registry
            .pools
            .values()
            .map(|pool| pool.handle().descriptor())
            .chain(registry.retired.values().cloned())
            .collect();
        descriptors.sort_by(|a, b| a.id.cmp(&b.id));
        descriptors
    }
    pub fn tasks(&self) -> Vec<PoolTaskSnapshot> {
        let handles: Vec<_> = {
            let registry = self.registry.lock().unwrap();
            registry
                .pools
                .values()
                .chain(registry.history.iter())
                .map(Pool::handle)
                .collect()
        };
        handles
            .into_iter()
            .flat_map(|pool| {
                pool.tasks()
                    .into_iter()
                    .map(move |snapshot| PoolTaskSnapshot {
                        task: pool.task_ref(snapshot.id),
                        snapshot,
                    })
            })
            .collect()
    }
    fn retained_pool(&self, uuid: PoolUuid) -> Outcome<PoolHandle> {
        let registry = self.registry.lock().unwrap();
        let id = registry
            .uuids
            .get(&uuid)
            .ok_or_else(|| reject("unknown pool UUID"))?;
        registry
            .pools
            .get(id)
            .or_else(|| {
                registry
                    .history
                    .iter()
                    .find(|pool| pool.handle().uuid() == uuid)
            })
            .map(Pool::handle)
            .ok_or_else(|| reject("pool execution history has expired"))
    }
    pub fn task(&self, task: TaskRef) -> Outcome<TaskSnapshot> {
        self.retained_pool(task.pool)?.task_by_ref(task)
    }
    pub fn observe_task(&self, task: TaskRef) -> Outcome<TaskObserver> {
        self.retained_pool(task.pool)?.observe_task_by_ref(task)
    }
    pub fn cancel(&self, task: TaskRef) -> Outcome<()> {
        let pool = self.get(task.pool)?;
        pool.check_task_ref(task)?;
        pool.cancel(task.workflow)
    }
    pub fn suspend(&self, task: TaskRef) -> Outcome<()> {
        let pool = self.get(task.pool)?;
        pool.check_task_ref(task)?;
        pool.suspend(task.workflow)
    }
    pub fn resume(&self, task: TaskRef) -> Outcome<()> {
        let pool = self.get(task.pool)?;
        pool.check_task_ref(task)?;
        pool.resume(task.workflow)
    }
    /// Compatibility facade. Membership validation and reservation live in Pool.
    pub async fn online_disk(&self, pool: &PoolId, spec: DiskSpec) -> Outcome<Run<MemberDisk>> {
        self.get(pool)?.add_disk(spec).await
    }
    pub async fn shutdown_pool(&self, id: &PoolId) -> Outcome<()> {
        self.get(id)?.shutdown().await
    }
    pub async fn shutdown(&self) -> Outcome<()> {
        let handles: Vec<_> = {
            let mut registry = self.registry.lock().unwrap();
            registry.stopped = true;
            registry.pools.values().map(Pool::handle).collect()
        };
        let mut closing: FuturesUnordered<_> = handles
            .into_iter()
            .map(|pool| async move { pool.shutdown().await })
            .collect();
        let mut error = None;
        while let Some(result) = closing.next().await {
            if let Err(failure) = result {
                error.get_or_insert(failure);
            }
        }
        error.map_or(Ok(()), Err)
    }
    pub(crate) fn fanout_handles(&self) -> Vec<PoolHandle> {
        let mut handles: Vec<_> = self
            .registry
            .lock()
            .unwrap()
            .pools
            .values()
            .map(Pool::handle)
            .filter(|p| p.descriptor().lifecycle != PoolLifecycle::Deleted)
            .collect();
        if !handles.is_empty() {
            let offset = self.next_fanout.fetch_add(1, Ordering::Relaxed) % handles.len();
            handles.rotate_left(offset);
        }
        handles
    }
}
