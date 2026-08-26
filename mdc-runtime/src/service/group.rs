use std::{collections::HashMap, sync::Arc};

use tokio::task::AbortHandle;

use crate::{
    Router, RuntimeError, Service, ServiceActivity, ServiceLifecycle, ServiceObserver,
    ServiceSnapshot, executor,
};

use super::{ControlHandle, ServiceClient, ServiceKey, ShutdownMode};

pub struct ServiceRef<K, S>
where
    K: ServiceKey,
    S: Service<Key = K>,
{
    pub key: K,
    pub client: ServiceClient<S>,
    pub control: ControlHandle,
    pub observer: ServiceObserver<K>,
}

impl<K, S> Clone for ServiceRef<K, S>
where
    K: ServiceKey,
    S: Service<Key = K>,
{
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            client: self.client.clone(),
            control: self.control.clone(),
            observer: self.observer.clone(),
        }
    }
}

pub(crate) struct SpawnedService<K, S>
where
    K: ServiceKey,
    S: Service<Key = K>,
{
    pub reference: ServiceRef<K, S>,
    pub guard: ServiceTaskGuard,
}

pub(crate) struct ServiceTaskGuard {
    abort: AbortHandle,
}

impl ServiceTaskGuard {
    pub(crate) fn new(abort: AbortHandle) -> Self {
        Self { abort }
    }
}

impl Drop for ServiceTaskGuard {
    fn drop(&mut self) {
        self.abort.abort();
    }
}

struct ServiceEntry<K> {
    control: ControlHandle,
    observer: ServiceObserver<K>,
    _guard: ServiceTaskGuard,
}

pub struct ServiceGroup<K: ServiceKey> {
    router: Router<K>,
    entries: HashMap<K, ServiceEntry<K>>,
}

impl<K: ServiceKey> ServiceGroup<K> {
    pub fn new() -> Self {
        Self {
            router: Router::new(),
            entries: HashMap::new(),
        }
    }

    pub fn router(&self) -> Router<K> {
        self.router.clone()
    }

    pub async fn spawn<S: Service<Key = K>>(
        &mut self,
        key: K,
        service: Arc<S>,
        queue_capacity: usize,
    ) -> Result<ServiceRef<K, S>, RuntimeError> {
        let name: Arc<str> = format!("{key:?}").into();
        let spawned = executor::spawn_service(key.clone(), name, service, queue_capacity).await?;
        self.router
            .register(key.clone(), spawned.reference.client.clone())?;
        let reference = spawned.reference.clone();
        self.entries.insert(
            key,
            ServiceEntry {
                control: spawned.reference.control,
                observer: spawned.reference.observer,
                _guard: spawned.guard,
            },
        );
        Ok(reference)
    }

    pub fn observer(&self, key: &K) -> Option<ServiceObserver<K>> {
        self.entries.get(key).map(|entry| entry.observer.clone())
    }

    pub async fn shutdown_all(&self, mode: ShutdownMode) -> Result<(), RuntimeError> {
        for entry in self.entries.values() {
            entry.control.shutdown(mode).await?;
        }
        Ok(())
    }
}

impl<K: ServiceKey> Default for ServiceGroup<K> {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) fn lifecycle_snapshot<K: Clone>(
    service: K,
    lifecycle: ServiceLifecycle,
) -> ServiceSnapshot<K> {
    ServiceSnapshot {
        service,
        lifecycle,
        activity: ServiceActivity::Idle,
        queued_requests: 0,
        inflight_requests: 0,
        managed_tasks: 0,
    }
}
