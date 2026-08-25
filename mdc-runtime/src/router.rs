use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{RuntimeError, Service, ServiceClient, ServiceKey};

pub struct Router<K: ServiceKey> {
    routes: Arc<RwLock<HashMap<K, Arc<dyn Any + Send + Sync>>>>,
}

impl<K: ServiceKey> Router<K> {
    pub fn new() -> Self {
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) fn register<S: Service>(
        &self,
        key: K,
        client: ServiceClient<S>,
    ) -> Result<(), RuntimeError> {
        let mut routes = self.routes.write().expect("router poisoned");
        if routes.contains_key(&key) {
            return Err(RuntimeError::ServiceUnavailable(format!(
                "{key:?} is already registered"
            )));
        }
        routes.insert(key, Arc::new(client));
        Ok(())
    }

    pub fn client<S: Service>(&self, key: &K) -> Result<ServiceClient<S>, RuntimeError> {
        let routes = self.routes.read().expect("router poisoned");
        let route = routes
            .get(key)
            .ok_or_else(|| RuntimeError::ServiceNotFound(format!("{key:?}")))?;
        route
            .downcast_ref::<ServiceClient<S>>()
            .cloned()
            .ok_or_else(|| RuntimeError::WrongServiceType(format!("{key:?}")))
    }
}

impl<K: ServiceKey> Clone for Router<K> {
    fn clone(&self) -> Self {
        Self {
            routes: self.routes.clone(),
        }
    }
}

impl<K: ServiceKey> Default for Router<K> {
    fn default() -> Self {
        Self::new()
    }
}
