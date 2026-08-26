use std::{
    any::Any,
    collections::HashMap,
    sync::{Arc, RwLock},
};

use crate::{
    CallError, RequestContext, RuntimeError, Service, ServiceClient, ServiceKey, Submission,
    TaskExit, TaskTicket,
};

pub struct Router<K: ServiceKey> {
    routes: Arc<RwLock<HashMap<K, Arc<dyn Any + Send + Sync>>>>,
}

impl<K: ServiceKey> Router<K> {
    pub fn new() -> Self {
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) fn register<S: Service<Key = K>>(
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

    pub fn client<S: Service<Key = K>>(&self, key: &K) -> Result<ServiceClient<S>, RuntimeError> {
        let routes = self.routes.read().expect("router poisoned");
        let route = routes
            .get(key)
            .ok_or_else(|| RuntimeError::ServiceNotFound(format!("{key:?}")))?;
        route
            .downcast_ref::<ServiceClient<S>>()
            .cloned()
            .ok_or_else(|| RuntimeError::WrongServiceType(format!("{key:?}")))
    }

    pub async fn call<S: Service<Key = K>>(
        &self,
        key: &K,
        request: S::Request,
        context: RequestContext,
    ) -> Result<S::Response, CallError<S::Error>> {
        if let Some(reason) = context.cancellation_reason() {
            return Err(CallError::Cancelled(reason));
        }
        let client = self.client::<S>(key).map_err(CallError::Runtime)?;
        let submission = client
            .submit_with(request, context.clone())
            .await
            .map_err(CallError::Runtime)?;
        match submission {
            Submission::Reply(result) => {
                if let Some(reason) = context.cancellation_reason() {
                    Err(CallError::Cancelled(reason))
                } else {
                    result.map_err(CallError::Service)
                }
            }
            Submission::Task(ticket) => wait_for_child(context, ticket).await,
        }
    }
}

async fn wait_for_child<T, E>(
    context: RequestContext,
    ticket: TaskTicket<T, E>,
) -> Result<T, CallError<E>> {
    let (task_id, control, mut completion) = ticket.into_parts();
    tokio::select! {
        biased;
        reason = context.cancelled() => {
            let _ = control.cancel_task(task_id, reason.clone()).await;
            let _ = (&mut completion)
                .await
                .map_err(|_| CallError::Runtime(RuntimeError::ResponseDropped))?;
            Err(CallError::Cancelled(reason))
        }
        exit = &mut completion => {
            match exit.map_err(|_| CallError::Runtime(RuntimeError::ResponseDropped))? {
                TaskExit::Completed(value) => Ok(value),
                TaskExit::Failed(error) => Err(CallError::Service(error)),
                TaskExit::Cancelled(reason) => Err(CallError::Cancelled(reason)),
                TaskExit::Aborted => Err(CallError::Aborted),
            }
        }
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
