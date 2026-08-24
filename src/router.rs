use std::{collections::HashMap, sync::Arc};

use tokio::sync::RwLock;

use crate::{CommandHandle, Message, MessageContext, MessagePayload, RuntimeError, ServiceKey};

pub struct Router<K, M>
where
    K: ServiceKey,
    M: Message,
{
    routes: Arc<RwLock<HashMap<K, CommandHandle<K, M>>>>,
}

impl<K, M> Router<K, M>
where
    K: ServiceKey,
    M: Message,
{
    pub fn new() -> Self {
        Self {
            routes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub(crate) async fn register(&self, service: K, handle: CommandHandle<K, M>) {
        self.routes.write().await.insert(service, handle);
    }

    pub async fn send(
        &self,
        target: K,
        message: M,
        context: MessageContext,
    ) -> Result<(), RuntimeError> {
        let handle = self
            .routes
            .read()
            .await
            .get(&target)
            .cloned()
            .ok_or_else(|| RuntimeError::ServiceNotFound(format!("{target:?}")))?;
        handle.send(message, context).await
    }

    pub async fn send_payload<P>(
        &self,
        target: K,
        payload: P,
        context: MessageContext,
    ) -> Result<(), RuntimeError>
    where
        P: MessagePayload<M>,
    {
        self.send(target, payload.into_message(), context).await
    }
}

impl<K, M> Clone for Router<K, M>
where
    K: ServiceKey,
    M: Message,
{
    fn clone(&self) -> Self {
        Self {
            routes: self.routes.clone(),
        }
    }
}

impl<K, M> Default for Router<K, M>
where
    K: ServiceKey,
    M: Message,
{
    fn default() -> Self {
        Self::new()
    }
}
