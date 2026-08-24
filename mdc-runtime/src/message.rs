use std::{collections::HashMap, fmt::Debug, hash::Hash, sync::Arc};

use crate::{RuntimeError, ServiceActivity, ServiceContext, ServiceKey};

pub trait Message: Debug + Send + 'static {
    type Kind: Copy + Debug + Eq + Hash + Send + Sync + 'static;

    fn kind(&self) -> Self::Kind;
}

pub trait MessagePayload<M>: Debug + Send + Sized + 'static
where
    M: Message,
{
    const KIND: M::Kind;

    fn into_message(self) -> M;
    fn from_message(message: M) -> Result<Self, M>;
}

type Handler<K, M, S> =
    dyn Fn(&mut ServiceContext<K, M, S>, M) -> Result<(), RuntimeError> + Send + Sync + 'static;
type ShutdownHandler<S> = dyn Fn(&mut S) + Send + Sync + 'static;
type ActivityHandler<S> = dyn Fn(&mut S, ServiceActivity) + Send + Sync + 'static;

pub struct HandlerRegistry<K, M, S>
where
    K: ServiceKey,
    M: Message,
    S: Send + 'static,
{
    handlers: HashMap<M::Kind, Arc<Handler<K, M, S>>>,
    shutdown: Option<Arc<ShutdownHandler<S>>>,
    activity: Option<Arc<ActivityHandler<S>>>,
}

impl<K, M, S> HandlerRegistry<K, M, S>
where
    K: ServiceKey,
    M: Message,
    S: Send + 'static,
{
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            shutdown: None,
            activity: None,
        }
    }

    pub fn on<P, H>(&mut self, handler: H) -> Result<(), RuntimeError>
    where
        P: MessagePayload<M>,
        H: Fn(&mut ServiceContext<K, M, S>, P) -> Result<(), RuntimeError> + Send + Sync + 'static,
    {
        if self.handlers.contains_key(&P::KIND) {
            return Err(RuntimeError::HandlerAlreadyInstalled(format!(
                "{:?}",
                P::KIND
            )));
        }
        self.handlers.insert(
            P::KIND,
            Arc::new(move |service, message| {
                let payload =
                    P::from_message(message).map_err(|_| RuntimeError::WrongMessagePayload)?;
                handler(service, payload)
            }),
        );
        Ok(())
    }

    pub fn on_shutdown<H>(&mut self, handler: H)
    where
        H: Fn(&mut S) + Send + Sync + 'static,
    {
        self.shutdown = Some(Arc::new(handler));
    }

    pub fn on_activity<H>(&mut self, handler: H)
    where
        H: Fn(&mut S, ServiceActivity) + Send + Sync + 'static,
    {
        self.activity = Some(Arc::new(handler));
    }

    pub(crate) fn handler(&self, kind: M::Kind) -> Option<Arc<Handler<K, M, S>>> {
        self.handlers.get(&kind).cloned()
    }

    pub(crate) fn shutdown(&self, state: &mut S) {
        if let Some(handler) = &self.shutdown {
            handler(state);
        }
    }

    pub(crate) fn activity(&self, state: &mut S, activity: ServiceActivity) {
        if let Some(handler) = &self.activity {
            handler(state, activity);
        }
    }
}

impl<K, M, S> Clone for HandlerRegistry<K, M, S>
where
    K: ServiceKey,
    M: Message,
    S: Send + 'static,
{
    fn clone(&self) -> Self {
        Self {
            handlers: self.handlers.clone(),
            shutdown: self.shutdown.clone(),
            activity: self.activity.clone(),
        }
    }
}

impl<K, M, S> Default for HandlerRegistry<K, M, S>
where
    K: ServiceKey,
    M: Message,
    S: Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}
