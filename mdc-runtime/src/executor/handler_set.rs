use std::{collections::HashMap, sync::Arc};

use futures::{
    FutureExt,
    future::{AbortHandle, Abortable, BoxFuture},
    stream::{FuturesUnordered, StreamExt},
};

use crate::{RequestContext, RequestId, Service};

pub(super) enum HandlerExit<T, E> {
    Finished(Result<T, E>),
    Aborted,
}

pub(super) struct FinishedHandler<T, E> {
    pub request_id: RequestId,
    pub exit: HandlerExit<T, E>,
}

pub(super) struct HandlerSet<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    running: HashMap<RequestId, AbortHandle>,
    futures: FuturesUnordered<BoxFuture<'static, FinishedHandler<T, E>>>,
}

impl<T, E> HandlerSet<T, E>
where
    T: Send + 'static,
    E: Send + 'static,
{
    pub(super) fn new() -> Self {
        Self {
            running: HashMap::new(),
            futures: FuturesUnordered::new(),
        }
    }

    pub(super) fn start<S>(&mut self, service: Arc<S>, request: S::Request, context: RequestContext)
    where
        S: Service<Response = T, Error = E>,
    {
        let request_id = context.request_id;
        let (abort, registration) = AbortHandle::new_pair();
        let future = Abortable::new(service.handle(request, context), registration)
            .map(move |result| FinishedHandler {
                request_id,
                exit: match result {
                    Ok(result) => HandlerExit::Finished(result),
                    Err(_) => HandlerExit::Aborted,
                },
            })
            .boxed();
        self.running.insert(request_id, abort);
        self.futures.push(future);
    }

    pub(super) async fn next_finished(&mut self) -> Option<FinishedHandler<T, E>> {
        let finished = self.futures.next().await?;
        self.running.remove(&finished.request_id);
        Some(finished)
    }

    pub(super) fn abort_all(&self) {
        for abort in self.running.values() {
            abort.abort();
        }
    }

    pub(super) fn len(&self) -> usize {
        self.running.len()
    }

    pub(super) fn is_empty(&self) -> bool {
        self.running.is_empty()
    }

    pub(super) fn has_running(&self) -> bool {
        !self.running.is_empty()
    }
}
