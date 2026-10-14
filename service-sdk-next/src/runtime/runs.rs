use futures_util::{Stream, stream::FuturesUnordered};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

type BoxRun<C> = Pin<Box<dyn Future<Output = C> + Send + 'static>>;

/// Futures started by one service scheduling pass.
///
/// Business code calls `push` with ordinary async functions. Pinning and type
/// erasure stay inside the runtime.
pub struct NewRuns<C> {
    futures: Vec<BoxRun<C>>,
}

impl<C> Default for NewRuns<C> {
    fn default() -> Self {
        Self {
            futures: Vec::new(),
        }
    }
}

impl<C> NewRuns<C> {
    pub fn push<F>(&mut self, future: F)
    where
        F: Future<Output = C> + Send + 'static,
    {
        self.futures.push(Box::pin(future));
    }

    pub fn len(&self) -> usize {
        self.futures.len()
    }

    pub fn is_empty(&self) -> bool {
        self.futures.is_empty()
    }

    pub(crate) fn drain(&mut self) -> impl Iterator<Item = BoxRun<C>> + '_ {
        self.futures.drain(..)
    }
}

pub(crate) struct RunDriver<C> {
    futures: FuturesUnordered<BoxRun<C>>,
}

impl<C> Default for RunDriver<C> {
    fn default() -> Self {
        Self {
            futures: FuturesUnordered::new(),
        }
    }
}

impl<C> RunDriver<C> {
    pub fn extend(&mut self, runs: &mut NewRuns<C>) {
        self.futures.extend(runs.drain());
    }

    pub fn len(&self) -> usize {
        self.futures.len()
    }

    pub fn is_empty(&self) -> bool {
        self.futures.is_empty()
    }
}

impl<C> Stream for RunDriver<C> {
    type Item = C;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.futures).poll_next(cx)
    }
}
