use std::sync::{Mutex, MutexGuard};

/// Private service state with a deliberately closure-only API.
///
/// No lock guard is exposed, so workflow code cannot carry a mutable borrow
/// across an `.await`.
#[derive(Debug)]
pub struct StateCell<S> {
    inner: Mutex<S>,
}

impl<S: Default> Default for StateCell<S> {
    fn default() -> Self {
        Self::new(S::default())
    }
}

impl<S> StateCell<S> {
    pub fn new(state: S) -> Self {
        Self {
            inner: Mutex::new(state),
        }
    }

    pub fn read<R>(&self, read: impl FnOnce(&S) -> R) -> R {
        let guard = self.lock();
        read(&guard)
    }

    pub fn update<R>(&self, update: impl FnOnce(&mut S) -> R) -> R {
        let mut guard = self.lock();
        update(&mut guard)
    }

    fn lock(&self) -> MutexGuard<'_, S> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
