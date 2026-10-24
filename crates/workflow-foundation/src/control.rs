//! Cooperative controls for existing activities. A notification is never a safe-stop ACK.
use crate::*;
use async_trait::async_trait;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

/// Completed finishes the business step. Suspended ends this invocation after
/// actual effects and callback lifetimes have settled, retaining only resumable
/// data. Cancellation uses Err(Failure::Cancelled), never Suspended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ActivityExit<T, C> {
    Completed(T),
    Suspended(C),
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ControlRequest {
    pub generation: u64,
    pub suspend_reasons: BTreeSet<String>,
}
impl ControlRequest {
    pub fn paused(&self) -> bool {
        !self.suspend_reasons.is_empty()
    }
}

/// Optional adapter for an operation which remains pending while safely stopped.
/// These methods issue and await domain commands; the framework knows no domain commands.
#[async_trait]
pub trait ActivityControl: Send + Sync + 'static {
    async fn suspend(&self) -> Outcome<()>;
    async fn resume(&self) -> Outcome<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readiness {
    Ready,
    Blocked(String),
}
/// Business-defined, side-effect-free check. It can use stage state shared with run().
#[async_trait]
pub trait RunCondition: Send + Sync + 'static {
    async fn check(&self) -> Outcome<Readiness>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationReason {
    Requested,
    Interrupted,
}

#[derive(Clone)]
pub struct ControlToken {
    pub(crate) requests: watch::Receiver<ControlRequest>,
    pub(crate) cancel: CancellationToken,
    user_cancelled: Arc<std::sync::atomic::AtomicBool>,
}
impl ControlToken {
    pub fn cancellation_reason(&self) -> Option<CancellationReason> {
        self.cancel.is_cancelled().then(|| {
            if self
                .user_cancelled
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                CancellationReason::Requested
            } else {
                CancellationReason::Interrupted
            }
        })
    }
    pub fn request(&self) -> ControlRequest {
        self.requests.borrow().clone()
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }
    pub async fn changed(&mut self) -> Outcome<()> {
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(Failure::Cancelled),
            r = self.requests.changed() => r.map_err(|_| Failure::Unavailable),
        }
    }
}

#[derive(Clone)]
pub(crate) struct Controls {
    pub tx: watch::Sender<ControlRequest>,
    ack: Arc<Mutex<Option<(u64, CancellationToken)>>>,
    pub wake: Arc<Notify>,
    pub changed: Arc<Notify>,
    pub recheck: Arc<Notify>,
    pub user_cancelled: Arc<std::sync::atomic::AtomicBool>,
}
impl Controls {
    pub fn new(wake: Arc<Notify>) -> Self {
        Self {
            tx: watch::channel(ControlRequest::default()).0,
            ack: Arc::default(),
            wake,
            changed: Arc::default(),
            recheck: Arc::default(),
            user_cancelled: Arc::default(),
        }
    }
    pub fn token(&self, cancel: CancellationToken) -> ControlToken {
        ControlToken {
            requests: self.tx.subscribe(),
            cancel,
            user_cancelled: self.user_cancelled.clone(),
        }
    }
    pub fn reasons(&self) -> BTreeSet<String> {
        self.tx.borrow().suspend_reasons.clone()
    }
    pub fn update(&self, update: impl FnOnce(&mut BTreeSet<String>)) {
        let changed = self.tx.send_if_modified(|request| {
            let old = request.suspend_reasons.clone();
            update(&mut request.suspend_reasons);
            if old == request.suspend_reasons {
                return false;
            }
            request.generation = request
                .generation
                .checked_add(1)
                .expect("control generation exhausted");
            *self.ack.lock().unwrap() = None;
            true
        });
        if changed {
            self.changed.notify_waiters();
            self.wake.notify_one();
        }
    }
    pub fn revoke(&self) {
        if self.ack.lock().unwrap().take().is_some() {
            self.changed.notify_waiters();
            self.wake.notify_one();
        }
    }
    pub fn acknowledge(&self, generation: u64, cancel: &CancellationToken) -> bool {
        let request = self.tx.borrow();
        if request.generation != generation || !request.paused() || cancel.is_cancelled() {
            return false;
        }
        *self.ack.lock().unwrap() = Some((generation, cancel.clone()));
        self.changed.notify_waiters();
        self.wake.notify_one();
        true
    }
    pub fn acknowledged(&self) -> bool {
        let request = self.tx.borrow();
        request.paused()
            && self
                .ack
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|(generation, token)| {
                    *generation == request.generation && !token.is_cancelled()
                })
    }
}
