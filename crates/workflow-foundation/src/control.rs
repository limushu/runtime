//! Control requests are notifications; only the executor acknowledges a safe pause.
use crate::{Failure, Outcome};
use async_trait::async_trait;
use futures_util::FutureExt;
use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ControlRequest {
    pub generation: u64,
    pub user_paused: bool,
    pub system_paused: bool,
}
impl ControlRequest {
    pub fn pause_requested(self) -> bool {
        self.user_paused || self.system_paused
    }
}

/// A notification, never a stop acknowledgement. Cancellation is sticky; pause
/// requests are reversible and generation-tagged. Domain code must observe cancel.
#[derive(Clone)]
pub struct ControlToken {
    cancel: Arc<CancellationToken>,
    requests: watch::Receiver<ControlRequest>,
    explicit_cancel: Arc<AtomicBool>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancellationReason {
    Requested,
    Interrupted,
}
impl ControlToken {
    pub fn cancellation_reason(&self) -> Option<CancellationReason> {
        self.is_cancelled().then(|| {
            if self.explicit_cancel.load(Ordering::SeqCst) {
                CancellationReason::Requested
            } else {
                CancellationReason::Interrupted
            }
        })
    }
    pub fn request(&self) -> ControlRequest {
        *self.requests.borrow()
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
    pub async fn cancelled(&self) {
        self.cancel.cancelled().await;
    }
    /// Bridge to cancellation-aware domain/data-plane ports.
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancel
    }
    pub async fn changed(&mut self) -> Outcome<ControlRequest> {
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(Failure::Cancelled),
            changed = self.requests.changed() => {
                changed.map_err(|_| Failure::Unavailable)?;
                Ok(*self.requests.borrow_and_update())
            }
        }
    }
    pub(crate) fn child(&self) -> Self {
        Self {
            cancel: Arc::new(self.cancel.child_token()),
            requests: self.requests.clone(),
            explicit_cancel: self.explicit_cancel.clone(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct ControlSource {
    cancel: Arc<CancellationToken>,
    requests: watch::Sender<ControlRequest>,
    acknowledged: watch::Sender<Option<(u64, Arc<CancellationToken>)>>,
    wake: Arc<Notify>,
    explicit_cancel: Arc<AtomicBool>,
}
impl ControlSource {
    pub fn new(cancel: CancellationToken, wake: Arc<Notify>) -> Self {
        let (requests, _) = watch::channel(ControlRequest::default());
        let (acknowledged, _) = watch::channel(None);
        Self {
            cancel: Arc::new(cancel),
            requests,
            acknowledged,
            wake,
            explicit_cancel: Arc::default(),
        }
    }
    pub fn token(&self) -> ControlToken {
        ControlToken {
            cancel: self.cancel.clone(),
            requests: self.requests.subscribe(),
            explicit_cancel: self.explicit_cancel.clone(),
        }
    }
    pub fn request(&self) -> ControlRequest {
        *self.requests.borrow()
    }
    pub fn is_cancelled(&self) -> bool {
        self.cancel.is_cancelled()
    }
    pub fn set_pause(&self, user: bool, value: bool) -> Outcome<bool> {
        let mut exhausted = false;
        let changed = self.requests.send_if_modified(|request| {
            let previous = if user {
                request.user_paused
            } else {
                request.system_paused
            };
            if previous == value {
                return false;
            }
            let Some(generation) = request.generation.checked_add(1) else {
                exhausted = true;
                return false;
            };
            request.generation = generation;
            if user {
                request.user_paused = value;
            } else {
                request.system_paused = value;
            }
            true
        });
        if exhausted {
            return Err(Failure::Rejected("control generation exhausted".into()));
        }
        if changed {
            self.wake.notify_one();
        }
        Ok(changed)
    }
    pub fn cancel(&self) {
        self.explicit_cancel.store(true, Ordering::SeqCst);
        self.cancel.cancel();
        self.wake.notify_one();
    }
    pub fn acknowledge_pause(&self, generation: u64, token: &ControlToken) -> bool {
        let request = self.requests.borrow();
        if self.is_cancelled()
            || token.is_cancelled()
            || !request.pause_requested()
            || request.generation != generation
        {
            return false;
        }
        if self.acknowledged.send_if_modified(|ack| {
            if ack
                .as_ref()
                .is_some_and(|(old, scope)| *old == generation && Arc::ptr_eq(scope, &token.cancel))
            {
                false
            } else {
                *ack = Some((generation, token.cancel.clone()));
                true
            }
        }) {
            self.wake.notify_one();
        }

        true
    }
    pub fn pause_acknowledged(&self) -> bool {
        let request = self.requests.borrow();
        !self.is_cancelled()
            && request.pause_requested()
            && self
                .acknowledged
                .borrow()
                .as_ref()
                .is_some_and(|(generation, cancel)| {
                    *generation == request.generation && !cancel.is_cancelled()
                })
    }
    pub fn acknowledgements(&self) -> watch::Receiver<Option<(u64, Arc<CancellationToken>)>> {
        self.acknowledged.subscribe()
    }
}

pub(crate) async fn pause_result(future: impl Future<Output = Outcome<()>>) -> Outcome<()> {
    AssertUnwindSafe(future)
        .catch_unwind()
        .await
        .unwrap_or_else(|_| {
            Err(crate::Failure::Uncertain(
                "Activity pause hook panicked".into(),
            ))
        })
}

/// Optional mid-Activity pause capability. Send domain commands and await their
/// actual outcomes. Cancellation uses ONLY ActivityContext::control().cancelled().
/// Register before long-running waits. If a short create-or-attach must first obtain
/// the job handle, settle that request and register immediately; no pause is acknowledged
/// before registration. Do not acquire resources held by the external work.
#[async_trait]
pub trait ActivityPause: Send + Sync + 'static {
    async fn pause(&self) -> Outcome<()>;
    async fn resume(&self) -> Outcome<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContinueDecision {
    Ready,
    Wait(String),
}

/// Read-only business preflight, run before first execution and each continuation.
/// A Wait is not a task failure. Call RunControl::recheck when business facts change.
/// Checks may be abandoned when a new pause/cancel arrives; do not perform effects here.
#[async_trait]
pub trait ContinuationGuard: Send + Sync + 'static {
    async fn check(&self) -> Outcome<ContinueDecision>;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn old_acknowledgements_cannot_survive_resume_repause_or_activity_cancellation() {
        let source = ControlSource::new(CancellationToken::new(), Arc::new(Notify::new()));
        let token = source.token().child();
        source.set_pause(true, true).unwrap();
        let old = token.request().generation;
        assert!(source.acknowledge_pause(old, &token));
        source.set_pause(true, false).unwrap();
        source.set_pause(true, true).unwrap();
        assert!(!source.pause_acknowledged());
        assert!(!source.acknowledge_pause(old, &token));
        assert!(source.acknowledge_pause(token.request().generation, &token));
        token.cancellation().cancel();
        assert!(!source.pause_acknowledged());
        assert!(!source.is_cancelled()); // A service stop never cancels the whole Pool.
    }
    #[tokio::test]
    async fn cancellation_reaches_descendants_and_cannot_be_cleared_by_resume() {
        let source = ControlSource::new(CancellationToken::new(), Arc::new(Notify::new()));
        let mut token = source.token().child();
        source.cancel();
        source.set_pause(true, false).unwrap();
        assert!(token.is_cancelled());
        assert_eq!(token.changed().await, Err(Failure::Cancelled));
        assert_eq!(
            token.cancellation_reason(),
            Some(CancellationReason::Requested)
        );
    }
    #[test]
    fn parent_shutdown_is_an_interruption_not_a_permanent_business_cancellation() {
        let parent = CancellationToken::new();
        let source = ControlSource::new(parent.child_token(), Arc::new(Notify::new()));
        let token = source.token().child();
        parent.cancel();
        assert_eq!(
            token.cancellation_reason(),
            Some(CancellationReason::Interrupted)
        );
    }
}
