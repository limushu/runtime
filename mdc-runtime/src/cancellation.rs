use std::sync::{Arc, Mutex};

use tokio_util::sync::CancellationToken;

use crate::CancelReason;

#[derive(Clone)]
pub(crate) struct CancellationScope {
    inner: Arc<CancellationInner>,
}

struct CancellationInner {
    token: CancellationToken,
    reason: Mutex<Option<CancelReason>>,
    parent: Option<CancellationScope>,
}

impl CancellationScope {
    pub(crate) fn root() -> Self {
        Self {
            inner: Arc::new(CancellationInner {
                token: CancellationToken::new(),
                reason: Mutex::new(None),
                parent: None,
            }),
        }
    }

    pub(crate) fn child(&self) -> Self {
        Self {
            inner: Arc::new(CancellationInner {
                token: self.inner.token.child_token(),
                reason: Mutex::new(None),
                parent: Some(self.clone()),
            }),
        }
    }

    pub(crate) fn cancel(&self, reason: CancelReason) {
        let mut current = self
            .inner
            .reason
            .lock()
            .expect("cancellation reason poisoned");
        if current.is_none() {
            *current = Some(reason);
        }
        drop(current);
        self.inner.token.cancel();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.inner.token.is_cancelled()
    }

    pub(crate) fn reason(&self) -> Option<CancelReason> {
        if !self.is_cancelled() {
            return None;
        }
        if let Some(reason) = self
            .inner
            .reason
            .lock()
            .expect("cancellation reason poisoned")
            .clone()
        {
            return Some(reason);
        }
        self.inner.parent.as_ref().and_then(Self::reason)
    }

    pub(crate) async fn cancelled(&self) -> CancelReason {
        self.inner.token.cancelled().await;
        self.reason().unwrap_or(CancelReason::ParentCancelled)
    }
}
