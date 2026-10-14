use super::{ExecutionScope, ObjectClaim, WorkflowError, WorkflowStatus};
use crate::{process_manager::WorkflowFailure, runtime::CommandId};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{Notify, watch};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
struct ActivityState {
    closed: bool,
    pending: usize,
    uncertain: bool,
}
#[derive(Clone, Default)]
pub(crate) struct ActivityTracker {
    state: Arc<Mutex<ActivityState>>,
    changed: Arc<Notify>,
}
impl ActivityTracker {
    pub(crate) async fn close_and_wait(&self, cancel: &CancellationToken) -> bool {
        {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            if state.pending != 0 {
                cancel.cancel();
            }
        }
        loop {
            let changed = self.changed.notified();
            {
                let state = self.state.lock().unwrap();
                if state.pending == 0 {
                    return state.uncertain;
                }
            }
            changed.await;
        }
    }
}
/// Owned by the request, not the caller's response future.
pub(crate) struct ActivityLease {
    tracker: ActivityTracker,
    uncertain: bool,
}
impl ActivityLease {
    pub(crate) fn started(&mut self) {
        self.uncertain = true;
    }
    pub(crate) fn settled<T, E: WorkflowFailure>(&mut self, result: &Result<T, E>) {
        self.uncertain = result.as_ref().is_err_and(WorkflowFailure::needs_recovery);
    }
}
impl Drop for ActivityLease {
    fn drop(&mut self) {
        let mut state = self.tracker.state.lock().unwrap();
        state.pending -= 1;
        state.uncertain |= self.uncertain;
        drop(state);
        self.tracker.changed.notify_one();
    }
}
/// Not Clone: one workflow cannot concurrently mutate objects under one grant.
pub struct WorkflowContext {
    pub id: CommandId,
    pub(crate) scope: ExecutionScope,
    pub(crate) claims: Vec<ObjectClaim>,
    pub(crate) cancel: CancellationToken,
    pub(crate) enabled: watch::Receiver<bool>,
    pub(crate) status: Arc<Mutex<WorkflowStatus>>,
    pub(crate) changed: Arc<Notify>,
    pub(crate) activities: ActivityTracker,
}
impl WorkflowContext {
    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancel
    }
    pub async fn checkpoint(&self) -> Result<(), WorkflowError> {
        let mut enabled = self.enabled.clone();
        loop {
            if self.cancel.is_cancelled() {
                return Err(WorkflowError::Cancelled);
            }
            if *enabled.borrow_and_update() {
                return Ok(());
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return Err(WorkflowError::Cancelled),
                changed = enabled.changed() => if changed.is_err() { return Err(WorkflowError::Cancelled); },
            }
        }
    }
    pub async fn sleep(&self, delay: Duration) -> Result<(), WorkflowError> {
        self.checkpoint().await?;
        tokio::select! {
            biased;
            _ = self.cancel.cancelled() => Err(WorkflowError::Cancelled),
            _ = tokio::time::sleep(delay) => self.checkpoint().await,
        }
    }
    pub(crate) async fn authorize(
        &mut self,
        scope: &ExecutionScope,
        required: &[ObjectClaim],
        name: &'static str,
    ) -> Result<ActivityLease, WorkflowError> {
        self.checkpoint().await?;
        if !self.scope.same(scope)
            || !required
                .iter()
                .all(|r| self.claims.iter().any(|c| c.covers(r)))
        {
            return Err(WorkflowError::Failed(
                "activity outside the workflow Pool/object grant".into(),
            ));
        }
        let mut state = self.activities.state.lock().unwrap();
        if state.uncertain {
            return Err(WorkflowError::NeedsRecovery(
                "prior activity needs reconciliation".into(),
            ));
        }
        if state.closed || state.pending != 0 {
            return Err(WorkflowError::Failed(
                "workflow closed or prior activity unsettled".into(),
            ));
        }
        state.pending += 1;
        *self.status.lock().unwrap() = WorkflowStatus::Running {
            activity: Some(name),
        };
        self.changed.notify_one();
        Ok(ActivityLease {
            tracker: self.activities.clone(),
            uncertain: false,
        })
    }
    pub(crate) fn activity_finished(&self) {
        *self.status.lock().unwrap() = WorkflowStatus::Running { activity: None };
        self.changed.notify_one();
    }
}
