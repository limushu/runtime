//! Task control: requests (RunControl), delivery (ControlToken), confirmation
//! (Controls). Driver execution persists control intent before cancellation delivery.
//! WorkflowContext in workflow.rs consumes Activity replies and acknowledges a
//! pause at wait_until_runnable, after business checkpoint handling has finished.
use crate::*;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
};
use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct RunControl {
    pub id: TaskId,
    pub name: &'static str,
    pub(crate) cancel: CancellationToken,
    pub(crate) signals: mpsc::Sender<Signal>,
    pub(crate) state: watch::Receiver<RunState>,
    pub(crate) max_signal_bytes: usize,
    pub(crate) controls: crate::control::Controls,
    pub(crate) persistent: bool,
    pub(crate) acceptance: watch::Receiver<Option<Outcome<()>>>,
    pub(crate) info: watch::Sender<TaskInfo>,
    pub(crate) progress: watch::Receiver<TaskProgress>,
    pub(crate) activities: watch::Receiver<ActivityHistory>,
    pub(crate) objects: Arc<Mutex<Vec<ObjectKey>>>,
}
impl RunControl {
    /// None while persisted registration is pending. Not a workflow completion result.
    pub fn acceptance_status(&self) -> Option<Outcome<()>> {
        self.acceptance.borrow().clone()
    }
    /// Request a pause at the next safe boundary, not preemption of external IO.
    pub fn suspend(&self) -> Outcome<()> {
        self.set_suspended(true)
    }
    pub fn resume(&self) -> Outcome<()> {
        self.set_suspended(false)
    }
    fn set_suspended(&self, value: bool) -> Outcome<()> {
        if self.cancel.is_cancelled()
            || matches!(self.state(), RunState::Completed | RunState::Failed(_))
        {
            return Err(Failure::Rejected("run is cancelled or terminal".into()));
        }
        self.controls.update(|reasons| {
            if value {
                reasons.insert("user".into());
            } else {
                reasons.remove("user");
            }
        });
        Ok(())
    }
    pub fn set_blockers(&self, blockers: Vec<TaskId>) {
        self.info.send_if_modified(|info| {
            if info.blockers == blockers {
                false
            } else {
                info.blockers = blockers;
                true
            }
        });
    }
    pub fn explain(&self, conflicts: Vec<ConflictMatch>, blockers: Vec<TaskId>) {
        self.info.send_modify(|info| {
            info.conflicts = conflicts;
            info.blockers = blockers;
        });
    }
    pub fn progress(&self) -> TaskProgress {
        self.progress.borrow().clone()
    }
    pub fn activities(&self) -> ActivityHistory {
        self.activities.borrow().clone()
    }
    pub fn suspend_requested(&self) -> bool {
        self.controls.tx.borrow().paused()
    }
    pub fn suspend_acknowledged(&self) -> bool {
        !self.cancel.is_cancelled()
            && !matches!(
                self.state.borrow().clone(),
                RunState::Completed | RunState::Failed(_)
            )
            && self.controls.acknowledged()
    }
    pub fn suspend_reasons(&self) -> std::collections::BTreeSet<String> {
        self.controls.reasons()
    }
    pub fn set_policy_suspensions(&self, reasons: &std::collections::BTreeSet<String>) {
        self.controls.update(|current| {
            current.retain(|s| !s.starts_with("policy:"));
            current.extend(reasons.iter().map(|s| format!("policy:{s}")));
        });
    }
    pub fn recheck(&self) {
        self.controls.recheck.notify_one();
    }
    /// Cancellation-safe wait for acknowledgement, not merely request delivery.
    pub async fn wait_suspended(&self) -> Outcome<()> {
        let mut state = self.state.clone();
        let mut request = self.controls.tx.subscribe();
        loop {
            if self.cancel.is_cancelled() {
                return Err(Failure::Cancelled);
            }
            if !request.borrow_and_update().paused() {
                return Err(Failure::Rejected("pause request is not active".into()));
            }
            match state.borrow_and_update().clone() {
                RunState::Suspended if self.suspend_acknowledged() => return Ok(()),
                RunState::Completed | RunState::Failed(_) => {
                    return Err(Failure::Rejected(
                        "run ended before pause acknowledgement".into(),
                    ));
                }
                _ => {}
            }
            tokio::select! {
                _ = self.cancel.cancelled() => return Err(Failure::Cancelled),
                _ = self.controls.changed.notified() => {},
                changed = state.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
                changed = request.changed() => { changed.map_err(|_| Failure::Unavailable)?; }
            }
        }
    }
    pub fn cancel(&self) {
        self.controls
            .user_cancelled
            .store(true, std::sync::atomic::Ordering::SeqCst);
        self.controls.revoke();
        if self.persistent {
            self.controls.tx.send_modify(|r| r.generation += 1);
        } else {
            self.cancel.cancel();
        }
    }
    pub fn cancellation_requested(&self) -> bool {
        self.cancel.is_cancelled()
            || self
                .controls
                .user_cancelled
                .load(std::sync::atomic::Ordering::SeqCst)
    }
    /// True only after the Workflow body has been entered, not merely released
    /// from its queue. Schedulers must not replace a task before this boundary.
    pub fn has_started(&self) -> bool {
        self.controls
            .entered
            .load(std::sync::atomic::Ordering::SeqCst)
    }
    pub fn state(&self) -> RunState {
        if self.suspend_acknowledged() {
            RunState::Suspended
        } else {
            self.state.borrow().clone()
        }
    }
    pub async fn changed(&mut self) -> Outcome<RunState> {
        tokio::select! {
            r = self.state.changed() => { r.map_err(|_| Failure::Unavailable)?; },
            _ = self.controls.changed.notified() => {},
        }
        Ok(self.state())
    }
    pub async fn signal(&self, signal: Signal) -> Outcome<()> {
        if signal.name.trim().is_empty()
            || signal.name.len().saturating_add(signal.payload.len()) > self.max_signal_bytes
        {
            return Err(Failure::Rejected("invalid or oversized signal".into()));
        }
        self.signals
            .send(signal)
            .await
            .map_err(|_| Failure::Unavailable)
    }
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
    pub entered: Arc<std::sync::atomic::AtomicBool>,
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
            entered: Arc::default(),
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
