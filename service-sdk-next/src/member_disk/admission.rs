//! Domain resource protection, independent of application workflow admission.
use super::{DiskUuid, MemberDiskError};
use crate::runtime::{Lifecycle, workflow::WorkflowFailure};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use tokio::sync::{
    Notify, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, watch,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub(crate) struct Admission {
    shared: Arc<Mutex<State>>,
    space: Arc<RwLock<()>>,
    disks: Arc<Mutex<BTreeMap<DiskUuid, Arc<tokio::sync::Mutex<()>>>>>,
    stopped: CancellationToken,
    enabled: watch::Sender<bool>,
    changed: Arc<Notify>,
}

#[derive(Default)]
struct State {
    next: u64,
    active: BTreeMap<u64, CancellationToken>,
    blocked: BTreeSet<Option<DiskUuid>>,
    closed: bool,
}

// These async guards reserve resources, NOT the mutable domain catalog.
enum Locks {
    Disk {
        _space: OwnedRwLockReadGuard<()>,
        _disk: OwnedMutexGuard<()>,
    },
    Space {
        _space: OwnedRwLockWriteGuard<()>,
    },
}

pub(crate) struct Reservation {
    owner: Admission,
    id: u64,
    key: Option<DiskUuid>,
    pub cancel: CancellationToken,
    uncertain: bool,
    _locks: Locks,
}

impl Reservation {
    pub fn settled<T>(&mut self, result: &Result<T, MemberDiskError>) {
        self.uncertain = result.as_ref().is_err_and(|e| e.needs_recovery());
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut state = self.owner.shared.lock().unwrap();
        if self.uncertain {
            state.blocked.insert(self.key.clone());
        }
        state.active.remove(&self.id);
        self.owner.changed.notify_one();
    }
}

impl Admission {
    pub fn new(changed: Arc<Notify>) -> Self {
        Self {
            shared: Arc::new(Mutex::new(State::default())),
            space: Arc::new(RwLock::new(())),
            disks: Arc::new(Mutex::new(BTreeMap::new())),
            stopped: CancellationToken::new(),
            enabled: watch::channel(true).0,
            changed,
        }
    }

    pub async fn acquire(
        &self,
        key: Option<DiskUuid>,
        caller: &CancellationToken,
    ) -> Result<Reservation, MemberDiskError> {
        let mut enabled = self.enabled.subscribe();
        let locks = async {
            loop {
                while !*enabled.borrow_and_update() {
                    enabled
                        .changed()
                        .await
                        .map_err(|_| MemberDiskError::Cancelled)?;
                }
                let locks = if let Some(disk) = &key {
                    let gate = self
                        .disks
                        .lock()
                        .unwrap()
                        .entry(disk.clone())
                        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                        .clone();
                    Locks::Disk {
                        _space: self.space.clone().read_owned().await,
                        _disk: gate.lock_owned().await,
                    }
                } else {
                    Locks::Space {
                        _space: self.space.clone().write_owned().await,
                    }
                };
                if *enabled.borrow() {
                    break Ok(locks);
                }
                drop(locks);
            }
        };
        let locks = tokio::select! {
            biased;
            _ = caller.cancelled() => return Err(MemberDiskError::Cancelled),
            _ = self.stopped.cancelled() => return Err(MemberDiskError::Cancelled),
            result = locks => result?,
        };
        let mut state = self.shared.lock().unwrap();
        if state.closed || caller.is_cancelled() {
            return Err(MemberDiskError::Cancelled);
        }
        if state.blocked.contains(&None)
            || match &key {
                None => !state.blocked.is_empty(),
                Some(_) => state.blocked.contains(&key),
            }
        {
            return Err(MemberDiskError::NeedsRecovery(
                "domain resource requires reconciliation".into(),
            ));
        }
        state.next += 1;
        let id = state.next;
        let cancel = CancellationToken::new();
        state.active.insert(id, cancel.clone());
        self.changed.notify_one();
        Ok(Reservation {
            owner: self.clone(),
            id,
            key,
            cancel,
            uncertain: true,
            _locks: locks,
        })
    }

    pub fn lifecycle(&self, lifecycle: Lifecycle) {
        if matches!(lifecycle, Lifecycle::Stopping | Lifecycle::Stopped) {
            self.stop();
        }
        self.enabled.send_if_modified(|value| {
            let next = matches!(lifecycle, Lifecycle::Running | Lifecycle::Draining);
            if *value == next {
                false
            } else {
                *value = next;
                true
            }
        });
    }

    pub fn stop(&self) {
        let mut state = self.shared.lock().unwrap();
        if state.closed {
            return;
        }
        state.closed = true;
        self.stopped.cancel();
        for cancel in state.active.values() {
            cancel.cancel();
        }
        self.changed.notify_one();
    }

    pub fn is_idle(&self) -> bool {
        self.shared.lock().unwrap().active.is_empty()
    }
    pub fn blocked(&self) -> Vec<Option<DiskUuid>> {
        self.shared
            .lock()
            .unwrap()
            .blocked
            .iter()
            .cloned()
            .collect()
    }
}
