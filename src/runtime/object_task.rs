use super::TaskControl;
use std::{
    collections::{HashMap, VecDeque},
    hash::Hash,
    sync::{Arc, Mutex},
};
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictDecision {
    Join,
    Queue,
    QueueAndCancel { cause: String },
}

pub struct ObjectActivity<'a, I> {
    active: &'a I,
    pending: Vec<&'a I>,
    cancelling: bool,
}

impl<'a, I> ObjectActivity<'a, I> {
    pub fn active(&self) -> &'a I {
        self.active
    }

    pub fn latest(&self) -> &'a I {
        self.pending.last().copied().unwrap_or(self.active)
    }

    pub fn pending(&self) -> &[&'a I] {
        &self.pending
    }

    pub fn is_cancelling(&self) -> bool {
        self.cancelling
    }
}

pub enum ObjectAdmission<K, I, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    Joined,
    Active(ObjectLease<K, I, E>),
    Pending(ObjectPending<K, I, E>),
}

/// Reusable, in-process object mailbox without one Tokio task per object.
/// Every submitted handler remains a Future polled by the owning service root.
pub struct ObjectTaskCoordinator<K, I, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    inner: Arc<CoordinatorInner<K, I, E>>,
}

impl<K, I, E> Clone for ObjectTaskCoordinator<K, I, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct CoordinatorInner<K, I, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    state: Mutex<CoordinatorState<K, I, E>>,
    dropped_error: E,
}

struct CoordinatorState<K, I, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    next_generation: u64,
    slots: HashMap<K, ObjectSlot<I, K, E>>,
    waiters: HashMap<K, Vec<oneshot::Sender<Result<(), E>>>>,
    last_results: HashMap<K, Result<(), E>>,
}

struct ObjectSlot<I, K, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    generation: u64,
    active: I,
    cancellation: CancellationToken,
    task: Option<TaskControl>,
    pending: VecDeque<PendingEntry<K, I, E>>,
}

struct PendingEntry<K, I, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    input: I,
    service_cancellation: CancellationToken,
    ready: oneshot::Sender<Option<ObjectLease<K, I, E>>>,
}

impl<K, I, E> ObjectTaskCoordinator<K, I, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    pub fn new(dropped_error: E) -> Self {
        Self {
            inner: Arc::new(CoordinatorInner {
                state: Mutex::new(CoordinatorState {
                    next_generation: 1,
                    slots: HashMap::new(),
                    waiters: HashMap::new(),
                    last_results: HashMap::new(),
                }),
                dropped_error,
            }),
        }
    }

    /// Atomically evaluates domain conflict policy against one occupied slot.
    /// The closure contains business meaning; this type only applies the
    /// resulting join/queue/cancel mechanics.
    pub fn admit(
        &self,
        key: K,
        input: I,
        service_cancellation: &CancellationToken,
        decide: impl FnOnce(ObjectActivity<'_, I>, &I) -> ConflictDecision,
    ) -> ObjectAdmission<K, I, E> {
        let mut state = self.inner.state.lock().expect("object tasks poisoned");
        let Some(slot) = state.slots.get_mut(&key) else {
            let lease = Self::activate(
                &self.inner,
                &mut state,
                key,
                input,
                service_cancellation,
                VecDeque::new(),
            );
            return ObjectAdmission::Active(lease);
        };

        let activity = ObjectActivity {
            active: &slot.active,
            pending: slot.pending.iter().map(|entry| &entry.input).collect(),
            cancelling: slot.cancellation.is_cancelled(),
        };
        match decide(activity, &input) {
            ConflictDecision::Join => ObjectAdmission::Joined,
            decision @ (ConflictDecision::Queue | ConflictDecision::QueueAndCancel { .. }) => {
                let (ready, receiver) = oneshot::channel();
                slot.pending.push_back(PendingEntry {
                    input,
                    service_cancellation: service_cancellation.clone(),
                    ready,
                });
                if let ConflictDecision::QueueAndCancel { cause } = decision {
                    if let Some(task) = &slot.task {
                        task.request_cancel(cause);
                    } else {
                        slot.cancellation.cancel();
                    }
                }
                ObjectAdmission::Pending(ObjectPending { receiver })
            }
        }
    }

    pub async fn wait_idle(&self, key: K) -> Result<(), E> {
        let response = {
            let mut state = self.inner.state.lock().expect("object tasks poisoned");
            if !state.slots.contains_key(&key) {
                return state.last_results.get(&key).cloned().unwrap_or(Ok(()));
            }
            let (reply, response) = oneshot::channel();
            state.waiters.entry(key).or_default().push(reply);
            response
        };
        response
            .await
            .expect("the service root owns object idle waiters")
    }

    pub fn active_count(&self) -> usize {
        self.inner
            .state
            .lock()
            .expect("object tasks poisoned")
            .slots
            .len()
    }

    /// Reports whether this object currently has an active or queued intent.
    /// Domain code can use this as an admission guard for a different
    /// operation that must not race an object workflow.
    pub fn is_active(&self, key: &K) -> bool {
        self.inner
            .state
            .lock()
            .expect("object tasks poisoned")
            .slots
            .contains_key(key)
    }

    fn activate(
        inner: &Arc<CoordinatorInner<K, I, E>>,
        state: &mut CoordinatorState<K, I, E>,
        key: K,
        input: I,
        service_cancellation: &CancellationToken,
        pending: VecDeque<PendingEntry<K, I, E>>,
    ) -> ObjectLease<K, I, E> {
        let generation = state.next_generation;
        state.next_generation += 1;
        let cancellation = service_cancellation.child_token();
        state.last_results.remove(&key);
        state.slots.insert(
            key.clone(),
            ObjectSlot {
                generation,
                active: input.clone(),
                cancellation: cancellation.clone(),
                task: None,
                pending,
            },
        );
        ObjectLease {
            inner: inner.clone(),
            key,
            generation,
            input,
            cancellation,
            finished: false,
        }
    }
}

pub struct ObjectPending<K, I, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    receiver: oneshot::Receiver<Option<ObjectLease<K, I, E>>>,
}

impl<K, I, E> ObjectPending<K, I, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    /// Returns `None` when service stop discarded this queued operation before
    /// it became active.
    pub async fn activate(self) -> Option<ObjectLease<K, I, E>> {
        self.receiver.await.ok().flatten()
    }
}

pub struct ObjectLease<K, I, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    inner: Arc<CoordinatorInner<K, I, E>>,
    key: K,
    generation: u64,
    input: I,
    cancellation: CancellationToken,
    finished: bool,
}

impl<K, I, E> ObjectLease<K, I, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    pub fn input(&self) -> &I {
        &self.input
    }

    pub fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    pub fn bind_task(&self, task: TaskControl) {
        let mut state = self.inner.state.lock().expect("object tasks poisoned");
        if let Some(slot) = state.slots.get_mut(&self.key)
            && slot.generation == self.generation
        {
            slot.task = Some(task);
        }
    }

    pub fn finish(mut self, result: Result<(), E>) {
        self.release(Some(result));
        self.finished = true;
    }

    fn release(&mut self, result: Option<Result<(), E>>) {
        let mut state = self.inner.state.lock().expect("object tasks poisoned");
        let Some(slot) = state.slots.remove(&self.key) else {
            return;
        };
        if slot.generation != self.generation {
            state.slots.insert(self.key.clone(), slot);
            return;
        }

        let mut pending = slot.pending;
        while let Some(next) = pending.pop_front() {
            if next.service_cancellation.is_cancelled() {
                let _ = next.ready.send(None);
                continue;
            }
            let generation = state.next_generation;
            state.next_generation += 1;
            let cancellation = next.service_cancellation.child_token();
            let input = next.input;
            let lease = ObjectLease {
                inner: self.inner.clone(),
                key: self.key.clone(),
                generation,
                input: input.clone(),
                cancellation: cancellation.clone(),
                finished: false,
            };
            match next.ready.send(Some(lease)) {
                Ok(()) => {
                    state.last_results.remove(&self.key);
                    state.slots.insert(
                        self.key.clone(),
                        ObjectSlot {
                            generation,
                            active: input,
                            cancellation,
                            task: None,
                            pending,
                        },
                    );
                    return;
                }
                Err(Some(mut abandoned)) => {
                    abandoned.finished = true;
                }
                Err(None) => {}
            }
        }

        if let Some(result) = result {
            state.last_results.insert(self.key.clone(), result.clone());
            if let Some(waiters) = state.waiters.remove(&self.key) {
                for waiter in waiters {
                    let _ = waiter.send(result.clone());
                }
            }
        }
    }
}

impl<K, I, E> Drop for ObjectLease<K, I, E>
where
    K: Clone + Eq + Hash,
    I: Clone,
    E: Clone,
{
    fn drop(&mut self) {
        if !self.finished {
            let error = self.inner.dropped_error.clone();
            self.release(Some(Err(error)));
        }
    }
}
