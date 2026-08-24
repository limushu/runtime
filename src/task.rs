use std::{collections::HashMap, future::Future};

use futures::{
    FutureExt,
    future::{AbortHandle, Abortable, BoxFuture},
    stream::{FuturesUnordered, StreamExt},
};
use tokio::sync::watch;

use crate::{
    Message, MessageContext, MessagePayload, OperationId, Router, RuntimeError, ServiceKey,
    TaskEvent, TaskId, TaskKey, TaskState, TaskVisibility, TraceContext,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CancelMode {
    Cooperative,
    Force,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CancelReason {
    Requested(String),
    ServiceStopping,
    NoLongerNeeded,
}

#[derive(Debug)]
pub enum TaskOutcome<T> {
    Completed(T),
    Failed(RuntimeError),
    Cancelled(CancelReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunOutcome {
    Started(TaskId),
    AlreadyRunning(TaskId),
}

pub struct TaskContext<K, M>
where
    K: ServiceKey,
    M: Message,
{
    task_id: TaskId,
    key: TaskKey,
    message: MessageContext,
    router: Router<K, M>,
    cancellation: watch::Receiver<Option<CancelReason>>,
}

impl<K, M> TaskContext<K, M>
where
    K: ServiceKey,
    M: Message,
{
    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub fn key(&self) -> &TaskKey {
        &self.key
    }

    pub fn operation_id(&self) -> OperationId {
        self.message.operation_id
    }

    pub fn trace(&self) -> TraceContext {
        self.message.trace.child(self.task_id)
    }

    pub fn cancellation_reason(&self) -> Option<CancelReason> {
        self.cancellation.borrow().clone()
    }

    pub async fn cancelled(&mut self) -> CancelReason {
        loop {
            if let Some(reason) = self.cancellation.borrow().clone() {
                return reason;
            }
            if self.cancellation.changed().await.is_err() {
                return CancelReason::ServiceStopping;
            }
        }
    }

    pub async fn send<P>(&self, target: K, payload: P) -> Result<(), RuntimeError>
    where
        P: MessagePayload<M>,
    {
        self.router
            .send_payload(
                target,
                payload,
                MessageContext::new(self.message.operation_id, self.trace()),
            )
            .await
    }
}

struct TaskSlot {
    task_id: TaskId,
    cancel: watch::Sender<Option<CancelReason>>,
    abort: AbortHandle,
}

pub(crate) struct TaskCompletion<M>
where
    M: Message,
{
    pub task_id: TaskId,
    pub key: TaskKey,
    pub label: String,
    pub visibility: TaskVisibility,
    pub context: MessageContext,
    pub state: TaskState,
    pub message: M,
}

pub(crate) struct TaskSet<K, M>
where
    K: ServiceKey,
    M: Message,
{
    service: K,
    router: Router<K, M>,
    next_task_id: u64,
    slots: HashMap<TaskKey, TaskSlot>,
    futures: FuturesUnordered<BoxFuture<'static, TaskCompletion<M>>>,
    events: tokio::sync::broadcast::Sender<TaskEvent<K>>,
}

impl<K, M> TaskSet<K, M>
where
    K: ServiceKey,
    M: Message,
{
    pub fn new(
        service: K,
        router: Router<K, M>,
        events: tokio::sync::broadcast::Sender<TaskEvent<K>>,
    ) -> Self {
        Self {
            service,
            router,
            next_task_id: 0,
            slots: HashMap::new(),
            futures: FuturesUnordered::new(),
            events,
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn run<T, Fut, Work, Complete>(
        &mut self,
        key: TaskKey,
        label: impl Into<String>,
        visibility: TaskVisibility,
        context: MessageContext,
        work: Work,
        complete: Complete,
    ) -> RunOutcome
    where
        T: Send + 'static,
        Fut: Future<Output = Result<T, RuntimeError>> + Send + 'static,
        Work: FnOnce(TaskContext<K, M>) -> Fut + Send + 'static,
        Complete: FnOnce(TaskOutcome<T>) -> M + Send + 'static,
    {
        if let Some(slot) = self.slots.get(&key) {
            return RunOutcome::AlreadyRunning(slot.task_id);
        }

        self.next_task_id += 1;
        let task_id = TaskId(self.next_task_id);
        let label = label.into();
        let (cancel_tx, cancel_rx) = watch::channel(None);
        let (abort, registration) = AbortHandle::new_pair();
        let task_context = TaskContext {
            task_id,
            key: key.clone(),
            message: context,
            router: self.router.clone(),
            cancellation: cancel_rx.clone(),
        };
        let future_key = key.clone();
        let future_label = label.clone();
        let future = async move {
            let result = Abortable::new(work(task_context), registration).await;
            let outcome = match result {
                Ok(Ok(value)) if cancel_rx.borrow().is_none() => TaskOutcome::Completed(value),
                Ok(Ok(_)) => TaskOutcome::Cancelled(
                    cancel_rx
                        .borrow()
                        .clone()
                        .unwrap_or(CancelReason::ServiceStopping),
                ),
                Ok(Err(error)) if cancel_rx.borrow().is_none() => TaskOutcome::Failed(error),
                Ok(Err(_)) | Err(_) => TaskOutcome::Cancelled(
                    cancel_rx
                        .borrow()
                        .clone()
                        .unwrap_or(CancelReason::ServiceStopping),
                ),
            };
            let state = match &outcome {
                TaskOutcome::Completed(_) => TaskState::Completed,
                TaskOutcome::Failed(_) => TaskState::Failed,
                TaskOutcome::Cancelled(_) => TaskState::Cancelled,
            };
            TaskCompletion {
                task_id,
                key: future_key,
                label: future_label,
                visibility,
                context,
                state,
                message: complete(outcome),
            }
        }
        .boxed();

        self.slots.insert(
            key.clone(),
            TaskSlot {
                task_id,
                cancel: cancel_tx,
                abort,
            },
        );
        self.futures.push(future);
        self.emit(task_id, key, label, TaskState::Started, visibility, context);
        RunOutcome::Started(task_id)
    }

    pub fn contains(&self, key: &TaskKey) -> bool {
        self.slots.contains_key(key)
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn cancel(&mut self, key: &TaskKey, mode: CancelMode, reason: CancelReason) -> bool {
        let Some(slot) = self.slots.get(key) else {
            return false;
        };
        let _ = slot.cancel.send(Some(reason));
        if mode == CancelMode::Force {
            slot.abort.abort();
        }
        true
    }

    pub fn cancel_all(&mut self, mode: CancelMode, reason: CancelReason) {
        for slot in self.slots.values() {
            let _ = slot.cancel.send(Some(reason.clone()));
            if mode == CancelMode::Force {
                slot.abort.abort();
            }
        }
    }

    pub async fn next_event(&mut self) -> Option<TaskCompletion<M>> {
        let completion = self.futures.next().await?;
        if self
            .slots
            .get(&completion.key)
            .is_some_and(|slot| slot.task_id == completion.task_id)
        {
            self.slots.remove(&completion.key);
        }
        self.emit(
            completion.task_id,
            completion.key.clone(),
            completion.label.clone(),
            completion.state,
            completion.visibility,
            completion.context,
        );
        Some(completion)
    }

    #[allow(clippy::too_many_arguments)]
    fn emit(
        &self,
        task_id: TaskId,
        key: TaskKey,
        label: String,
        state: TaskState,
        visibility: TaskVisibility,
        context: MessageContext,
    ) {
        let _ = self.events.send(TaskEvent {
            service: self.service.clone(),
            task_id,
            key,
            label,
            state,
            visibility,
            operation_id: context.operation_id,
            trace: context.trace.child(task_id),
        });
    }
}
