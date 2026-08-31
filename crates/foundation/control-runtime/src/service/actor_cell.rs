use super::container::{Reply, ResponseOf};
use crate::client::BusinessEnvelope;
use crate::{
    CancelCause, CancellationScope, ObjectKey, OperationId, RuntimeError, Service, TaskAttemptId,
    WorkflowContext, WorkflowMeta,
};
use futures::future::join_all;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;

pub(crate) struct Subscriber<R> {
    operation_id: OperationId,
    cancellation: CancellationScope,
    pub(crate) reply: Reply<R>,
}

impl<R> Subscriber<R> {
    fn new(context: &WorkflowContext, reply: Reply<R>) -> Self {
        Self {
            operation_id: context.operation_id(),
            cancellation: context.cancellation().clone(),
            reply,
        }
    }
}

pub(crate) struct Intent<S: Service> {
    pub(crate) origin: WorkflowContext,
    pub(crate) request: S::Request,
    pub(crate) meta: WorkflowMeta<S::WorkflowKind>,
    pub(crate) subscribers: Vec<Subscriber<ResponseOf<S>>>,
}

impl<S: Service> Intent<S> {
    pub(crate) fn new(
        envelope: BusinessEnvelope<S::Request>,
        meta: WorkflowMeta<S::WorkflowKind>,
    ) -> Self {
        let BusinessEnvelope {
            context,
            request,
            reply,
        } = envelope;
        let subscriber = Subscriber::new(&context, reply);
        Self {
            origin: context,
            request,
            meta,
            subscribers: vec![subscriber],
        }
    }

    pub(crate) fn operation_id(&self) -> OperationId {
        self.origin.operation_id()
    }
}

#[derive(Clone)]
pub(crate) struct SubscriberGroup {
    scopes: Arc<Mutex<Vec<CancellationScope>>>,
    changed: Arc<Notify>,
}

impl SubscriberGroup {
    fn new<R>(subscribers: &[Subscriber<R>]) -> Self {
        Self {
            scopes: Arc::new(Mutex::new(
                subscribers
                    .iter()
                    .map(|subscriber| subscriber.cancellation.clone())
                    .collect(),
            )),
            changed: Arc::new(Notify::new()),
        }
    }

    fn add<R>(&self, subscribers: &[Subscriber<R>]) {
        self.scopes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .extend(
                subscribers
                    .iter()
                    .map(|subscriber| subscriber.cancellation.clone()),
            );
        self.changed.notify_waiters();
    }

    pub(crate) async fn all_cancelled(&self) {
        loop {
            let changed = self.changed.notified();
            let scopes = self
                .scopes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone();

            if !scopes.is_empty() && scopes.iter().all(CancellationScope::is_requested) {
                return;
            }

            tokio::select! {
                _ = changed => {}
                _ = join_all(scopes.iter().map(CancellationScope::requested)) => {}
            }
        }
    }
}

struct ActiveIntent<S: Service> {
    meta: WorkflowMeta<S::WorkflowKind>,
    task_attempt_id: TaskAttemptId,
    context: WorkflowContext,
    subscribers: Vec<Subscriber<ResponseOf<S>>>,
    subscriber_group: SubscriberGroup,
}

enum ActorPhase<S: Service> {
    Idle,
    Pending(Box<Intent<S>>),
    Running(Box<ActiveIntent<S>>),
    Cancelling {
        active: Box<ActiveIntent<S>>,
        replacement: Box<Intent<S>>,
    },
}

/// Hidden runtime state for one logical domain object.
///
/// Domain code only declares the desired workflow. This cell serializes the
/// object, joins equal workflows, settles replacements and owns subscribers.
/// It is driven by the service root task and creates no per-object Tokio task.
pub(crate) struct ActorCell<S: Service> {
    key: ObjectKey,
    phase: ActorPhase<S>,
    queued: VecDeque<Intent<S>>,
}

pub(crate) struct CancelNotice {
    pub(crate) task_attempt_id: TaskAttemptId,
    pub(crate) operation_id: OperationId,
}

pub(crate) struct JoinNotice {
    pub(crate) active_operation_id: OperationId,
    pub(crate) joined_operation_ids: Vec<OperationId>,
}

pub(crate) enum ReplaceOutcome<R> {
    CancelRequested(CancelNotice),
    ReplacementSuperseded(Vec<Subscriber<R>>),
}

pub(crate) struct Settled<R> {
    pub(crate) subscribers: Vec<Subscriber<R>>,
    pub(crate) ready: bool,
}

impl<S: Service> ActorCell<S> {
    pub(crate) fn new(key: ObjectKey) -> Self {
        Self {
            key,
            phase: ActorPhase::Idle,
            queued: VecDeque::new(),
        }
    }

    pub(crate) fn targets(&self, kind: &S::WorkflowKind) -> bool {
        match &self.phase {
            ActorPhase::Idle => false,
            ActorPhase::Pending(pending) => &pending.meta.kind == kind,
            ActorPhase::Running(active) => &active.meta.kind == kind,
            ActorPhase::Cancelling { replacement, .. } => &replacement.meta.kind == kind,
        }
    }

    pub(crate) fn is_idle(&self) -> bool {
        matches!(self.phase, ActorPhase::Idle)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.is_idle() && self.queued.is_empty()
    }

    pub(crate) fn is_active(&self) -> bool {
        matches!(
            self.phase,
            ActorPhase::Running(_) | ActorPhase::Cancelling { .. }
        )
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.queued.len()
            + usize::from(matches!(
                self.phase,
                ActorPhase::Pending(_) | ActorPhase::Cancelling { .. }
            ))
    }

    /// Queue an intent and report whether it is ready to be launched now.
    pub(crate) fn queue(&mut self, intent: Intent<S>) -> bool {
        if self.is_idle() {
            self.phase = ActorPhase::Pending(Box::new(intent));
            true
        } else {
            self.queued.push_back(intent);
            false
        }
    }

    pub(crate) fn take_ready(&mut self) -> Option<Intent<S>> {
        let phase = std::mem::replace(&mut self.phase, ActorPhase::Idle);
        match phase {
            ActorPhase::Pending(intent) => Some(*intent),
            other => {
                self.phase = other;
                None
            }
        }
    }

    pub(crate) fn activate(
        &mut self,
        meta: WorkflowMeta<S::WorkflowKind>,
        task_attempt_id: TaskAttemptId,
        context: WorkflowContext,
        subscribers: Vec<Subscriber<ResponseOf<S>>>,
    ) -> Result<SubscriberGroup, RuntimeError> {
        if !self.is_idle() {
            return Err(RuntimeError::Internal(format!(
                "actor cell {} was activated while busy",
                self.key
            )));
        }
        let subscriber_group = SubscriberGroup::new(&subscribers);
        self.phase = ActorPhase::Running(Box::new(ActiveIntent {
            meta,
            task_attempt_id,
            context,
            subscribers,
            subscriber_group: subscriber_group.clone(),
        }));
        Ok(subscriber_group)
    }

    pub(crate) fn join(&mut self, mut intent: Intent<S>) -> Result<JoinNotice, Box<Intent<S>>> {
        let joined_operation_ids = intent
            .subscribers
            .iter()
            .map(|subscriber| subscriber.operation_id)
            .collect();

        let active_operation_id = match &mut self.phase {
            ActorPhase::Pending(pending) if pending.meta.kind == intent.meta.kind => {
                pending.subscribers.append(&mut intent.subscribers);
                pending.operation_id()
            }
            ActorPhase::Running(active) if active.meta.kind == intent.meta.kind => {
                active.subscriber_group.add(&intent.subscribers);
                active.subscribers.append(&mut intent.subscribers);
                active.context.operation_id()
            }
            ActorPhase::Cancelling { replacement, .. }
                if replacement.meta.kind == intent.meta.kind =>
            {
                replacement.subscribers.append(&mut intent.subscribers);
                replacement.operation_id()
            }
            ActorPhase::Idle
            | ActorPhase::Pending(_)
            | ActorPhase::Running(_)
            | ActorPhase::Cancelling { .. } => return Err(Box::new(intent)),
        };

        Ok(JoinNotice {
            active_operation_id,
            joined_operation_ids,
        })
    }

    pub(crate) fn replace(
        &mut self,
        intent: Intent<S>,
        cause: CancelCause,
    ) -> Result<ReplaceOutcome<ResponseOf<S>>, Box<Intent<S>>> {
        let phase = std::mem::replace(&mut self.phase, ActorPhase::Idle);
        match phase {
            ActorPhase::Running(active) => {
                active.context.cancellation().request(cause);
                let notice = CancelNotice {
                    task_attempt_id: active.task_attempt_id,
                    operation_id: active.context.operation_id(),
                };
                self.phase = ActorPhase::Cancelling {
                    active,
                    replacement: Box::new(intent),
                };
                Ok(ReplaceOutcome::CancelRequested(notice))
            }
            ActorPhase::Cancelling {
                active,
                replacement,
            } => {
                let superseded = replacement.subscribers;
                self.phase = ActorPhase::Cancelling {
                    active,
                    replacement: Box::new(intent),
                };
                Ok(ReplaceOutcome::ReplacementSuperseded(superseded))
            }
            ActorPhase::Pending(pending) => {
                let superseded = pending.subscribers;
                self.phase = ActorPhase::Pending(Box::new(intent));
                Ok(ReplaceOutcome::ReplacementSuperseded(superseded))
            }
            ActorPhase::Idle => {
                self.phase = ActorPhase::Idle;
                Err(Box::new(intent))
            }
        }
    }

    pub(crate) fn settle(
        &mut self,
        task_attempt_id: TaskAttemptId,
    ) -> Option<Settled<ResponseOf<S>>> {
        let phase = std::mem::replace(&mut self.phase, ActorPhase::Idle);
        match phase {
            ActorPhase::Running(active) if active.task_attempt_id == task_attempt_id => {
                let ready = self.promote_queued();
                Some(Settled {
                    subscribers: active.subscribers,
                    ready,
                })
            }
            ActorPhase::Cancelling {
                active,
                replacement,
            } if active.task_attempt_id == task_attempt_id => {
                self.phase = ActorPhase::Pending(replacement);
                Some(Settled {
                    subscribers: active.subscribers,
                    ready: true,
                })
            }
            other => {
                self.phase = other;
                None
            }
        }
    }

    pub(crate) fn request_cancel(&self, cause: CancelCause) -> Option<CancelNotice> {
        let active = match &self.phase {
            ActorPhase::Running(active) | ActorPhase::Cancelling { active, .. } => active,
            ActorPhase::Idle | ActorPhase::Pending(_) => return None,
        };
        active.context.cancellation().request(cause);
        Some(CancelNotice {
            task_attempt_id: active.task_attempt_id,
            operation_id: active.context.operation_id(),
        })
    }

    pub(crate) fn reject_pending(&mut self) -> Vec<Subscriber<ResponseOf<S>>> {
        let mut subscribers = Vec::new();
        while let Some(intent) = self.queued.pop_front() {
            subscribers.extend(intent.subscribers);
        }
        let phase = std::mem::replace(&mut self.phase, ActorPhase::Idle);
        match phase {
            ActorPhase::Pending(intent) => subscribers.extend(intent.subscribers),
            ActorPhase::Cancelling {
                active,
                replacement,
            } => {
                subscribers.extend(replacement.subscribers);
                self.phase = ActorPhase::Running(active);
            }
            ActorPhase::Running(active) => self.phase = ActorPhase::Running(active),
            ActorPhase::Idle => {}
        }
        subscribers
    }

    fn promote_queued(&mut self) -> bool {
        self.phase = match self.queued.pop_front() {
            Some(intent) => ActorPhase::Pending(Box::new(intent)),
            None => ActorPhase::Idle,
        };
        !self.is_idle()
    }
}
