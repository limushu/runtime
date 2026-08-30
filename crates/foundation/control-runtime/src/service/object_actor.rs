use super::container::{Reply, ResponseOf};
use crate::router::BusinessEnvelope;
use crate::{
    CancelCause, ObjectActivity, ObjectKey, OperationId, RuntimeError, Service, TaskAttemptId,
    WorkflowContext, WorkflowMeta,
};
use std::collections::VecDeque;

pub(crate) struct Intent<S: Service> {
    pub(crate) context: WorkflowContext,
    pub(crate) request: S::Request,
    pub(crate) meta: WorkflowMeta<S::WorkflowKind>,
    pub(crate) waiters: Vec<Reply<ResponseOf<S>>>,
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
        Self {
            context,
            request,
            meta,
            waiters: vec![reply],
        }
    }
}

struct ActiveIntent<S: Service> {
    meta: WorkflowMeta<S::WorkflowKind>,
    task_attempt_id: TaskAttemptId,
    context: WorkflowContext,
    waiters: Vec<Reply<ResponseOf<S>>>,
}

enum ObjectPhase<S: Service> {
    Idle,
    Pending(Box<Intent<S>>),
    Running(Box<ActiveIntent<S>>),
    Cancelling {
        active: Box<ActiveIntent<S>>,
        replacement: Box<Intent<S>>,
    },
}

pub(crate) struct ObjectActor<S: Service> {
    key: ObjectKey,
    phase: ObjectPhase<S>,
    queued: VecDeque<Intent<S>>,
}

pub(crate) struct CancelNotice {
    pub(crate) task_attempt_id: TaskAttemptId,
    pub(crate) operation_id: OperationId,
}

pub(crate) enum ReplaceOutcome<R> {
    CancelRequested(CancelNotice),
    ReplacementSuperseded(Vec<Reply<R>>),
}

pub(crate) struct Settled<R> {
    pub(crate) waiters: Vec<Reply<R>>,
}

impl<S: Service> ObjectActor<S> {
    pub(crate) fn new(key: ObjectKey) -> Self {
        Self {
            key,
            phase: ObjectPhase::Idle,
            queued: VecDeque::new(),
        }
    }

    pub(crate) fn activity(&self) -> ObjectActivity<S::WorkflowKind> {
        match &self.phase {
            ObjectPhase::Idle => ObjectActivity::Idle,
            ObjectPhase::Pending(intent) => ObjectActivity::Pending {
                operation_id: intent.context.operation_id(),
                kind: intent.meta.kind.clone(),
            },
            ObjectPhase::Running(active) => ObjectActivity::Running {
                operation_id: active.context.operation_id(),
                kind: active.meta.kind.clone(),
            },
            ObjectPhase::Cancelling {
                active,
                replacement,
            } => ObjectActivity::Cancelling {
                current_operation_id: active.context.operation_id(),
                current_kind: active.meta.kind.clone(),
                replacement_operation_id: replacement.context.operation_id(),
                replacement_kind: replacement.meta.kind.clone(),
            },
        }
    }

    pub(crate) fn is_idle(&self) -> bool {
        matches!(self.phase, ObjectPhase::Idle)
    }

    pub(crate) fn is_active(&self) -> bool {
        matches!(
            self.phase,
            ObjectPhase::Running(_) | ObjectPhase::Cancelling { .. }
        )
    }

    pub(crate) fn pending_count(&self) -> usize {
        self.queued.len()
            + usize::from(matches!(
                self.phase,
                ObjectPhase::Pending(_) | ObjectPhase::Cancelling { .. }
            ))
    }

    pub(crate) fn has_ready(&self) -> bool {
        matches!(self.phase, ObjectPhase::Pending(_))
    }

    pub(crate) fn queue(&mut self, intent: Intent<S>) {
        if self.is_idle() {
            self.phase = ObjectPhase::Pending(Box::new(intent));
        } else {
            self.queued.push_back(intent);
        }
    }

    pub(crate) fn take_ready(&mut self) -> Option<Intent<S>> {
        let phase = std::mem::replace(&mut self.phase, ObjectPhase::Idle);
        match phase {
            ObjectPhase::Pending(intent) => Some(*intent),
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
        waiters: Vec<Reply<ResponseOf<S>>>,
    ) -> Result<(), RuntimeError> {
        if !self.is_idle() {
            return Err(RuntimeError::Internal(format!(
                "object actor {} was activated while busy",
                self.key
            )));
        }
        self.phase = ObjectPhase::Running(Box::new(ActiveIntent {
            meta,
            task_attempt_id,
            context,
            waiters,
        }));
        Ok(())
    }

    pub(crate) fn join_current(
        &mut self,
        waiters: Vec<Reply<ResponseOf<S>>>,
    ) -> Result<OperationId, Vec<Reply<ResponseOf<S>>>> {
        let active = match &mut self.phase {
            ObjectPhase::Running(active) => active,
            ObjectPhase::Idle | ObjectPhase::Pending(_) | ObjectPhase::Cancelling { .. } => {
                return Err(waiters)
            }
        };
        active.waiters.extend(waiters);
        Ok(active.context.operation_id())
    }

    pub(crate) fn join_pending(
        &mut self,
        waiters: Vec<Reply<ResponseOf<S>>>,
    ) -> Result<OperationId, Vec<Reply<ResponseOf<S>>>> {
        let ObjectPhase::Pending(intent) = &mut self.phase else {
            return Err(waiters);
        };
        intent.waiters.extend(waiters);
        Ok(intent.context.operation_id())
    }

    pub(crate) fn join_replacement(
        &mut self,
        waiters: Vec<Reply<ResponseOf<S>>>,
    ) -> Result<OperationId, Vec<Reply<ResponseOf<S>>>> {
        let ObjectPhase::Cancelling { replacement, .. } = &mut self.phase else {
            return Err(waiters);
        };
        replacement.waiters.extend(waiters);
        Ok(replacement.context.operation_id())
    }

    pub(crate) fn replace(
        &mut self,
        intent: Intent<S>,
        cause: CancelCause,
    ) -> Result<ReplaceOutcome<ResponseOf<S>>, Box<Intent<S>>> {
        let phase = std::mem::replace(&mut self.phase, ObjectPhase::Idle);
        match phase {
            ObjectPhase::Running(active) => {
                active.context.cancellation().request(cause);
                let notice = CancelNotice {
                    task_attempt_id: active.task_attempt_id,
                    operation_id: active.context.operation_id(),
                };
                self.phase = ObjectPhase::Cancelling {
                    active,
                    replacement: Box::new(intent),
                };
                Ok(ReplaceOutcome::CancelRequested(notice))
            }
            ObjectPhase::Cancelling {
                active,
                replacement,
            } => {
                let superseded = replacement.waiters;
                self.phase = ObjectPhase::Cancelling {
                    active,
                    replacement: Box::new(intent),
                };
                Ok(ReplaceOutcome::ReplacementSuperseded(superseded))
            }
            ObjectPhase::Pending(pending) => {
                let superseded = pending.waiters;
                self.phase = ObjectPhase::Pending(Box::new(intent));
                Ok(ReplaceOutcome::ReplacementSuperseded(superseded))
            }
            ObjectPhase::Idle => {
                self.phase = ObjectPhase::Idle;
                Err(Box::new(intent))
            }
        }
    }

    pub(crate) fn settle(
        &mut self,
        task_attempt_id: TaskAttemptId,
    ) -> Option<Settled<ResponseOf<S>>> {
        let phase = std::mem::replace(&mut self.phase, ObjectPhase::Idle);
        match phase {
            ObjectPhase::Running(active) if active.task_attempt_id == task_attempt_id => {
                self.promote_queued();
                Some(Settled {
                    waiters: active.waiters,
                })
            }
            ObjectPhase::Cancelling {
                active,
                replacement,
            } if active.task_attempt_id == task_attempt_id => {
                self.phase = ObjectPhase::Pending(replacement);
                Some(Settled {
                    waiters: active.waiters,
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
            ObjectPhase::Running(active) | ObjectPhase::Cancelling { active, .. } => active,
            ObjectPhase::Idle | ObjectPhase::Pending(_) => return None,
        };
        active.context.cancellation().request(cause);
        Some(CancelNotice {
            task_attempt_id: active.task_attempt_id,
            operation_id: active.context.operation_id(),
        })
    }

    pub(crate) fn reject_pending(&mut self) -> Vec<Reply<ResponseOf<S>>> {
        let mut waiters = Vec::new();
        while let Some(intent) = self.queued.pop_front() {
            waiters.extend(intent.waiters);
        }
        let phase = std::mem::replace(&mut self.phase, ObjectPhase::Idle);
        match phase {
            ObjectPhase::Pending(intent) => waiters.extend(intent.waiters),
            ObjectPhase::Cancelling {
                active,
                replacement,
            } => {
                waiters.extend(replacement.waiters);
                self.phase = ObjectPhase::Running(active);
            }
            ObjectPhase::Running(active) => self.phase = ObjectPhase::Running(active),
            ObjectPhase::Idle => {}
        }
        waiters
    }

    fn promote_queued(&mut self) {
        self.phase = match self.queued.pop_front() {
            Some(intent) => ObjectPhase::Pending(Box::new(intent)),
            None => ObjectPhase::Idle,
        };
    }
}
