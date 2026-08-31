use crate::observation::ObservationHub;
use crate::{
    CallOutcome, ObservationEvent, RuntimeError, RuntimeResult, ServiceId, ServiceRequest,
    WorkflowContext,
};
use futures::future::join_all;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub(crate) struct BusinessEnvelope<R: ServiceRequest> {
    pub context: WorkflowContext,
    pub request: R,
    pub reply: oneshot::Sender<RuntimeResult<R::Response>>,
}

struct BusinessHandle<R: ServiceRequest> {
    service_id: ServiceId,
    sender: mpsc::Sender<BusinessEnvelope<R>>,
}

impl<R: ServiceRequest> Clone for BusinessHandle<R> {
    fn clone(&self) -> Self {
        Self {
            service_id: self.service_id.clone(),
            sender: self.sender.clone(),
        }
    }
}

impl<R: ServiceRequest> BusinessHandle<R> {
    async fn call(&self, context: WorkflowContext, request: R) -> RuntimeResult<R::Response> {
        let (reply, ticket) = oneshot::channel();
        self.sender
            .send(BusinessEnvelope {
                context,
                request,
                reply,
            })
            .await
            .map_err(|_| RuntimeError::ChannelClosed(self.service_id.clone()))?;
        ticket
            .await
            .map_err(|_| RuntimeError::ChannelClosed(self.service_id.clone()))?
    }
}

/// An explicit, typed address for exactly one service instance.
///
/// Domain-facing service facades wrap this type so workflow code calls named
/// capabilities rather than routing command enums by hidden request metadata.
pub struct ServiceClient<R: ServiceRequest> {
    service_id: ServiceId,
    handle: BusinessHandle<R>,
    observation: ObservationHub,
}

impl<R: ServiceRequest> Clone for ServiceClient<R> {
    fn clone(&self) -> Self {
        Self {
            service_id: self.service_id.clone(),
            handle: self.handle.clone(),
            observation: self.observation.clone(),
        }
    }
}

impl<R: ServiceRequest> ServiceClient<R> {
    pub(crate) fn channel(
        service_id: ServiceId,
        capacity: usize,
        observation: ObservationHub,
    ) -> (Self, mpsc::Receiver<BusinessEnvelope<R>>) {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        let handle = BusinessHandle {
            service_id: service_id.clone(),
            sender,
        };
        (
            Self {
                service_id,
                handle,
                observation,
            },
            receiver,
        )
    }

    pub fn service_id(&self) -> &ServiceId {
        &self.service_id
    }

    /// Call this explicit service as part of an existing operation.
    ///
    /// The child call is never dropped merely because cancellation was
    /// requested. Once the callee returns a stable result, cancellation is
    /// propagated as `Cancelled` before the parent workflow can start another
    /// step. Ordinary workflows therefore use normal `.await?` sequencing and
    /// do not poll cancellation tokens between calls.
    pub async fn call(
        &self,
        parent: &WorkflowContext,
        label: impl Into<Arc<str>>,
        request: R,
    ) -> RuntimeResult<R::Response> {
        if parent.cancellation().is_requested() {
            return Err(RuntimeError::Cancelled);
        }
        let child = parent
            .child(self.service_id.clone(), label)
            .with_observation(self.observation.clone());
        self.observation.publish(ObservationEvent::CallStarted {
            service_id: self.service_id.clone(),
            operation_id: child.operation_id(),
            call_id: child.call_id(),
            parent_call_id: child.parent_call_id(),
            parent_task_attempt_id: child.parent_task_attempt_id(),
        });
        let mut guard = CallGuard::new(
            child.clone(),
            self.service_id.clone(),
            self.observation.clone(),
        );
        let result = self.handle.call(child, request).await;
        guard.finish(call_outcome(&result));
        if result.is_ok() && parent.cancellation().is_requested() {
            Err(RuntimeError::Cancelled)
        } else {
            result
        }
    }

    pub async fn call_root(
        &self,
        label: impl Into<Arc<str>>,
        request: R,
    ) -> RuntimeResult<R::Response> {
        let context = WorkflowContext::root(self.service_id.clone(), label)
            .with_observation(self.observation.clone());
        self.observation
            .publish(ObservationEvent::OperationStarted {
                service_id: self.service_id.clone(),
                operation_id: context.operation_id(),
                call_id: context.call_id(),
                label: context.label().into(),
            });
        self.observation.publish(ObservationEvent::CallStarted {
            service_id: self.service_id.clone(),
            operation_id: context.operation_id(),
            call_id: context.call_id(),
            parent_call_id: None,
            parent_task_attempt_id: None,
        });
        let mut operation_guard = OperationGuard::new(
            context.clone(),
            self.service_id.clone(),
            self.observation.clone(),
        );
        let mut call_guard = CallGuard::new(
            context.clone(),
            self.service_id.clone(),
            self.observation.clone(),
        );
        let result = self.handle.call(context, request).await;
        let outcome = call_outcome(&result);
        call_guard.finish(outcome.clone());
        operation_guard.finish(outcome);
        result
    }

    pub async fn call_batch<I>(
        &self,
        label: impl Into<Arc<str>>,
        requests: I,
    ) -> RuntimeResult<Vec<RuntimeResult<R::Response>>>
    where
        I: IntoIterator<Item = R>,
    {
        let context = WorkflowContext::root(self.service_id.clone(), label)
            .with_observation(self.observation.clone());
        self.observation
            .publish(ObservationEvent::OperationStarted {
                service_id: self.service_id.clone(),
                operation_id: context.operation_id(),
                call_id: context.call_id(),
                label: context.label().into(),
            });
        let mut operation_guard = OperationGuard::new(
            context.clone(),
            self.service_id.clone(),
            self.observation.clone(),
        );
        let results = join_all(
            requests
                .into_iter()
                .map(|request| self.call(&context, self.service_id.as_str(), request)),
        )
        .await;
        operation_guard.finish(batch_outcome(&results));
        Ok(results)
    }
}

struct CallGuard {
    context: WorkflowContext,
    service_id: ServiceId,
    observation: ObservationHub,
    armed: bool,
}

impl CallGuard {
    fn new(context: WorkflowContext, service_id: ServiceId, observation: ObservationHub) -> Self {
        Self {
            context,
            service_id,
            observation,
            armed: true,
        }
    }

    fn finish(&mut self, outcome: CallOutcome) {
        self.observation.publish(ObservationEvent::CallFinished {
            service_id: self.service_id.clone(),
            operation_id: self.context.operation_id(),
            call_id: self.context.call_id(),
            outcome,
        });
        self.armed = false;
    }
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        if self.armed {
            self.context
                .cancellation()
                .request(crate::CancelCause::new("caller dropped service call"));
            self.observation.publish(ObservationEvent::CallFinished {
                service_id: self.service_id.clone(),
                operation_id: self.context.operation_id(),
                call_id: self.context.call_id(),
                outcome: CallOutcome::Cancelled,
            });
        }
    }
}

struct OperationGuard {
    context: WorkflowContext,
    service_id: ServiceId,
    observation: ObservationHub,
    armed: bool,
}

impl OperationGuard {
    fn new(context: WorkflowContext, service_id: ServiceId, observation: ObservationHub) -> Self {
        Self {
            context,
            service_id,
            observation,
            armed: true,
        }
    }

    fn finish(&mut self, outcome: CallOutcome) {
        self.observation
            .publish(ObservationEvent::OperationFinished {
                service_id: self.service_id.clone(),
                operation_id: self.context.operation_id(),
                outcome,
            });
        self.armed = false;
    }
}

impl Drop for OperationGuard {
    fn drop(&mut self) {
        if self.armed {
            self.context
                .cancellation()
                .request(crate::CancelCause::new("caller dropped operation"));
            self.observation
                .publish(ObservationEvent::OperationFinished {
                    service_id: self.service_id.clone(),
                    operation_id: self.context.operation_id(),
                    outcome: CallOutcome::Cancelled,
                });
        }
    }
}

fn call_outcome<T>(result: &RuntimeResult<T>) -> CallOutcome {
    match result {
        Ok(_) => CallOutcome::Completed,
        Err(RuntimeError::Cancelled) => CallOutcome::Cancelled,
        Err(error) => CallOutcome::Failed(error.to_string().into()),
    }
}

fn batch_outcome<T>(results: &[RuntimeResult<T>]) -> CallOutcome {
    if let Some(error) = results.iter().find_map(|result| result.as_ref().err()) {
        return match error {
            RuntimeError::Cancelled => CallOutcome::Cancelled,
            error => CallOutcome::Failed(error.to_string().into()),
        };
    }
    CallOutcome::Completed
}
