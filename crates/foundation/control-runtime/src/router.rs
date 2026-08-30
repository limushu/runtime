use crate::observation::ObservationHub;
use crate::{
    CallOutcome, ObservationEvent, RuntimeError, RuntimeResult, ServiceId, ServiceRequest,
    WorkflowContext,
};
use futures::future::join_all;
use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, oneshot};

pub(crate) struct BusinessEnvelope<R: ServiceRequest> {
    pub context: WorkflowContext,
    pub request: R,
    pub reply: oneshot::Sender<RuntimeResult<R::Response>>,
}

pub(crate) struct BusinessHandle<R: ServiceRequest> {
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
    pub(crate) fn new(service_id: ServiceId, sender: mpsc::Sender<BusinessEnvelope<R>>) -> Self {
        Self { service_id, sender }
    }

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

struct BusinessEndpoint<R: ServiceRequest> {
    handle: BusinessHandle<R>,
    observation: ObservationHub,
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
        if !self.armed {
            return;
        }
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
        if !self.armed {
            return;
        }
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

#[derive(Clone, Default)]
pub struct Router {
    endpoints: Arc<RwLock<HashMap<ServiceId, Arc<dyn Any + Send + Sync>>>>,
}

impl Router {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register<R: ServiceRequest>(
        &self,
        handle: BusinessHandle<R>,
        observation: ObservationHub,
    ) {
        self.endpoints
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                R::service_id(),
                Arc::new(BusinessEndpoint {
                    handle,
                    observation,
                }),
            );
    }

    pub async fn call<R: ServiceRequest>(
        &self,
        parent: &WorkflowContext,
        request: R,
    ) -> RuntimeResult<R::Response> {
        if parent.cancellation().is_requested() {
            return Err(RuntimeError::Cancelled);
        }
        let service_id = R::service_id();
        let endpoint = self.endpoint::<R>(&service_id)?;
        let child = parent
            .child(service_id.clone(), service_id.as_str())
            .with_observation(endpoint.observation.clone());
        endpoint.observation.publish(ObservationEvent::CallStarted {
            service_id: service_id.clone(),
            operation_id: child.operation_id(),
            call_id: child.call_id(),
            parent_call_id: child.parent_call_id(),
            parent_task_attempt_id: child.parent_task_attempt_id(),
        });
        let mut call_guard = CallGuard::new(
            child.clone(),
            service_id.clone(),
            endpoint.observation.clone(),
        );

        // The callee owns the meaning of a stable result. Do not rewrite a
        // typed "settled/stopped" response merely because the parent asked to
        // cancel while the call was in flight; the caller's workflow must
        // inspect that result and decide its next domain transition.
        let result = endpoint.handle.call(child.clone(), request).await;
        call_guard.finish(call_outcome(&result));
        result
    }

    pub async fn call_root<R: ServiceRequest>(
        &self,
        label: impl Into<Arc<str>>,
        request: R,
    ) -> RuntimeResult<R::Response> {
        let service_id = R::service_id();
        let endpoint = self.endpoint::<R>(&service_id)?;
        let context = WorkflowContext::root(service_id.clone(), label)
            .with_observation(endpoint.observation.clone());
        endpoint
            .observation
            .publish(ObservationEvent::OperationStarted {
                service_id: service_id.clone(),
                operation_id: context.operation_id(),
                call_id: context.call_id(),
                label: context.label().into(),
            });
        endpoint.observation.publish(ObservationEvent::CallStarted {
            service_id: service_id.clone(),
            operation_id: context.operation_id(),
            call_id: context.call_id(),
            parent_call_id: None,
            parent_task_attempt_id: None,
        });
        let mut operation_guard = OperationGuard::new(
            context.clone(),
            service_id.clone(),
            endpoint.observation.clone(),
        );
        let mut call_guard = CallGuard::new(
            context.clone(),
            service_id.clone(),
            endpoint.observation.clone(),
        );

        let result = endpoint.handle.call(context.clone(), request).await;
        let outcome = call_outcome(&result);
        call_guard.finish(outcome.clone());
        operation_guard.finish(outcome);
        result
    }

    /// Execute homogeneous requests as one observable operation.
    ///
    /// A batch is transport composition, not a workflow of its own: every item
    /// still passes through the target service's normal object admission and
    /// may independently start, join, queue, preempt, complete, or fail.
    pub async fn call_batch<R, I>(
        &self,
        label: impl Into<Arc<str>>,
        requests: I,
    ) -> RuntimeResult<Vec<RuntimeResult<R::Response>>>
    where
        R: ServiceRequest,
        I: IntoIterator<Item = R>,
    {
        let service_id = R::service_id();
        let endpoint = self.endpoint::<R>(&service_id)?;
        let context = WorkflowContext::root(service_id.clone(), label)
            .with_observation(endpoint.observation.clone());
        endpoint
            .observation
            .publish(ObservationEvent::OperationStarted {
                service_id: service_id.clone(),
                operation_id: context.operation_id(),
                call_id: context.call_id(),
                label: context.label().into(),
            });
        let mut operation_guard =
            OperationGuard::new(context.clone(), service_id, endpoint.observation.clone());

        let results = join_all(
            requests
                .into_iter()
                .map(|request| self.call(&context, request)),
        )
        .await;
        operation_guard.finish(batch_outcome(&results));
        Ok(results)
    }

    fn endpoint<R: ServiceRequest>(
        &self,
        service_id: &ServiceId,
    ) -> RuntimeResult<Arc<BusinessEndpoint<R>>> {
        self.endpoints
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(service_id)
            .cloned()
            .ok_or_else(|| RuntimeError::ServiceNotFound(service_id.clone()))?
            .downcast::<BusinessEndpoint<R>>()
            .map_err(|_| RuntimeError::WrongProtocol(service_id.clone()))
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
    if let Some(error) = results.iter().find_map(|result| match result {
        Err(error @ RuntimeError::Cancelled) => Some(error),
        Err(error) => Some(error),
        Ok(_) => None,
    }) {
        return match error {
            RuntimeError::Cancelled => CallOutcome::Cancelled,
            error => CallOutcome::Failed(error.to_string().into()),
        };
    }
    CallOutcome::Completed
}
