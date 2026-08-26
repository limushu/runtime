use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    CallError, CancelReason, OperationId, RequestContext, RequestId, RuntimeError, Service,
    ServiceLifecycle, TaskExit, TaskId, TraceContext,
};

use super::ControlHandle;

static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) type Accepted<S> =
    Result<Submission<<S as Service>::Response, <S as Service>::Error>, RuntimeError>;

pub(crate) struct RequestEnvelope<S: Service> {
    pub request: S::Request,
    pub context: RequestContext,
    pub accepted: oneshot::Sender<Accepted<S>>,
}

pub enum Submission<T, E> {
    Reply(Result<T, E>),
    Task(TaskTicket<T, E>),
}

pub struct ServiceClient<S: Service> {
    name: Arc<str>,
    sender: mpsc::Sender<RequestEnvelope<S>>,
    status: watch::Receiver<ServiceLifecycle>,
}

impl<S: Service> ServiceClient<S> {
    pub fn service_name(&self) -> &str {
        &self.name
    }

    pub async fn submit(
        &self,
        request: S::Request,
    ) -> Result<Submission<S::Response, S::Error>, RuntimeError> {
        let operation_id = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
        let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        self.send_with(
            request,
            RequestContext::root(
                RequestId(request_id),
                OperationId(operation_id),
                TraceContext::root(u128::from(operation_id)),
            ),
        )
        .await
    }

    pub async fn call(&self, request: S::Request) -> Result<S::Response, CallError<S::Error>> {
        match self.submit(request).await.map_err(CallError::Runtime)? {
            Submission::Reply(result) => result.map_err(CallError::Service),
            Submission::Task(ticket) => match ticket.wait().await.map_err(CallError::Runtime)? {
                TaskExit::Completed(value) => Ok(value),
                TaskExit::Failed(error) => Err(CallError::Service(error)),
                TaskExit::Cancelled(reason) => Err(CallError::Cancelled(reason)),
                TaskExit::Aborted => Err(CallError::Aborted),
            },
        }
    }

    pub async fn send(&self, request: S::Request) -> Result<(), RuntimeError> {
        let operation_id = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
        let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let context = RequestContext::root(
            RequestId(request_id),
            OperationId(operation_id),
            TraceContext::root(u128::from(operation_id)),
        );
        let lifecycle = *self.status.borrow();
        if lifecycle != ServiceLifecycle::Running {
            return Err(RuntimeError::ServiceUnavailable(format!(
                "{} is {lifecycle:?}",
                self.name
            )));
        }
        let (accepted, _ignored) = oneshot::channel();
        self.sender
            .send(RequestEnvelope {
                request,
                context,
                accepted,
            })
            .await
            .map_err(|_| RuntimeError::ChannelClosed(self.name.to_string()))
    }

    pub async fn submit_with(
        &self,
        request: S::Request,
        context: RequestContext,
    ) -> Result<Submission<S::Response, S::Error>, RuntimeError> {
        let request_id = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        self.send_with(request, context.for_request(RequestId(request_id)))
            .await
    }

    async fn send_with(
        &self,
        request: S::Request,
        context: RequestContext,
    ) -> Result<Submission<S::Response, S::Error>, RuntimeError> {
        let lifecycle = *self.status.borrow();
        if lifecycle != ServiceLifecycle::Running {
            return Err(RuntimeError::ServiceUnavailable(format!(
                "{} is {lifecycle:?}",
                self.name
            )));
        }
        let (accepted, reply) = oneshot::channel();
        self.sender
            .send(RequestEnvelope {
                request,
                context,
                accepted,
            })
            .await
            .map_err(|_| RuntimeError::ChannelClosed(self.name.to_string()))?;
        reply.await.map_err(|_| RuntimeError::ResponseDropped)?
    }
}

impl<S: Service> Clone for ServiceClient<S> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            sender: self.sender.clone(),
            status: self.status.clone(),
        }
    }
}

pub struct TaskTicket<T, E> {
    task_id: TaskId,
    control: ControlHandle,
    completion: oneshot::Receiver<TaskExit<T, E>>,
}

impl<T, E> TaskTicket<T, E> {
    pub(crate) fn new(
        task_id: TaskId,
        control: ControlHandle,
        completion: oneshot::Receiver<TaskExit<T, E>>,
    ) -> Self {
        Self {
            task_id,
            control,
            completion,
        }
    }

    pub fn task_id(&self) -> TaskId {
        self.task_id
    }

    pub async fn request_cancel(&self, reason: CancelReason) -> Result<(), RuntimeError> {
        self.control.cancel_task(self.task_id, reason).await
    }

    pub async fn wait(self) -> Result<TaskExit<T, E>, RuntimeError> {
        self.completion
            .await
            .map_err(|_| RuntimeError::ResponseDropped)
    }

    pub async fn cancel_and_wait(
        self,
        reason: CancelReason,
    ) -> Result<TaskExit<T, E>, RuntimeError> {
        self.request_cancel(reason).await?;
        self.wait().await
    }

    pub(crate) fn into_parts(self) -> (TaskId, ControlHandle, oneshot::Receiver<TaskExit<T, E>>) {
        (self.task_id, self.control, self.completion)
    }
}

pub(crate) fn client<S: Service>(
    name: Arc<str>,
    sender: mpsc::Sender<RequestEnvelope<S>>,
    status: watch::Receiver<ServiceLifecycle>,
) -> ServiceClient<S> {
    ServiceClient {
        name,
        sender,
        status,
    }
}
