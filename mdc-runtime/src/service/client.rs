use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use tokio::sync::{mpsc, oneshot, watch};

use crate::{
    CallError, CancelReason, OperationId, RequestContext, RuntimeError, Service, ServiceLifecycle,
    TaskExit, TaskId, TraceContext,
};

use super::ControlHandle;

static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);

type Accepted<S> =
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
        let id = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
        self.submit_with(
            request,
            RequestContext::root(OperationId(id), TraceContext::root(u128::from(id))),
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
        let _ = self.submit(request).await?;
        Ok(())
    }

    pub async fn submit_with(
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

impl crate::TaskContext {
    pub async fn call<S: Service>(
        &self,
        client: &ServiceClient<S>,
        request: S::Request,
    ) -> Result<S::Response, CallError<S::Error>> {
        if let Some(reason) = self.cancellation_reason() {
            return Err(CallError::Cancelled(reason));
        }
        let submission = client
            .submit_with(request, self.child_request())
            .await
            .map_err(CallError::Runtime)?;
        match submission {
            Submission::Reply(result) => {
                if let Some(reason) = self.cancellation_reason() {
                    Err(CallError::Cancelled(reason))
                } else {
                    result.map_err(CallError::Service)
                }
            }
            Submission::Task(ticket) => self.wait_for_child(ticket).await,
        }
    }

    async fn wait_for_child<T, E>(&self, ticket: TaskTicket<T, E>) -> Result<T, CallError<E>> {
        let (task_id, control, mut completion) = ticket.into_parts();
        tokio::select! {
            biased;
            reason = self.cancelled() => {
                let _ = control.cancel_task(task_id, reason.clone()).await;
                let _ = (&mut completion)
                    .await
                    .map_err(|_| CallError::Runtime(RuntimeError::ResponseDropped))?;
                Err(CallError::Cancelled(reason))
            }
            exit = &mut completion => {
                match exit.map_err(|_| CallError::Runtime(RuntimeError::ResponseDropped))? {
                    TaskExit::Completed(value) => Ok(value),
                    TaskExit::Failed(error) => Err(CallError::Service(error)),
                    TaskExit::Cancelled(reason) => Err(CallError::Cancelled(reason)),
                    TaskExit::Aborted => Err(CallError::Aborted),
                }
            }
        }
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
