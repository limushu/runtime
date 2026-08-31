use super::service_loop::ServiceLoop;
use crate::{
    CancelCause, CancellationScope, Router, RuntimeError, RuntimeResult, Service, ServiceId,
    ServiceObserver, ServiceRequest,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

pub(super) type ResponseOf<S> = <<S as Service>::Request as ServiceRequest>::Response;
pub(super) type Reply<R> = oneshot::Sender<RuntimeResult<R>>;

#[derive(Debug, Clone)]
pub struct RuntimeConfig {
    pub business_capacity: usize,
    pub control_capacity: usize,
    pub max_active_workflows: usize,
    pub max_untracked_requests: usize,
    pub max_pending_requests: usize,
    pub max_pending_per_object: usize,
    pub shutdown_timeout: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            business_capacity: 128,
            control_capacity: 16,
            max_active_workflows: 64,
            max_untracked_requests: 64,
            max_pending_requests: 256,
            max_pending_per_object: 16,
            shutdown_timeout: Duration::from_secs(5),
        }
    }
}

pub(super) enum ControlRequest {
    Pause(Reply<()>),
    Resume(Reply<()>),
    Drain(Reply<()>),
    Stop(Reply<()>),
}

#[derive(Clone)]
pub struct ControlHandle {
    service_id: ServiceId,
    sender: mpsc::Sender<ControlRequest>,
}

impl ControlHandle {
    pub(super) fn new(service_id: ServiceId, sender: mpsc::Sender<ControlRequest>) -> Self {
        Self { service_id, sender }
    }

    async fn request(&self, build: impl FnOnce(Reply<()>) -> ControlRequest) -> RuntimeResult<()> {
        let (reply, ticket) = oneshot::channel();
        self.sender
            .send(build(reply))
            .await
            .map_err(|_| RuntimeError::ChannelClosed(self.service_id.clone()))?;
        ticket
            .await
            .map_err(|_| RuntimeError::ChannelClosed(self.service_id.clone()))?
    }

    pub async fn pause(&self) -> RuntimeResult<()> {
        self.request(ControlRequest::Pause).await
    }

    pub async fn resume(&self) -> RuntimeResult<()> {
        self.request(ControlRequest::Resume).await
    }

    pub async fn drain(&self) -> RuntimeResult<()> {
        self.request(ControlRequest::Drain).await
    }

    pub async fn stop(&self) -> RuntimeResult<()> {
        self.request(ControlRequest::Stop).await
    }
}

pub struct ManagedService {
    pub service_id: ServiceId,
    pub control: ControlHandle,
    pub observer: ServiceObserver,
    shutdown_timeout: Duration,
    cancellation: CancellationScope,
    join: Option<JoinHandle<()>>,
}

impl ManagedService {
    pub(super) fn new(
        service_id: ServiceId,
        control: ControlHandle,
        observer: ServiceObserver,
        shutdown_timeout: Duration,
        cancellation: CancellationScope,
        join: JoinHandle<()>,
    ) -> Self {
        Self {
            service_id,
            control,
            observer,
            shutdown_timeout,
            cancellation,
            join: Some(join),
        }
    }

    pub fn force_abort(&self) {
        self.cancellation
            .request(CancelCause::new("service force aborted"));
        if let Some(join) = &self.join {
            join.abort();
        }
    }

    pub async fn shutdown(mut self) -> RuntimeResult<()> {
        self.cancellation
            .request(CancelCause::new("service shutting down"));
        match tokio::time::timeout(self.shutdown_timeout, self.control.stop()).await {
            Ok(result) => result?,
            Err(_) => {
                self.force_abort();
                return Err(RuntimeError::Timeout(format!(
                    "service {} did not stop cooperatively",
                    self.service_id
                )));
            }
        }
        if let Some(join) = self.join.take() {
            join.await
                .map_err(|error| RuntimeError::Internal(error.to_string()))?;
        }
        Ok(())
    }
}

impl Drop for ManagedService {
    fn drop(&mut self) {
        self.cancellation
            .request(CancelCause::new("service owner dropped"));
        if let Some(join) = &self.join {
            join.abort();
        }
    }
}

pub fn spawn_service<S: Service>(
    service: Arc<S>,
    router: &Router,
    config: RuntimeConfig,
) -> ManagedService {
    ServiceLoop::spawn(service, router, config)
}
