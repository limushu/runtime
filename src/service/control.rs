use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::{CancelReason, RuntimeError, TaskId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ShutdownMode {
    Graceful,
    Immediate,
}

pub(crate) enum RuntimeControl {
    Pause(oneshot::Sender<()>),
    Resume(oneshot::Sender<()>),
    Drain(oneshot::Sender<()>),
    Shutdown(ShutdownMode, oneshot::Sender<()>),
    CancelTask {
        task_id: TaskId,
        reason: CancelReason,
        reply: oneshot::Sender<Result<(), RuntimeError>>,
    },
}

#[derive(Clone)]
pub struct ControlHandle {
    service: Arc<str>,
    sender: mpsc::Sender<RuntimeControl>,
}

impl ControlHandle {
    pub async fn pause(&self) -> Result<(), RuntimeError> {
        self.request(RuntimeControl::Pause).await
    }

    pub async fn resume(&self) -> Result<(), RuntimeError> {
        self.request(RuntimeControl::Resume).await
    }

    pub async fn drain(&self) -> Result<(), RuntimeError> {
        self.request(RuntimeControl::Drain).await
    }

    pub async fn shutdown(&self, mode: ShutdownMode) -> Result<(), RuntimeError> {
        let (reply, completed) = oneshot::channel();
        self.sender
            .send(RuntimeControl::Shutdown(mode, reply))
            .await
            .map_err(|_| RuntimeError::ChannelClosed(self.service.to_string()))?;
        completed.await.map_err(|_| RuntimeError::ResponseDropped)
    }

    pub async fn cancel_task(
        &self,
        task_id: TaskId,
        reason: CancelReason,
    ) -> Result<(), RuntimeError> {
        let (reply, completed) = oneshot::channel();
        self.sender
            .send(RuntimeControl::CancelTask {
                task_id,
                reason,
                reply,
            })
            .await
            .map_err(|_| RuntimeError::ChannelClosed(self.service.to_string()))?;
        completed.await.map_err(|_| RuntimeError::ResponseDropped)?
    }

    async fn request(
        &self,
        build: fn(oneshot::Sender<()>) -> RuntimeControl,
    ) -> Result<(), RuntimeError> {
        let (reply, completed) = oneshot::channel();
        self.sender
            .send(build(reply))
            .await
            .map_err(|_| RuntimeError::ChannelClosed(self.service.to_string()))?;
        completed.await.map_err(|_| RuntimeError::ResponseDropped)
    }
}

pub(crate) fn control(name: Arc<str>, sender: mpsc::Sender<RuntimeControl>) -> ControlHandle {
    ControlHandle {
        service: name,
        sender,
    }
}
