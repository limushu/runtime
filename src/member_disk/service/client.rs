use super::MemberDiskServiceError;
use crate::member_disk::{DiskUuid, MemberDisk, MemberDiskEvent};
use tokio::sync::{mpsc, oneshot};

#[derive(Clone)]
pub struct MemberDiskClient {
    sender: mpsc::Sender<ServiceMessage>,
}

impl MemberDiskClient {
    pub(super) fn new(sender: mpsc::Sender<ServiceMessage>) -> Self {
        Self { sender }
    }

    /// Returns after the event has updated the object's facts or intent and
    /// entered its per-disk execution slot. It does not wait for convergence.
    pub async fn submit(&self, event: MemberDiskEvent) -> Result<(), MemberDiskServiceError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(ServiceMessage::Submit { event, reply })
            .await
            .map_err(|_| MemberDiskServiceError::ServiceStopped)?;
        response
            .await
            .map_err(|_| MemberDiskServiceError::ServiceStopped)?
    }

    pub async fn get(&self, disk: DiskUuid) -> Result<MemberDisk, MemberDiskServiceError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(ServiceMessage::Get { disk, reply })
            .await
            .map_err(|_| MemberDiskServiceError::ServiceStopped)?;
        response
            .await
            .map_err(|_| MemberDiskServiceError::ServiceStopped)?
    }

    pub async fn wait_idle(&self, disk: DiskUuid) -> Result<(), MemberDiskServiceError> {
        let (reply, response) = oneshot::channel();
        self.sender
            .send(ServiceMessage::WaitIdle { disk, reply })
            .await
            .map_err(|_| MemberDiskServiceError::ServiceStopped)?;
        response
            .await
            .map_err(|_| MemberDiskServiceError::ServiceStopped)?
    }
}

pub(super) enum ServiceMessage {
    Submit {
        event: MemberDiskEvent,
        reply: oneshot::Sender<Result<(), MemberDiskServiceError>>,
    },
    Get {
        disk: DiskUuid,
        reply: oneshot::Sender<Result<MemberDisk, MemberDiskServiceError>>,
    },
    WaitIdle {
        disk: DiskUuid,
        reply: oneshot::Sender<Result<(), MemberDiskServiceError>>,
    },
}
