use super::MemberDiskServiceError;
use crate::{
    member_disk::{AllocateBlks, Allocation, DiskUuid, MemberDisk, MemberDiskEvent, PhysicalState},
    runtime::{OperationSpec, ServiceClient, ServiceMessage, ServiceRequest, ServiceUnavailable},
};
use tokio::sync::oneshot;

/// Confirms that an external fact or intent entered its MemberDisk task slot.
/// It does not mean that background convergence has completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Accepted;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetMemberDisk(pub DiskUuid);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WaitMemberDiskIdle(pub DiskUuid);

pub type MemberDiskClient = ServiceClient<MemberDiskMessage>;
pub type MemberDiskRuntime = crate::runtime::ServiceInstance<MemberDiskMessage>;

impl ServiceMessage for MemberDiskMessage {
    type Error = MemberDiskServiceError;

    fn stopped() -> Self::Error {
        MemberDiskServiceError::ServiceStopped
    }

    fn unavailable(reason: ServiceUnavailable) -> Self::Error {
        reason.into()
    }

    fn reject(self, reason: ServiceUnavailable) {
        match self {
            Self::ApplyEvent { reply, .. } => {
                let _ = reply.send(Err(reason.into()));
            }
            Self::Get { reply, .. } => {
                let _ = reply.send(Err(reason.into()));
            }
            Self::WaitIdle { reply, .. } => {
                let _ = reply.send(Err(reason.into()));
            }
            Self::Allocate { reply, .. } => {
                let _ = reply.send(Err(reason.into()));
            }
        }
    }
}

impl ServiceRequest<MemberDiskMessage> for MemberDiskEvent {
    type Response = Accepted;

    fn operation(&self) -> Option<OperationSpec> {
        let (kind, action) = match self {
            Self::PhysicalChanged {
                state: PhysicalState::Down,
                ..
            } => ("offline", "take offline"),
            Self::PhysicalChanged {
                state: PhysicalState::Up,
                ..
            } => ("online", "bring online"),
            Self::Shrink { .. } => ("shrink", "shrink"),
        };
        Some(OperationSpec::new(
            "member_disk",
            kind,
            format!("disk/{}", self.disk()),
            format!("{action} member disk {}", self.disk()),
        ))
    }

    fn into_message(
        self,
        reply: oneshot::Sender<Result<Self::Response, MemberDiskServiceError>>,
    ) -> MemberDiskMessage {
        MemberDiskMessage::ApplyEvent { event: self, reply }
    }
}

impl ServiceRequest<MemberDiskMessage> for GetMemberDisk {
    type Response = MemberDisk;

    fn into_message(
        self,
        reply: oneshot::Sender<Result<Self::Response, MemberDiskServiceError>>,
    ) -> MemberDiskMessage {
        MemberDiskMessage::Get {
            disk: self.0,
            reply,
        }
    }
}

impl ServiceRequest<MemberDiskMessage> for WaitMemberDiskIdle {
    type Response = ();

    fn into_message(
        self,
        reply: oneshot::Sender<Result<Self::Response, MemberDiskServiceError>>,
    ) -> MemberDiskMessage {
        MemberDiskMessage::WaitIdle {
            disk: self.0,
            reply,
        }
    }
}

impl ServiceRequest<MemberDiskMessage> for AllocateBlks {
    type Response = Allocation;

    fn operation(&self) -> Option<OperationSpec> {
        Some(OperationSpec::new(
            "member_disk",
            "allocate_blks",
            format!("tier/{}", self.tier()),
            format!("allocate {} BLKs from Tier {}", self.count(), self.tier()),
        ))
    }

    fn into_message(
        self,
        reply: oneshot::Sender<Result<Self::Response, MemberDiskServiceError>>,
    ) -> MemberDiskMessage {
        MemberDiskMessage::Allocate {
            request: self,
            reply,
        }
    }
}

#[doc(hidden)]
pub enum MemberDiskMessage {
    ApplyEvent {
        event: MemberDiskEvent,
        reply: oneshot::Sender<Result<Accepted, MemberDiskServiceError>>,
    },
    Get {
        disk: DiskUuid,
        reply: oneshot::Sender<Result<MemberDisk, MemberDiskServiceError>>,
    },
    WaitIdle {
        disk: DiskUuid,
        reply: oneshot::Sender<Result<(), MemberDiskServiceError>>,
    },
    Allocate {
        request: AllocateBlks,
        reply: oneshot::Sender<Result<Allocation, MemberDiskServiceError>>,
    },
}
