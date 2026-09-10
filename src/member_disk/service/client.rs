use super::{MemberDiskService, MemberDiskServiceError};
use crate::{
    member_disk::{AllocateBlks, Allocation, DiskUuid, MemberDisk, MemberDiskEvent, PhysicalState},
    runtime::{
        CallError, OperationContext, OperationSpec, ServiceClient, ServiceControl, ServiceInstance,
        ServiceObserver, ServiceTask,
    },
};

/// Confirms that an external fact or intent entered its MemberDisk object slot.
/// It does not mean that background convergence has completed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Accepted;

/// The complete protocol accepted by one MemberDisk service instance.
#[doc(hidden)]
pub enum MemberDiskRequest {
    ApplyEvent(MemberDiskEvent),
    Get(DiskUuid),
    WaitIdle(DiskUuid),
    Allocate(AllocateBlks),
}

/// One reply type shared by the complete MemberDisk protocol.
#[doc(hidden)]
pub enum MemberDiskReply {
    Accepted(Accepted),
    Member(MemberDisk),
    Idle,
    Allocation(Allocation),
}

impl MemberDiskRequest {
    pub(super) fn operation(&self) -> Option<OperationSpec> {
        match self {
            Self::ApplyEvent(event) => {
                let (kind, action) = match event {
                    MemberDiskEvent::PhysicalChanged {
                        state: PhysicalState::Down,
                        ..
                    } => ("offline", "take offline"),
                    MemberDiskEvent::PhysicalChanged {
                        state: PhysicalState::Up,
                        ..
                    } => ("online", "bring online"),
                    MemberDiskEvent::Shrink { .. } => ("shrink", "shrink"),
                };
                Some(OperationSpec::new(
                    "member_disk",
                    kind,
                    format!("disk/{}", event.disk()),
                    format!("{action} member disk {}", event.disk()),
                ))
            }
            Self::Allocate(request) => Some(OperationSpec::new(
                "member_disk",
                "allocate_blks",
                format!("tier/{}", request.tier()),
                format!(
                    "allocate {} BLKs from Tier {}",
                    request.count(),
                    request.tier()
                ),
            )),
            Self::Get(_) | Self::WaitIdle(_) => None,
        }
    }
}

pub type MemberDiskCallError = CallError<MemberDiskServiceError>;

/// Business-facing MemberDisk API. The private request/reply enums never leak
/// into Pool workflows.
#[derive(Clone)]
pub struct MemberDiskClient {
    inner: ServiceClient<MemberDiskService>,
}

impl MemberDiskClient {
    pub(super) fn new(inner: ServiceClient<MemberDiskService>) -> Self {
        Self { inner }
    }

    /// Accepts a physical fact or management intent. The matching convergence
    /// Future continues in the service after this returns `Accepted`.
    pub async fn submit(&self, event: MemberDiskEvent) -> Result<Accepted, MemberDiskCallError> {
        expect_accepted(
            self.inner
                .call(MemberDiskRequest::ApplyEvent(event))
                .await?,
        )
    }

    /// Same operation as [`Self::submit`], but preserves the caller's causal
    /// operation and trace when invoked from another service workflow.
    pub async fn submit_in(
        &self,
        operation: &OperationContext,
        event: MemberDiskEvent,
    ) -> Result<Accepted, MemberDiskCallError> {
        expect_accepted(
            self.inner
                .call_in(operation, MemberDiskRequest::ApplyEvent(event))
                .await?,
        )
    }

    pub async fn get(&self, disk: DiskUuid) -> Result<MemberDisk, MemberDiskCallError> {
        expect_member(self.inner.call(MemberDiskRequest::Get(disk)).await?)
    }

    /// Reads a MemberDisk while retaining the caller's operation identity.
    pub async fn get_in(
        &self,
        operation: &OperationContext,
        disk: DiskUuid,
    ) -> Result<MemberDisk, MemberDiskCallError> {
        expect_member(
            self.inner
                .call_in(operation, MemberDiskRequest::Get(disk))
                .await?,
        )
    }

    pub async fn wait_idle(&self, disk: DiskUuid) -> Result<(), MemberDiskCallError> {
        expect_idle(self.inner.call(MemberDiskRequest::WaitIdle(disk)).await?)
    }

    /// Waits for the object slot while retaining the caller's operation
    /// identity.
    pub async fn wait_idle_in(
        &self,
        operation: &OperationContext,
        disk: DiskUuid,
    ) -> Result<(), MemberDiskCallError> {
        expect_idle(
            self.inner
                .call_in(operation, MemberDiskRequest::WaitIdle(disk))
                .await?,
        )
    }

    pub async fn allocate_blks(
        &self,
        request: AllocateBlks,
    ) -> Result<Allocation, MemberDiskCallError> {
        expect_allocation(
            self.inner
                .call(MemberDiskRequest::Allocate(request))
                .await?,
        )
    }

    /// Allocates BLKs while retaining the caller's operation identity and
    /// trace across the service boundary.
    pub async fn allocate_blks_in(
        &self,
        operation: &OperationContext,
        request: AllocateBlks,
    ) -> Result<Allocation, MemberDiskCallError> {
        expect_allocation(
            self.inner
                .call_in(operation, MemberDiskRequest::Allocate(request))
                .await?,
        )
    }
}

fn expect_accepted(reply: MemberDiskReply) -> Result<Accepted, MemberDiskCallError> {
    match reply {
        MemberDiskReply::Accepted(accepted) => Ok(accepted),
        _ => Err(CallError::ProtocolViolation(
            "MemberDisk ApplyEvent returned an unexpected reply",
        )),
    }
}

fn expect_member(reply: MemberDiskReply) -> Result<MemberDisk, MemberDiskCallError> {
    match reply {
        MemberDiskReply::Member(member) => Ok(member),
        _ => Err(CallError::ProtocolViolation(
            "MemberDisk Get returned an unexpected reply",
        )),
    }
}

fn expect_idle(reply: MemberDiskReply) -> Result<(), MemberDiskCallError> {
    match reply {
        MemberDiskReply::Idle => Ok(()),
        _ => Err(CallError::ProtocolViolation(
            "MemberDisk WaitIdle returned an unexpected reply",
        )),
    }
}

fn expect_allocation(reply: MemberDiskReply) -> Result<Allocation, MemberDiskCallError> {
    match reply {
        MemberDiskReply::Allocation(allocation) => Ok(allocation),
        _ => Err(CallError::ProtocolViolation(
            "MemberDisk Allocate returned an unexpected reply",
        )),
    }
}

/// The four handles owned by one Pool for its MemberDisk service instance.
pub struct MemberDiskRuntime {
    pub client: MemberDiskClient,
    pub control: ServiceControl,
    pub observer: ServiceObserver,
    pub task: ServiceTask,
}

impl From<ServiceInstance<MemberDiskService>> for MemberDiskRuntime {
    fn from(instance: ServiceInstance<MemberDiskService>) -> Self {
        Self {
            client: MemberDiskClient::new(instance.client),
            control: instance.control,
            observer: instance.observer,
            task: instance.task,
        }
    }
}
