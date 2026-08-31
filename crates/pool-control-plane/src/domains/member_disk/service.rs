use super::machine::{
    MemberDiskEffect, MemberDiskEvent, MemberDiskMachine, MemberDiskTransition,
    MemberDiskWorkflowKind,
};
use super::protocol::{MemberDiskReply, MemberDiskRequest, MemberDiskState};
use crate::domains::pool_node::protocol::{MemberDiskIoAvailability, PoolNodeRequest};
use crate::domains::virtual_disk::protocol::{VirtualDiskReply, VirtualDiskRequest};
use crate::kernel::MemberDiskId;
use async_trait::async_trait;
use control_runtime::{
    Admission, CancelCause, ObjectActivity, ObjectKey, RequestRoute, Router, RuntimeError,
    RuntimeResult, Service, StateCell, WorkflowContext, WorkflowMeta,
};
use std::collections::HashMap;

#[derive(Debug, Default)]
struct MemberDiskMetadata {
    disks: HashMap<MemberDiskId, MemberDiskState>,
}

impl MemberDiskMetadata {
    fn state(&self, disk: &MemberDiskId) -> RuntimeResult<MemberDiskState> {
        self.disks
            .get(disk)
            .copied()
            .ok_or_else(|| RuntimeError::InvalidState(format!("unknown member disk {disk}")))
    }

    fn apply(
        &mut self,
        disk: &MemberDiskId,
        event: MemberDiskEvent,
    ) -> RuntimeResult<MemberDiskTransition> {
        let transition = MemberDiskMachine::transition(self.state(disk)?, event)?;
        self.disks.insert(disk.clone(), transition.to);
        Ok(transition)
    }
}

/// Owns all mutable MemberDisk metadata for one Pool.
pub struct MemberDiskService {
    metadata: StateCell<MemberDiskMetadata>,
    router: Router,
}

impl MemberDiskService {
    pub fn new(router: Router, disks: impl IntoIterator<Item = MemberDiskId>) -> Self {
        let disks = disks
            .into_iter()
            .map(|disk| (disk, MemberDiskState::Ua))
            .collect();
        Self {
            metadata: StateCell::new(MemberDiskMetadata { disks }),
            router,
        }
    }

    fn apply_event(
        &self,
        context: &WorkflowContext,
        disk: &MemberDiskId,
        event: MemberDiskEvent,
    ) -> RuntimeResult<MemberDiskTransition> {
        let transition = self
            .metadata
            .update(|metadata| metadata.apply(disk, event))?;
        if transition.from != transition.to {
            context.state_transition(
                Self::object_key(disk),
                format!("{:?}", transition.from),
                format!("{:?}", transition.to),
                format!("{:?}", transition.event),
            );
        }
        Ok(transition)
    }

    fn object_key(disk: &MemberDiskId) -> ObjectKey {
        ObjectKey::new(format!("member-disk/{disk}"))
    }

    fn reply(&self, disk: MemberDiskId) -> RuntimeResult<MemberDiskReply> {
        Ok(MemberDiskReply {
            state: self.metadata.read(|metadata| metadata.state(&disk))?,
            disk,
        })
    }

    async fn offline_workflow(
        &self,
        disk: MemberDiskId,
        context: WorkflowContext,
    ) -> RuntimeResult<MemberDiskReply> {
        self.router
            .call(
                &context,
                PoolNodeRequest::PublishMemberDisk {
                    disk: disk.clone(),
                    availability: MemberDiskIoAvailability::Down,
                },
            )
            .await?;
        context.milestone("member disk down fact published to PoolNode");
        context.stable_boundary()?;

        self.apply_event(&context, &disk, MemberDiskEvent::BeginDrain)?;

        let settled = self
            .router
            .call(
                &context,
                VirtualDiskRequest::EvacuateMemberDisk(disk.clone()),
            )
            .await?;
        context.milestone("VirtualDisk evacuation returned a stable result");

        match settled {
            VirtualDiskReply::Evacuated { .. } => context.stable_boundary()?,
            VirtualDiskReply::DrainStopped { .. } => return Err(RuntimeError::Cancelled),
            VirtualDiskReply::Stats(_) => {
                return Err(RuntimeError::Internal(
                    "VirtualDisk returned stats to an evacuation request".into(),
                ))
            }
        }

        self.apply_event(&context, &disk, MemberDiskEvent::DrainCompleted)?;
        self.reply(disk)
    }

    async fn online_workflow(
        &self,
        disk: MemberDiskId,
        context: WorkflowContext,
    ) -> RuntimeResult<MemberDiskReply> {
        self.router
            .call(
                &context,
                PoolNodeRequest::PublishMemberDisk {
                    disk: disk.clone(),
                    availability: MemberDiskIoAvailability::Up,
                },
            )
            .await?;
        context.stable_boundary()?;
        self.apply_event(&context, &disk, MemberDiskEvent::OnlineSettled)?;
        self.reply(disk)
    }

    fn admit_event(
        &self,
        context: &WorkflowContext,
        disk: &MemberDiskId,
        event: MemberDiskEvent,
        kind: MemberDiskWorkflowKind,
        activity: &ObjectActivity<MemberDiskWorkflowKind>,
    ) -> RuntimeResult<Admission<MemberDiskReply>> {
        if activity.target_kind() == Some(&kind) {
            return Ok(Admission::Join);
        }

        let transition = self.apply_event(context, disk, event)?;
        Ok(match transition.effect {
            MemberDiskEffect::Complete => Admission::Complete(self.reply(disk.clone())?),
            MemberDiskEffect::Reject(reason) => Admission::Reject {
                reason: reason.into(),
            },
            MemberDiskEffect::Continue if activity.is_idle() => Admission::Start,
            MemberDiskEffect::Continue => Admission::Replace {
                cause: CancelCause::new(match kind {
                    MemberDiskWorkflowKind::Offline => {
                        "disk went down; replace the current online intent"
                    }
                    MemberDiskWorkflowKind::Online => {
                        "disk recovered; stop draining at a stable boundary"
                    }
                }),
            },
        })
    }
}

#[async_trait]
impl Service for MemberDiskService {
    type Request = MemberDiskRequest;
    type WorkflowKind = MemberDiskWorkflowKind;

    fn route(&self, request: &Self::Request) -> RequestRoute<Self::WorkflowKind> {
        match request {
            MemberDiskRequest::Get(_) => RequestRoute::Untracked,
            // DiskMap facts must converge after admission even if their
            // original producer disconnects.
            MemberDiskRequest::Offline(disk) => RequestRoute::Workflow(
                WorkflowMeta::object(
                    Self::object_key(disk),
                    MemberDiskWorkflowKind::Offline,
                    format!("take member disk {disk} offline"),
                )
                .continue_when_orphaned(),
            ),
            MemberDiskRequest::Online(disk) => RequestRoute::Workflow(
                WorkflowMeta::object(
                    Self::object_key(disk),
                    MemberDiskWorkflowKind::Online,
                    format!("bring member disk {disk} online"),
                )
                .continue_when_orphaned(),
            ),
        }
    }

    fn admit(
        &self,
        context: &WorkflowContext,
        request: &Self::Request,
        activity: &ObjectActivity<Self::WorkflowKind>,
    ) -> RuntimeResult<Admission<MemberDiskReply>> {
        match request {
            MemberDiskRequest::Offline(disk) => self.admit_event(
                context,
                disk,
                MemberDiskEvent::DiskDown,
                MemberDiskWorkflowKind::Offline,
                activity,
            ),
            MemberDiskRequest::Online(disk) => self.admit_event(
                context,
                disk,
                MemberDiskEvent::DiskUp,
                MemberDiskWorkflowKind::Online,
                activity,
            ),
            MemberDiskRequest::Get(_) => Err(RuntimeError::Internal(
                "untracked request reached object admission".into(),
            )),
        }
    }

    async fn handle(
        &self,
        request: Self::Request,
        context: WorkflowContext,
    ) -> RuntimeResult<MemberDiskReply> {
        match request {
            MemberDiskRequest::Offline(disk) => self.offline_workflow(disk, context).await,
            MemberDiskRequest::Online(disk) => self.online_workflow(disk, context).await,
            MemberDiskRequest::Get(disk) => self.reply(disk),
        }
    }
}
