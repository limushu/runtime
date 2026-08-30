use super::machine::{
    MemberDiskActivity, MemberDiskEffect, MemberDiskEvent, MemberDiskMachine, MemberDiskTransition,
    MemberDiskWorkflowKind,
};
use super::protocol::{MemberDiskReply, MemberDiskRequest, MemberDiskState};
use crate::domains::pool_node::protocol::{MemberDiskIoAvailability, PoolNodeRequest};
use crate::domains::virtual_disk::protocol::{VirtualDiskReply, VirtualDiskRequest};
use crate::kernel::MemberDiskId;
use async_trait::async_trait;
use control_runtime::{
    CancelCause, ExecutionClass, ObjectActivity, ObjectDecision, ObjectKey, Router, RuntimeError,
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
        activity: MemberDiskActivity,
    ) -> RuntimeResult<MemberDiskTransition> {
        let transition = MemberDiskMachine::transition(self.state(disk)?, event, activity)?;
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
        activity: MemberDiskActivity,
    ) -> RuntimeResult<MemberDiskTransition> {
        let transition = self
            .metadata
            .update(|metadata| metadata.apply(disk, event, activity))?;
        if transition.from != transition.to {
            context.state_transition(
                ObjectKey::new(format!("member-disk/{disk}")),
                format!("{:?}", transition.from),
                format!("{:?}", transition.to),
                format!("{:?}", transition.event),
            );
        }
        Ok(transition)
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

        self.apply_event(
            &context,
            &disk,
            MemberDiskEvent::BeginDrain,
            MemberDiskActivity::Running(MemberDiskWorkflowKind::Offline),
        )?;

        let settled = self
            .router
            .call(
                &context,
                VirtualDiskRequest::EvacuateMemberDisk(disk.clone()),
            )
            .await?;
        context.milestone("VirtualDisk evacuation returned a stable result");

        match settled {
            VirtualDiskReply::Evacuated { .. } => {}
            VirtualDiskReply::DrainStopped { .. } => return Err(RuntimeError::Cancelled),
            VirtualDiskReply::Stats(_) => {
                return Err(RuntimeError::Internal(
                    "VirtualDisk returned stats to an evacuation request".into(),
                ))
            }
        }

        self.apply_event(
            &context,
            &disk,
            MemberDiskEvent::DrainCompleted,
            MemberDiskActivity::Running(MemberDiskWorkflowKind::Offline),
        )?;
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
        self.apply_event(
            &context,
            &disk,
            MemberDiskEvent::OnlineSettled,
            MemberDiskActivity::Running(MemberDiskWorkflowKind::Online),
        )?;
        self.reply(disk)
    }

    fn decision(
        &self,
        context: &WorkflowContext,
        disk: &MemberDiskId,
        event: MemberDiskEvent,
        activity: &ObjectActivity<MemberDiskWorkflowKind>,
    ) -> RuntimeResult<ObjectDecision<MemberDiskReply>> {
        let activity = match activity {
            ObjectActivity::Idle => MemberDiskActivity::Idle,
            ObjectActivity::Pending { kind, .. } => MemberDiskActivity::Pending(*kind),
            ObjectActivity::Running { kind, .. } => MemberDiskActivity::Running(*kind),
            ObjectActivity::Cancelling {
                current_kind,
                replacement_kind,
                ..
            } => MemberDiskActivity::Cancelling {
                current: *current_kind,
                next: *replacement_kind,
            },
        };
        let transition = self.apply_event(context, disk, event, activity)?;
        Ok(match transition.effect {
            MemberDiskEffect::Start(_) => ObjectDecision::Start,
            MemberDiskEffect::Join => match activity {
                MemberDiskActivity::Pending(_) => ObjectDecision::JoinPending,
                MemberDiskActivity::Running(_) => ObjectDecision::JoinExisting,
                MemberDiskActivity::Cancelling { .. } => ObjectDecision::JoinReplacement,
                MemberDiskActivity::Idle => {
                    return Err(RuntimeError::Internal(
                        "idle MemberDisk statechart returned Join".into(),
                    ))
                }
            },
            MemberDiskEffect::Replace { cause, .. } => ObjectDecision::CancelThenStart {
                cause: CancelCause::new(cause),
            },
            MemberDiskEffect::Complete => ObjectDecision::Complete(self.reply(disk.clone())?),
            MemberDiskEffect::Reject(reason) => ObjectDecision::Reject {
                reason: reason.into(),
            },
            MemberDiskEffect::None => {
                return Err(RuntimeError::Internal(
                    "internal MemberDisk effect escaped object admission".into(),
                ))
            }
        })
    }
}

#[async_trait]
impl Service for MemberDiskService {
    type Request = MemberDiskRequest;
    type WorkflowKind = MemberDiskWorkflowKind;

    fn classify(&self, request: &Self::Request) -> ExecutionClass<Self::WorkflowKind> {
        match request {
            MemberDiskRequest::Get(_) => ExecutionClass::Inline,
            MemberDiskRequest::Offline(disk) => ExecutionClass::Workflow(WorkflowMeta::object(
                ObjectKey::new(format!("member-disk/{disk}")),
                MemberDiskWorkflowKind::Offline,
                format!("take member disk {disk} offline"),
            )),
            MemberDiskRequest::Online(disk) => ExecutionClass::Workflow(WorkflowMeta::object(
                ObjectKey::new(format!("member-disk/{disk}")),
                MemberDiskWorkflowKind::Online,
                format!("bring member disk {disk} online"),
            )),
        }
    }

    fn decide(
        &self,
        context: &WorkflowContext,
        request: &Self::Request,
        _incoming: &WorkflowMeta<Self::WorkflowKind>,
        activity: &ObjectActivity<Self::WorkflowKind>,
    ) -> RuntimeResult<ObjectDecision<MemberDiskReply>> {
        match request {
            MemberDiskRequest::Offline(disk) => {
                self.decision(context, disk, MemberDiskEvent::DiskDown, activity)
            }
            MemberDiskRequest::Online(disk) => {
                self.decision(context, disk, MemberDiskEvent::DiskUp, activity)
            }
            MemberDiskRequest::Get(_) => Err(RuntimeError::Internal(
                "inline request reached object admission".into(),
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
