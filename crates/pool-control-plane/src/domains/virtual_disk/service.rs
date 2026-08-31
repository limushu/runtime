use super::protocol::{VirtualDiskReply, VirtualDiskRequest, VirtualDiskStats};
use crate::kernel::MemberDiskId;
use async_trait::async_trait;
use control_runtime::{
    Admission, ObjectActivity, ObjectKey, RequestRoute, RuntimeResult, Service, StateCell,
    WorkflowContext, WorkflowMeta,
};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VirtualDiskWorkflowKind {
    EvacuateMemberDisk,
}

#[derive(Debug, Default)]
pub struct VirtualDiskService {
    state: StateCell<VirtualDiskStats>,
}

impl VirtualDiskService {
    const BG_COUNT: usize = 4;

    pub fn new() -> Self {
        Self::default()
    }

    async fn evacuate_member_disk(
        &self,
        _disk: MemberDiskId,
        context: WorkflowContext,
    ) -> RuntimeResult<VirtualDiskReply> {
        self.state.update(|state| state.started += 1);
        let mut committed = 0;

        for _ in 0..Self::BG_COUNT {
            tokio::select! {
                biased;
                _ = context.cancellation().requested() => {
                    // An issued BG operation is not dropped. Its stable result
                    // is awaited before the service reports DrainStopped.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    self.state.update(|state| {
                        state.cancelled += 1;
                        state.stable_stops += 1;
                    });
                    return Ok(VirtualDiskReply::DrainStopped {
                        committed,
                        skipped: Self::BG_COUNT - committed,
                    });
                }
                _ = tokio::time::sleep(Duration::from_millis(25)) => {
                    committed += 1;
                    self.state.update(|state| state.bg_completed += 1);
                }
            }
        }

        self.state.update(|state| state.completed += 1);
        Ok(VirtualDiskReply::Evacuated {
            bg_count: Self::BG_COUNT,
        })
    }
}

#[async_trait]
impl Service for VirtualDiskService {
    type Request = VirtualDiskRequest;
    type WorkflowKind = VirtualDiskWorkflowKind;

    fn route(&self, request: &Self::Request) -> RequestRoute<Self::WorkflowKind> {
        match request {
            VirtualDiskRequest::Stats => RequestRoute::Untracked,
            VirtualDiskRequest::EvacuateMemberDisk(disk) => {
                RequestRoute::Workflow(WorkflowMeta::object(
                    ObjectKey::new(format!("member-disk/{disk}")),
                    VirtualDiskWorkflowKind::EvacuateMemberDisk,
                    format!("evacuate all BGs on {disk}"),
                ))
            }
        }
    }

    fn admit(
        &self,
        _context: &WorkflowContext,
        _request: &Self::Request,
        activity: &ObjectActivity<Self::WorkflowKind>,
    ) -> RuntimeResult<Admission<VirtualDiskReply>> {
        Ok(if activity.is_idle() {
            Admission::Start
        } else {
            Admission::Join
        })
    }

    async fn handle(
        &self,
        request: Self::Request,
        context: WorkflowContext,
    ) -> RuntimeResult<VirtualDiskReply> {
        match request {
            VirtualDiskRequest::EvacuateMemberDisk(disk) => {
                self.evacuate_member_disk(disk, context).await
            }
            VirtualDiskRequest::Stats => {
                Ok(VirtualDiskReply::Stats(self.state.read(|state| *state)))
            }
        }
    }
}
