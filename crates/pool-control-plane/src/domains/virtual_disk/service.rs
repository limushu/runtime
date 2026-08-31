use super::protocol::{
    EvacuationResult, VirtualDiskCommand, VirtualDiskResponse, VirtualDiskStats,
};
use crate::kernel::{MemberDiskId, PoolId};
use async_trait::async_trait;
use control_runtime::{
    spawn_service, Admission, ManagedService, ObjectActivity, ObjectKey, RequestRoute,
    RuntimeConfig, RuntimeError, RuntimeResult, Service, ServiceClient, ServiceId, StateCell,
    WorkflowContext, WorkflowMeta,
};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct VirtualDiskService {
    client: ServiceClient<VirtualDiskCommand>,
}

impl VirtualDiskService {
    pub(crate) fn spawn(pool: &PoolId, config: RuntimeConfig) -> (Self, ManagedService) {
        let worker = Arc::new(VirtualDiskWorker::new(pool));
        let (client, managed) = spawn_service(worker, config);
        (Self { client }, managed)
    }

    pub async fn evacuate_member_disk(
        &self,
        context: &WorkflowContext,
        disk: MemberDiskId,
    ) -> RuntimeResult<EvacuationResult> {
        match self
            .client
            .call(
                context,
                format!("evacuate BGs on {disk}"),
                VirtualDiskCommand::EvacuateMemberDisk(disk),
            )
            .await?
        {
            VirtualDiskResponse::Evacuated(result) => Ok(result),
            VirtualDiskResponse::Stats(_) => Err(RuntimeError::Internal(
                "VirtualDisk returned Stats to evacuate_member_disk".into(),
            )),
        }
    }

    pub async fn stats(&self) -> RuntimeResult<VirtualDiskStats> {
        match self
            .client
            .call_root("query virtual disk stats", VirtualDiskCommand::Stats)
            .await?
        {
            VirtualDiskResponse::Stats(stats) => Ok(stats),
            VirtualDiskResponse::Evacuated(_) => Err(RuntimeError::Internal(
                "VirtualDisk returned Evacuated to stats".into(),
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VirtualDiskWorkflowKind {
    EvacuateMemberDisk,
}

struct VirtualDiskWorker {
    id: ServiceId,
    state: StateCell<VirtualDiskStats>,
}

impl VirtualDiskWorker {
    const BG_COUNT: usize = 4;

    fn new(pool: &PoolId) -> Self {
        Self {
            id: ServiceId::new(format!("pool/{pool}/virtual-disk")),
            state: StateCell::new(VirtualDiskStats::default()),
        }
    }

    async fn evacuate_member_disk(
        &self,
        _disk: MemberDiskId,
        context: WorkflowContext,
    ) -> RuntimeResult<VirtualDiskResponse> {
        self.state.update(|state| state.started += 1);
        let mut committed = 0;

        // Cancellation is interpreted once by the VD scheduler, not by every
        // parent workflow. An issued BG step settles before cancellation is
        // returned; unissued BGs are not scheduled.
        for _ in 0..Self::BG_COUNT {
            tokio::select! {
                biased;
                _ = context.cancellation().requested() => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    self.state.update(|state| {
                        state.cancelled += 1;
                        state.stable_stops += 1;
                    });
                    return Err(RuntimeError::Cancelled);
                }
                _ = tokio::time::sleep(Duration::from_millis(25)) => {
                    committed += 1;
                    self.state.update(|state| state.bg_completed += 1);
                }
            }
        }

        self.state.update(|state| state.completed += 1);
        Ok(VirtualDiskResponse::Evacuated(EvacuationResult {
            bg_count: committed,
        }))
    }
}

#[async_trait]
impl Service for VirtualDiskWorker {
    type Request = VirtualDiskCommand;
    type WorkflowKind = VirtualDiskWorkflowKind;

    fn id(&self) -> ServiceId {
        self.id.clone()
    }

    fn route(&self, request: &Self::Request) -> RequestRoute<Self::WorkflowKind> {
        match request {
            VirtualDiskCommand::Stats => RequestRoute::Untracked,
            VirtualDiskCommand::EvacuateMemberDisk(disk) => {
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
    ) -> RuntimeResult<Admission<VirtualDiskResponse>> {
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
    ) -> RuntimeResult<VirtualDiskResponse> {
        match request {
            VirtualDiskCommand::EvacuateMemberDisk(disk) => {
                self.evacuate_member_disk(disk, context).await
            }
            VirtualDiskCommand::Stats => {
                Ok(VirtualDiskResponse::Stats(self.state.read(|state| *state)))
            }
        }
    }
}
