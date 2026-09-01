use super::machine::{
    MemberDiskEffect, MemberDiskEvent, MemberDiskTransition, MemberDiskWorkflowKind,
};
use super::model::{MemberDisk, PhysicalState};
use super::protocol::{MemberDiskCommand, MemberDiskResponse};
use crate::domains::pool_node::{MemberDiskIoAvailability, PoolNodeService};
use crate::domains::virtual_disk::VirtualDiskService;
use crate::kernel::{BlkId, MemberDiskId, PoolId};
use crate::ports::ControlPlaneStore;
use async_trait::async_trait;
use control_runtime::{
    spawn_service, ManagedService, ObjectKey, RequestPlan, RuntimeConfig, RuntimeError,
    RuntimeResult, Service, ServiceClient, ServiceId, StateCell, WorkflowContext, WorkflowMeta,
};
use std::collections::HashMap;
use std::sync::Arc;

/// Developer-facing MemberDisk capability. Commands, channels and oneshot
/// response enums remain private to the domain.
#[derive(Clone)]
pub struct MemberDiskService {
    client: ServiceClient<MemberDiskCommand>,
}

impl MemberDiskService {
    pub(crate) fn spawn(
        pool: PoolId,
        disks: Vec<MemberDisk>,
        store: Arc<dyn ControlPlaneStore>,
        node: PoolNodeService,
        virtual_disk: VirtualDiskService,
        config: RuntimeConfig,
    ) -> (Self, ManagedService) {
        let worker = Arc::new(MemberDiskWorker::new(
            pool,
            disks,
            store,
            node,
            virtual_disk,
        ));
        let (client, managed) = spawn_service(worker, config);
        (Self { client }, managed)
    }

    pub(crate) async fn create(&self, disk: MemberDisk) -> RuntimeResult<MemberDisk> {
        expect_disk(
            self.client
                .call_root(
                    format!("create member disk {}", disk.id()),
                    MemberDiskCommand::Create(disk),
                )
                .await?,
        )
    }

    pub(crate) async fn delete(&self, disk: MemberDiskId) -> RuntimeResult<()> {
        match self
            .client
            .call_root(
                format!("delete member disk {disk}"),
                MemberDiskCommand::Delete(disk),
            )
            .await?
        {
            MemberDiskResponse::Deleted => Ok(()),
            response => Err(unexpected("delete", response)),
        }
    }

    pub async fn apply_physical(
        &self,
        disk: MemberDiskId,
        state: PhysicalState,
    ) -> RuntimeResult<MemberDisk> {
        expect_disk(
            self.client
                .call_root(
                    format!("DiskMap observed {disk} as {state:?}"),
                    MemberDiskCommand::ApplyPhysical { disk, state },
                )
                .await?,
        )
    }

    pub async fn allocate(&self, disk: MemberDiskId) -> RuntimeResult<BlkId> {
        match self
            .client
            .call_root(
                format!("allocate BLK from {disk}"),
                MemberDiskCommand::Allocate(disk),
            )
            .await?
        {
            MemberDiskResponse::Allocated(blk) => Ok(blk),
            response => Err(unexpected("allocate", response)),
        }
    }

    pub async fn release(&self, disk: MemberDiskId, blk: BlkId) -> RuntimeResult<()> {
        match self
            .client
            .call_root(
                format!("release BLK {blk} from {disk}"),
                MemberDiskCommand::Release { disk, blk },
            )
            .await?
        {
            MemberDiskResponse::Released => Ok(()),
            response => Err(unexpected("release", response)),
        }
    }

    pub async fn get(&self, disk: MemberDiskId) -> RuntimeResult<MemberDisk> {
        expect_disk(
            self.client
                .call_root(
                    format!("query member disk {disk}"),
                    MemberDiskCommand::Get(disk),
                )
                .await?,
        )
    }

    pub async fn list(&self) -> RuntimeResult<Vec<MemberDisk>> {
        match self
            .client
            .call_root("list member disks", MemberDiskCommand::List)
            .await?
        {
            MemberDiskResponse::Disks(disks) => Ok(disks),
            response => Err(unexpected("list", response)),
        }
    }
}

fn expect_disk(response: MemberDiskResponse) -> RuntimeResult<MemberDisk> {
    match response {
        MemberDiskResponse::Disk(disk) => Ok(disk),
        response => Err(unexpected("member disk", response)),
    }
}

fn unexpected(operation: &str, response: MemberDiskResponse) -> RuntimeError {
    RuntimeError::Internal(format!("MemberDisk returned {response:?} to {operation}"))
}

fn find_disk<'a>(
    disks: &'a HashMap<MemberDiskId, MemberDisk>,
    disk: &MemberDiskId,
) -> RuntimeResult<&'a MemberDisk> {
    disks
        .get(disk)
        .ok_or_else(|| RuntimeError::InvalidState(format!("unknown member disk {disk}")))
}

fn find_disk_mut<'a>(
    disks: &'a mut HashMap<MemberDiskId, MemberDisk>,
    disk: &MemberDiskId,
) -> RuntimeResult<&'a mut MemberDisk> {
    disks
        .get_mut(disk)
        .ok_or_else(|| RuntimeError::InvalidState(format!("unknown member disk {disk}")))
}

struct MemberDiskWorker {
    id: ServiceId,
    pool: PoolId,
    disks: StateCell<HashMap<MemberDiskId, MemberDisk>>,
    store: Arc<dyn ControlPlaneStore>,
    node: PoolNodeService,
    virtual_disk: VirtualDiskService,
}

impl MemberDiskWorker {
    fn new(
        pool: PoolId,
        disks: Vec<MemberDisk>,
        store: Arc<dyn ControlPlaneStore>,
        node: PoolNodeService,
        virtual_disk: VirtualDiskService,
    ) -> Self {
        Self {
            id: ServiceId::new(format!("pool/{pool}/member-disk")),
            pool,
            disks: StateCell::new(
                disks
                    .into_iter()
                    .map(|disk| (disk.id().clone(), disk))
                    .collect(),
            ),
            store,
            node,
            virtual_disk,
        }
    }

    fn object_key(disk: &MemberDiskId) -> ObjectKey {
        ObjectKey::new(format!("member-disk/{disk}"))
    }

    fn disk(&self, disk: &MemberDiskId) -> RuntimeResult<MemberDisk> {
        self.disks.read(|disks| find_disk(disks, disk).cloned())
    }

    fn plan_physical(
        &self,
        disk: &MemberDiskId,
        state: PhysicalState,
    ) -> RuntimeResult<RequestPlan<MemberDiskWorkflowKind, MemberDiskResponse>> {
        let transition = self.disks.update(|disks| {
            find_disk_mut(disks, disk)?.apply_event(MemberDiskEvent::Physical(state))
        })?;
        Ok(match transition.effect {
            MemberDiskEffect::Ensure(kind) => RequestPlan::Ensure(
                WorkflowMeta::object(
                    Self::object_key(disk),
                    kind,
                    format!("apply {state:?} fact for {disk}"),
                )
                .continue_when_orphaned(),
            ),
            MemberDiskEffect::None => {
                RequestPlan::Complete(MemberDiskResponse::Disk(self.disk(disk)?))
            }
            MemberDiskEffect::Reject(reason) => RequestPlan::Reject { reason },
        })
    }

    async fn apply_persisted_event(
        &self,
        context: &WorkflowContext,
        disk: &MemberDiskId,
        event: MemberDiskEvent,
    ) -> RuntimeResult<MemberDiskTransition> {
        if context.cancellation().is_requested() {
            return Err(RuntimeError::Cancelled);
        }

        let (expected_revision, next, mut transition) = self.disks.read(|disks| {
            let current = find_disk(disks, disk)?;
            let expected_revision = current.revision();
            let mut next = current.clone();
            let transition = next.apply_event(event)?;
            Ok((expected_revision, next, transition))
        })?;

        self.store.save_member_disk(next.clone()).await?;
        let (from, to) = self.disks.update(|disks| {
            let current = find_disk_mut(disks, disk)?;
            let from = current.state();
            current.commit_persisted(expected_revision, next)?;
            Ok((from, current.state()))
        })?;
        transition.from = from;
        transition.to = to;
        self.observe_transition(context, disk, &transition);

        // The durable decision is stable. A replacement may now stop this
        // workflow before it begins another business step.
        if context.cancellation().is_requested() {
            return Err(RuntimeError::Cancelled);
        }
        Ok(transition)
    }

    async fn begin_drain(
        &self,
        context: &WorkflowContext,
        disk: &MemberDiskId,
    ) -> RuntimeResult<()> {
        self.apply_persisted_event(context, disk, MemberDiskEvent::DrainStarted)
            .await
            .map(|_| ())
    }

    async fn finish_drain(
        &self,
        context: &WorkflowContext,
        disk: &MemberDiskId,
    ) -> RuntimeResult<()> {
        self.apply_persisted_event(context, disk, MemberDiskEvent::DrainCompleted)
            .await
            .map(|_| ())
    }

    async fn finish_online(
        &self,
        context: &WorkflowContext,
        disk: &MemberDiskId,
    ) -> RuntimeResult<()> {
        self.apply_persisted_event(context, disk, MemberDiskEvent::OnlineSettled)
            .await
            .map(|_| ())
    }

    fn observe_transition(
        &self,
        context: &WorkflowContext,
        disk: &MemberDiskId,
        transition: &MemberDiskTransition,
    ) {
        if transition.from != transition.to {
            context.state_transition(
                Self::object_key(disk),
                format!("{:?}", transition.from),
                format!("{:?}", transition.to),
                format!("{:?}", transition.event),
            );
        }
    }

    async fn offline_workflow(
        &self,
        disk: MemberDiskId,
        context: WorkflowContext,
    ) -> RuntimeResult<MemberDiskResponse> {
        self.node
            .publish_member_disk(&context, disk.clone(), MemberDiskIoAvailability::Down)
            .await?;
        context.milestone("PoolNode accepted the MemberDisk Down projection");

        self.begin_drain(&context, &disk).await?;

        self.virtual_disk
            .evacuate_member_disk(&context, disk.clone())
            .await?;
        context.milestone("VirtualDisk evacuation reached a stable result");

        self.finish_drain(&context, &disk).await?;
        Ok(MemberDiskResponse::Disk(self.disk(&disk)?))
    }

    async fn online_workflow(
        &self,
        disk: MemberDiskId,
        context: WorkflowContext,
    ) -> RuntimeResult<MemberDiskResponse> {
        self.node
            .publish_member_disk(&context, disk.clone(), MemberDiskIoAvailability::Up)
            .await?;
        self.finish_online(&context, &disk).await?;
        Ok(MemberDiskResponse::Disk(self.disk(&disk)?))
    }

    async fn create(&self, disk: MemberDisk) -> RuntimeResult<MemberDiskResponse> {
        if disk.pool() != &self.pool {
            return Err(RuntimeError::Rejected(format!(
                "member disk {} belongs to pool {}, not {}",
                disk.id(),
                disk.pool(),
                self.pool
            )));
        }
        if self.disks.read(|disks| disks.contains_key(disk.id())) {
            return Err(RuntimeError::Rejected(format!(
                "member disk {} already exists",
                disk.id()
            )));
        }
        self.store.save_member_disk(disk.clone()).await?;
        let result = disk.clone();
        self.disks.update(|disks| {
            disks.insert(disk.id().clone(), disk);
        });
        Ok(MemberDiskResponse::Disk(result))
    }

    async fn allocate(&self, disk: MemberDiskId) -> RuntimeResult<MemberDiskResponse> {
        let (expected_revision, next, blk) = self.disks.read(|disks| {
            let current = find_disk(disks, &disk)?;
            let expected_revision = current.revision();
            let mut next = current.clone();
            let blk = next.allocate_one()?;
            Ok((expected_revision, next, blk))
        })?;
        self.persist_candidate(&disk, expected_revision, next)
            .await?;
        Ok(MemberDiskResponse::Allocated(blk))
    }

    async fn release(&self, disk: MemberDiskId, blk: BlkId) -> RuntimeResult<MemberDiskResponse> {
        let (expected_revision, next) = self.disks.read(|disks| {
            let current = find_disk(disks, &disk)?;
            let expected_revision = current.revision();
            let mut next = current.clone();
            next.release(&blk)?;
            Ok((expected_revision, next))
        })?;
        self.persist_candidate(&disk, expected_revision, next)
            .await?;
        Ok(MemberDiskResponse::Released)
    }

    async fn persist_candidate(
        &self,
        disk: &MemberDiskId,
        expected_revision: u64,
        next: MemberDisk,
    ) -> RuntimeResult<()> {
        self.store.save_member_disk(next.clone()).await?;
        self.disks
            .update(|disks| find_disk_mut(disks, disk)?.commit_persisted(expected_revision, next))
    }

    async fn delete(&self, disk: MemberDiskId) -> RuntimeResult<MemberDiskResponse> {
        if !self.disks.read(|disks| {
            find_disk(disks, &disk)
                .map(MemberDisk::can_delete)
                .unwrap_or(false)
        }) {
            return Err(RuntimeError::Rejected(
                "member disk must be Removed and own no BLKs before deletion".into(),
            ));
        }
        self.store.delete_member_disk(&self.pool, &disk).await?;
        self.disks.update(|disks| {
            disks.remove(&disk);
        });
        Ok(MemberDiskResponse::Deleted)
    }
}

#[async_trait]
impl Service for MemberDiskWorker {
    type Request = MemberDiskCommand;
    type WorkflowKind = MemberDiskWorkflowKind;

    fn id(&self) -> ServiceId {
        self.id.clone()
    }

    fn plan(
        &self,
        request: &Self::Request,
    ) -> RuntimeResult<RequestPlan<Self::WorkflowKind, MemberDiskResponse>> {
        let metadata = |disk: &MemberDiskId, label: String| {
            RequestPlan::Enqueue(WorkflowMeta::object(
                Self::object_key(disk),
                MemberDiskWorkflowKind::Metadata,
                label,
            ))
        };
        Ok(match request {
            MemberDiskCommand::Get(_) | MemberDiskCommand::List => RequestPlan::Inline,
            MemberDiskCommand::ApplyPhysical { disk, state } => self.plan_physical(disk, *state)?,
            MemberDiskCommand::Create(disk) => {
                metadata(disk.id(), format!("create member disk {}", disk.id()))
            }
            MemberDiskCommand::Delete(disk) => metadata(disk, format!("delete member disk {disk}")),
            MemberDiskCommand::Allocate(disk) => {
                metadata(disk, format!("allocate a BLK from {disk}"))
            }
            MemberDiskCommand::Release { disk, blk } => {
                metadata(disk, format!("release BLK {blk} from {disk}"))
            }
        })
    }

    async fn handle(
        &self,
        request: Self::Request,
        context: WorkflowContext,
    ) -> RuntimeResult<MemberDiskResponse> {
        match request {
            MemberDiskCommand::ApplyPhysical {
                disk,
                state: PhysicalState::Down,
            } => self.offline_workflow(disk, context).await,
            MemberDiskCommand::ApplyPhysical {
                disk,
                state: PhysicalState::Up,
            } => self.online_workflow(disk, context).await,
            MemberDiskCommand::Create(disk) => self.create(disk).await,
            MemberDiskCommand::Delete(disk) => self.delete(disk).await,
            MemberDiskCommand::Allocate(disk) => self.allocate(disk).await,
            MemberDiskCommand::Release { disk, blk } => self.release(disk, blk).await,
            MemberDiskCommand::Get(disk) => Ok(MemberDiskResponse::Disk(self.disk(&disk)?)),
            MemberDiskCommand::List => Ok(MemberDiskResponse::Disks(
                self.disks.read(|disks| disks.values().cloned().collect()),
            )),
        }
    }
}
