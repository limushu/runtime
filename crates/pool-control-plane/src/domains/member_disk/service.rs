use super::actor::{
    MemberDiskActivity, MemberDiskActor, MemberDiskActorDecision, PlannedRecordUpdate,
};
use super::machine::{MemberDiskInput, MemberDiskTransition, MemberDiskWorkflowKind};
use super::model::{
    MemberDiskPatch, MemberDiskRecord, MemberDiskSnapshot, MemberDiskSpec, PhysicalState,
};
use super::protocol::{MemberDiskCommand, MemberDiskResponse};
use crate::domains::pool_node::{MemberDiskIoAvailability, PoolNodeService};
use crate::domains::virtual_disk::VirtualDiskService;
use crate::kernel::{BlkId, MemberDiskId, PoolId};
use crate::ports::ControlPlaneStore;
use async_trait::async_trait;
use control_runtime::{
    spawn_service, Admission, CancelCause, ManagedService, ObjectActivity, ObjectKey, RequestRoute,
    RuntimeConfig, RuntimeError, RuntimeResult, Service, ServiceClient, ServiceId, StateCell,
    WorkflowContext, WorkflowMeta,
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
        records: Vec<MemberDiskRecord>,
        store: Arc<dyn ControlPlaneStore>,
        node: PoolNodeService,
        virtual_disk: VirtualDiskService,
        config: RuntimeConfig,
    ) -> (Self, ManagedService) {
        let worker = Arc::new(MemberDiskWorker::new(
            pool,
            records,
            store,
            node,
            virtual_disk,
        ));
        let (client, managed) = spawn_service(worker, config);
        (Self { client }, managed)
    }

    pub(crate) async fn create(&self, spec: MemberDiskSpec) -> RuntimeResult<MemberDiskSnapshot> {
        expect_snapshot(
            self.client
                .call_root(
                    format!("create member disk {}", spec.id),
                    MemberDiskCommand::Create(spec),
                )
                .await?,
        )
    }

    pub async fn update(
        &self,
        disk: MemberDiskId,
        patch: MemberDiskPatch,
    ) -> RuntimeResult<MemberDiskSnapshot> {
        expect_snapshot(
            self.client
                .call_root(
                    format!("update member disk {disk}"),
                    MemberDiskCommand::Update { disk, patch },
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
    ) -> RuntimeResult<MemberDiskSnapshot> {
        expect_snapshot(
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

    pub async fn get(&self, disk: MemberDiskId) -> RuntimeResult<MemberDiskSnapshot> {
        expect_snapshot(
            self.client
                .call_root(
                    format!("query member disk {disk}"),
                    MemberDiskCommand::Get(disk),
                )
                .await?,
        )
    }

    pub async fn list(&self) -> RuntimeResult<Vec<MemberDiskSnapshot>> {
        match self
            .client
            .call_root("list member disks", MemberDiskCommand::List)
            .await?
        {
            MemberDiskResponse::Snapshots(snapshots) => Ok(snapshots),
            response => Err(unexpected("list", response)),
        }
    }
}

fn expect_snapshot(response: MemberDiskResponse) -> RuntimeResult<MemberDiskSnapshot> {
    match response {
        MemberDiskResponse::Snapshot(snapshot) => Ok(snapshot),
        response => Err(unexpected("snapshot", response)),
    }
}

fn unexpected(operation: &str, response: MemberDiskResponse) -> RuntimeError {
    RuntimeError::Internal(format!("MemberDisk returned {response:?} to {operation}"))
}

#[derive(Debug, Default)]
struct MemberDiskDirectory {
    actors: HashMap<MemberDiskId, MemberDiskActor>,
}

impl MemberDiskDirectory {
    fn restore(records: Vec<MemberDiskRecord>) -> Self {
        Self {
            actors: records
                .into_iter()
                .map(|record| (record.id().clone(), MemberDiskActor::restore(record)))
                .collect(),
        }
    }

    fn actor(&self, disk: &MemberDiskId) -> RuntimeResult<&MemberDiskActor> {
        self.actors
            .get(disk)
            .ok_or_else(|| RuntimeError::InvalidState(format!("unknown member disk {disk}")))
    }

    fn actor_mut(&mut self, disk: &MemberDiskId) -> RuntimeResult<&mut MemberDiskActor> {
        self.actors
            .get_mut(disk)
            .ok_or_else(|| RuntimeError::InvalidState(format!("unknown member disk {disk}")))
    }
}

struct MemberDiskWorker {
    id: ServiceId,
    pool: PoolId,
    actors: StateCell<MemberDiskDirectory>,
    store: Arc<dyn ControlPlaneStore>,
    node: PoolNodeService,
    virtual_disk: VirtualDiskService,
}

impl MemberDiskWorker {
    fn new(
        pool: PoolId,
        records: Vec<MemberDiskRecord>,
        store: Arc<dyn ControlPlaneStore>,
        node: PoolNodeService,
        virtual_disk: VirtualDiskService,
    ) -> Self {
        Self {
            id: ServiceId::new(format!("pool/{pool}/member-disk")),
            pool,
            actors: StateCell::new(MemberDiskDirectory::restore(records)),
            store,
            node,
            virtual_disk,
        }
    }

    fn object_key(disk: &MemberDiskId) -> ObjectKey {
        ObjectKey::new(format!("member-disk/{disk}"))
    }

    fn snapshot(&self, disk: &MemberDiskId) -> RuntimeResult<MemberDiskSnapshot> {
        self.actors
            .read(|actors| actors.actor(disk).map(MemberDiskActor::snapshot))
    }

    fn activity(activity: &ObjectActivity<MemberDiskWorkflowKind>) -> MemberDiskActivity {
        match activity {
            ObjectActivity::Idle => MemberDiskActivity::Idle,
            ObjectActivity::Busy {
                current_kind,
                replacement_kind,
            } => MemberDiskActivity::Busy {
                current: *current_kind,
                replacement: *replacement_kind,
            },
        }
    }

    fn admit_physical(
        &self,
        disk: &MemberDiskId,
        state: PhysicalState,
        activity: &ObjectActivity<MemberDiskWorkflowKind>,
    ) -> RuntimeResult<Admission<MemberDiskResponse>> {
        let decision = self.actors.update(|actors| {
            actors
                .actor_mut(disk)?
                .admit_physical(state, Self::activity(activity))
        })?;
        Ok(match decision {
            MemberDiskActorDecision::Start => Admission::Start,
            MemberDiskActorDecision::Join => Admission::Join,
            MemberDiskActorDecision::Replace { cause } => Admission::Replace {
                cause: CancelCause::new(cause),
            },
            MemberDiskActorDecision::Complete(snapshot) => {
                Admission::Complete(MemberDiskResponse::Snapshot(snapshot))
            }
            MemberDiskActorDecision::Reject(reason) => Admission::Reject {
                reason: reason.into(),
            },
        })
    }

    async fn commit_progress(
        &self,
        context: &WorkflowContext,
        disk: &MemberDiskId,
        input: MemberDiskInput,
    ) -> RuntimeResult<MemberDiskTransition> {
        let mutation = self.actors.read(|actors| actors.actor(disk)?.plan(input))?;
        if mutation.requires_persistence() {
            self.store
                .save_member_disk(mutation.record().clone())
                .await?;
        }
        let transition = self
            .actors
            .update(|actors| actors.actor_mut(disk)?.commit(mutation))?;
        self.observe_transition(context, disk, &transition);
        // Persistence is already stable and the in-memory projection mirrors
        // it. Stop here instead of letting a stale workflow begin another step.
        if context.cancellation().is_requested() {
            return Err(RuntimeError::Cancelled);
        }
        Ok(transition)
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
                format!("{:?}", transition.input),
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

        self.commit_progress(&context, &disk, MemberDiskInput::DrainStarted)
            .await?;

        self.virtual_disk
            .evacuate_member_disk(&context, disk.clone())
            .await?;
        context.milestone("VirtualDisk evacuation reached a stable result");

        self.commit_progress(&context, &disk, MemberDiskInput::DrainCompleted)
            .await?;
        Ok(MemberDiskResponse::Snapshot(self.snapshot(&disk)?))
    }

    async fn online_workflow(
        &self,
        disk: MemberDiskId,
        context: WorkflowContext,
    ) -> RuntimeResult<MemberDiskResponse> {
        self.node
            .publish_member_disk(&context, disk.clone(), MemberDiskIoAvailability::Up)
            .await?;
        self.commit_progress(&context, &disk, MemberDiskInput::OnlineSettled)
            .await?;
        Ok(MemberDiskResponse::Snapshot(self.snapshot(&disk)?))
    }

    async fn create(&self, spec: MemberDiskSpec) -> RuntimeResult<MemberDiskResponse> {
        if spec.pool != self.pool {
            return Err(RuntimeError::Rejected(format!(
                "member disk {} belongs to pool {}, not {}",
                spec.id, spec.pool, self.pool
            )));
        }
        if self
            .actors
            .read(|actors| actors.actors.contains_key(&spec.id))
        {
            return Err(RuntimeError::Rejected(format!(
                "member disk {} already exists",
                spec.id
            )));
        }
        let record = MemberDiskRecord::new(spec);
        self.store.save_member_disk(record.clone()).await?;
        let snapshot = self.actors.update(|actors| {
            let actor = MemberDiskActor::restore(record);
            let snapshot = actor.snapshot();
            actors.actors.insert(snapshot.spec.id.clone(), actor);
            snapshot
        });
        Ok(MemberDiskResponse::Snapshot(snapshot))
    }

    async fn update(
        &self,
        disk: MemberDiskId,
        patch: MemberDiskPatch,
    ) -> RuntimeResult<MemberDiskResponse> {
        let update = self
            .actors
            .read(|actors| actors.actor(&disk).map(|actor| actor.plan_patch(patch)))?;
        self.commit_record_update(&disk, update).await?;
        Ok(MemberDiskResponse::Snapshot(self.snapshot(&disk)?))
    }

    async fn allocate(&self, disk: MemberDiskId) -> RuntimeResult<MemberDiskResponse> {
        let (update, blk) = self
            .actors
            .read(|actors| actors.actor(&disk)?.plan_allocate())?;
        self.commit_record_update(&disk, update).await?;
        Ok(MemberDiskResponse::Allocated(blk))
    }

    async fn release(&self, disk: MemberDiskId, blk: BlkId) -> RuntimeResult<MemberDiskResponse> {
        let update = self
            .actors
            .read(|actors| actors.actor(&disk)?.plan_release(&blk))?;
        self.commit_record_update(&disk, update).await?;
        Ok(MemberDiskResponse::Released)
    }

    async fn commit_record_update(
        &self,
        disk: &MemberDiskId,
        update: PlannedRecordUpdate,
    ) -> RuntimeResult<()> {
        self.store.save_member_disk(update.record().clone()).await?;
        self.actors
            .update(|actors| actors.actor_mut(disk)?.commit_record(update))
    }

    async fn delete(&self, disk: MemberDiskId) -> RuntimeResult<MemberDiskResponse> {
        if !self.actors.read(|actors| {
            actors
                .actor(&disk)
                .map(MemberDiskActor::can_delete)
                .unwrap_or(false)
        }) {
            return Err(RuntimeError::Rejected(
                "member disk must be Removed and own no BLKs before deletion".into(),
            ));
        }
        self.store.delete_member_disk(&self.pool, &disk).await?;
        self.actors.update(|actors| {
            actors.actors.remove(&disk);
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

    fn route(&self, request: &Self::Request) -> RequestRoute<Self::WorkflowKind> {
        let workflow = |disk: &MemberDiskId, kind, label: String| {
            RequestRoute::Workflow(WorkflowMeta::object(Self::object_key(disk), kind, label))
        };
        match request {
            MemberDiskCommand::Get(_) | MemberDiskCommand::List => RequestRoute::Untracked,
            MemberDiskCommand::ApplyPhysical { disk, state } => {
                let kind = match state {
                    PhysicalState::Down => MemberDiskWorkflowKind::Offline,
                    PhysicalState::Up => MemberDiskWorkflowKind::Online,
                };
                match workflow(disk, kind, format!("apply {state:?} fact for {disk}")) {
                    RequestRoute::Workflow(meta) => {
                        RequestRoute::Workflow(meta.continue_when_orphaned())
                    }
                    RequestRoute::Untracked => unreachable!(),
                }
            }
            MemberDiskCommand::Create(spec) => workflow(
                &spec.id,
                MemberDiskWorkflowKind::Metadata,
                format!("create member disk {}", spec.id),
            ),
            MemberDiskCommand::Update { disk, .. } => workflow(
                disk,
                MemberDiskWorkflowKind::Metadata,
                format!("update member disk {disk}"),
            ),
            MemberDiskCommand::Delete(disk) => workflow(
                disk,
                MemberDiskWorkflowKind::Metadata,
                format!("delete member disk {disk}"),
            ),
            MemberDiskCommand::Allocate(disk) => workflow(
                disk,
                MemberDiskWorkflowKind::Metadata,
                format!("allocate a BLK from {disk}"),
            ),
            MemberDiskCommand::Release { disk, blk } => workflow(
                disk,
                MemberDiskWorkflowKind::Metadata,
                format!("release BLK {blk} from {disk}"),
            ),
        }
    }

    fn admit(
        &self,
        _context: &WorkflowContext,
        request: &Self::Request,
        activity: &ObjectActivity<Self::WorkflowKind>,
    ) -> RuntimeResult<Admission<MemberDiskResponse>> {
        match request {
            MemberDiskCommand::ApplyPhysical { disk, state } => {
                self.admit_physical(disk, *state, activity)
            }
            MemberDiskCommand::Create(_)
            | MemberDiskCommand::Update { .. }
            | MemberDiskCommand::Delete(_)
            | MemberDiskCommand::Allocate(_)
            | MemberDiskCommand::Release { .. } => Ok(if activity.is_idle() {
                Admission::Start
            } else {
                Admission::Queue
            }),
            MemberDiskCommand::Get(_) | MemberDiskCommand::List => Err(RuntimeError::Internal(
                "untracked MemberDisk query reached admission".into(),
            )),
        }
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
            MemberDiskCommand::Create(spec) => self.create(spec).await,
            MemberDiskCommand::Update { disk, patch } => self.update(disk, patch).await,
            MemberDiskCommand::Delete(disk) => self.delete(disk).await,
            MemberDiskCommand::Allocate(disk) => self.allocate(disk).await,
            MemberDiskCommand::Release { disk, blk } => self.release(disk, blk).await,
            MemberDiskCommand::Get(disk) => Ok(MemberDiskResponse::Snapshot(self.snapshot(&disk)?)),
            MemberDiskCommand::List => {
                Ok(MemberDiskResponse::Snapshots(self.actors.read(|actors| {
                    actors
                        .actors
                        .values()
                        .map(MemberDiskActor::snapshot)
                        .collect()
                })))
            }
        }
    }
}
