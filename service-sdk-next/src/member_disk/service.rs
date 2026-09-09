use super::model::MemberDisk;
use super::{
    DiskUuid, MemberDiskCommand, MemberDiskMetadata, MemberDiskMutation, MemberDiskQuery,
    MemberDiskQueryReply, MemberDiskReply, MemberDiskSeed, MemberDiskServiceError, MemberDiskState,
    PoolNodes, VirtualDisks,
};
use crate::service::{
    CommandActivity, CommandContext, CommandDecision, Service, TaskSnapshot, TaskState,
};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};
use tokio::sync::Mutex as AsyncMutex;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct MemberDiskConfig {
    pub recovery_window: Duration,
    pub mandatory_retry_delay: Duration,
}

impl Default for MemberDiskConfig {
    fn default() -> Self {
        Self {
            recovery_window: Duration::from_secs(30),
            mandatory_retry_delay: Duration::from_millis(200),
        }
    }
}

/// One Pool's MemberDisk domain service.
///
/// `disks` is the private, authoritative in-memory projection. External code
/// can only use `MemberDiskQuery` and `MemberDiskCommand` through the SDK.
pub struct MemberDiskService {
    pub(super) disks: RwLock<HashMap<DiskUuid, MemberDisk>>,
    pub(super) commit_gate: AsyncMutex<()>,
    pub(super) metadata: Arc<dyn MemberDiskMetadata>,
    pub(super) pool_nodes: Arc<dyn PoolNodes>,
    pub(super) virtual_disks: Arc<dyn VirtualDisks>,
    pub(super) config: MemberDiskConfig,
    tasks: Mutex<HashMap<DiskUuid, DomainTask>>,
}

struct DomainTask {
    id: u64,
    kind: &'static str,
    disk: DiskUuid,
    progress: u8,
    detail: String,
    cancellation: CancellationToken,
    failed: bool,
}

impl MemberDiskService {
    pub fn new(
        seeds: impl IntoIterator<Item = MemberDiskSeed>,
        metadata: Arc<dyn MemberDiskMetadata>,
        pool_nodes: Arc<dyn PoolNodes>,
        virtual_disks: Arc<dyn VirtualDisks>,
        config: MemberDiskConfig,
    ) -> Self {
        let disks = seeds
            .into_iter()
            .map(|seed| (seed.uuid.clone(), MemberDisk::from_seed(seed)))
            .collect();
        Self {
            disks: RwLock::new(disks),
            commit_gate: AsyncMutex::new(()),
            metadata,
            pool_nodes,
            virtual_disks,
            config,
            tasks: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn state(
        &self,
        disk: &DiskUuid,
    ) -> Result<(MemberDiskState, bool), MemberDiskServiceError> {
        let disks = self.disks.read().expect("MemberDisk table poisoned");
        let member = disks
            .get(disk)
            .ok_or_else(|| MemberDiskServiceError::UnknownDisk(disk.clone()))?;
        Ok((member.state(), member.shrinking()))
    }

    fn reply(&self, disk: &DiskUuid) -> Result<MemberDiskReply, MemberDiskServiceError> {
        let (state, _) = self.state(disk)?;
        Ok(MemberDiskReply {
            disk: disk.clone(),
            state,
        })
    }

    /// The sole SDB-first mutation boundary:
    /// validate current object -> commit typed mutation -> publish in memory.
    pub(super) async fn commit(
        &self,
        disk: &DiskUuid,
        mutation: MemberDiskMutation,
    ) -> Result<bool, MemberDiskServiceError> {
        let _commit = self.commit_gate.lock().await;
        let changed = {
            let disks = self.disks.read().expect("MemberDisk table poisoned");
            disks
                .get(disk)
                .ok_or_else(|| MemberDiskServiceError::UnknownDisk(disk.clone()))?
                .validate(&mutation)?
        };
        if !changed {
            return Ok(false);
        }

        self.metadata
            .commit(disk, &mutation)
            .await
            .map_err(MemberDiskServiceError::Metadata)?;

        self.disks
            .write()
            .expect("MemberDisk table poisoned")
            .get_mut(disk)
            .expect("validated MemberDisk disappeared")
            .apply_committed(&mutation);
        Ok(true)
    }

    fn begin_task(&self, command: &MemberDiskCommand, context: &CommandContext<DiskUuid>) {
        let disk = command.disk().clone();
        self.tasks.lock().expect("task table poisoned").insert(
            disk.clone(),
            DomainTask {
                id: context.execution_id(),
                kind: command.kind(),
                disk,
                progress: 0,
                detail: "accepted".into(),
                cancellation: context.cancellation().clone(),
                failed: false,
            },
        );
    }

    pub(super) fn task_progress(&self, disk: &DiskUuid, progress: u8, detail: &'static str) {
        if let Some(task) = self
            .tasks
            .lock()
            .expect("task table poisoned")
            .get_mut(disk)
        {
            task.progress = progress;
            task.detail = detail.into();
        }
    }

    fn finish_task(
        &self,
        disk: &DiskUuid,
        result: &Result<MemberDiskReply, MemberDiskServiceError>,
    ) {
        let mut tasks = self.tasks.lock().expect("task table poisoned");
        if result.is_ok() || matches!(result, Err(MemberDiskServiceError::Cancelled)) {
            tasks.remove(disk);
        } else if let Some(task) = tasks.get_mut(disk) {
            task.failed = true;
            task.detail = result.as_ref().unwrap_err().to_string();
        }
    }

    fn command_is_stable(&self, command: &MemberDiskCommand) -> bool {
        let Ok((state, shrinking)) = self.state(command.disk()) else {
            return false;
        };
        match command {
            MemberDiskCommand::DiskDown { .. } => state == MemberDiskState::Removed,
            MemberDiskCommand::DiskUp { .. } => state == MemberDiskState::UpActive && !shrinking,
            MemberDiskCommand::Shrink { .. } => state == MemberDiskState::Removed,
        }
    }
}

#[async_trait]
impl Service for MemberDiskService {
    type Query = MemberDiskQuery;
    type QueryReply = MemberDiskQueryReply;
    type Command = MemberDiskCommand;
    type CommandReply = MemberDiskReply;
    type Key = DiskUuid;
    type Error = MemberDiskServiceError;

    fn name(&self) -> &'static str {
        "member-disk"
    }

    fn command_key(&self, command: &Self::Command) -> Option<Self::Key> {
        Some(command.disk().clone())
    }

    fn admit(
        &self,
        command: &Self::Command,
        activity: CommandActivity<'_, Self::Command>,
    ) -> Result<CommandDecision<Self::CommandReply>, Self::Error> {
        self.state(command.disk())?;

        if matches!(activity, CommandActivity::Idle) && self.command_is_stable(command) {
            return Ok(CommandDecision::Complete(self.reply(command.disk())?));
        }

        let CommandActivity::Running { latest, .. } = activity else {
            return Ok(CommandDecision::Run);
        };

        if latest.kind() == command.kind() {
            return Ok(CommandDecision::Join);
        }

        // Shrink is a durable removal target. UP waits behind it and becomes a
        // rejoin after removal; DOWN preempts it because stopping IO is urgent.
        if matches!(latest, MemberDiskCommand::Shrink { .. })
            && matches!(command, MemberDiskCommand::DiskUp { .. })
        {
            return Ok(CommandDecision::Queue);
        }

        Ok(CommandDecision::Replace {
            cause: format!("{} supersedes {}", command.kind(), latest.kind()),
        })
    }

    async fn handle_query(&self, query: Self::Query) -> Result<Self::QueryReply, Self::Error> {
        let disks = self.disks.read().expect("MemberDisk table poisoned");
        match query {
            MemberDiskQuery::Get(disk) => disks
                .get(&disk)
                .map(|member| MemberDiskQueryReply::One(member.view()))
                .ok_or(MemberDiskServiceError::UnknownDisk(disk)),
            MemberDiskQuery::List => {
                let mut views: Vec<_> = disks.values().map(MemberDisk::view).collect();
                views.sort_by_key(|view| view.uuid.to_string());
                Ok(MemberDiskQueryReply::List(views))
            }
        }
    }

    async fn handle_command(
        self: Arc<Self>,
        command: Self::Command,
        context: CommandContext<Self::Key>,
    ) -> Result<Self::CommandReply, Self::Error> {
        let disk = command.disk().clone();
        self.begin_task(&command, &context);
        context.milestone("business task registered");
        let result = self.drive_to_stable(&command, &context).await;
        self.finish_task(&disk, &result);
        context.milestone(match &result {
            Ok(_) => "business task completed",
            Err(MemberDiskServiceError::Cancelled) => "business task cancelled",
            Err(_) => "business task failed",
        });
        result
    }

    fn task_snapshots(&self) -> Vec<TaskSnapshot> {
        let mut snapshots: Vec<_> = self
            .tasks
            .lock()
            .expect("task table poisoned")
            .values()
            .map(|task| TaskSnapshot {
                id: task.id.to_string(),
                kind: task.kind.into(),
                subject: task.disk.to_string(),
                state: if task.failed {
                    TaskState::Failed
                } else if task.cancellation.is_cancelled() {
                    TaskState::Cancelling
                } else {
                    TaskState::Running
                },
                progress: Some(task.progress),
                detail: Some(task.detail.clone()),
            })
            .collect();
        snapshots.sort_by(|left, right| left.id.cmp(&right.id));
        snapshots
    }
}
