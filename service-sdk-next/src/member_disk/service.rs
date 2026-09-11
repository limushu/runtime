use super::model::MemberDisk;
use super::operations::{OperationKind, OperationTable};
use super::{
    DiskUuid, MemberDiskCommand, MemberDiskCommit, MemberDiskMetadata, MemberDiskMutation,
    MemberDiskQuery, MemberDiskQueryReply, MemberDiskReply, MemberDiskSeed, MemberDiskServiceError,
    MemberDiskState, PoolNodes, VirtualDisks,
};
use crate::service::{CommandActivity, CommandDecision, ExecutionContext, Service, TaskSnapshot};
use async_trait::async_trait;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

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
/// The live MemberDisk table is private. Queries return read-only views and
/// commands are the only way to start a state-changing business flow.
pub struct MemberDiskService {
    disks: RwLock<HashMap<DiskUuid, MemberDisk>>,
    pub(super) metadata: Arc<dyn MemberDiskMetadata>,
    pub(super) pool_nodes: Arc<dyn PoolNodes>,
    pub(super) virtual_disks: Arc<dyn VirtualDisks>,
    pub(super) config: MemberDiskConfig,
    pub(super) operations: OperationTable,
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
            metadata,
            pool_nodes,
            virtual_disks,
            config,
            operations: OperationTable::new(),
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

    /// The only mutation boundary: validate the complete batch, commit it to
    /// SDB, then publish the same mutations to the in-memory projection.
    pub(super) async fn commit(
        &self,
        mutations: Vec<(DiskUuid, MemberDiskMutation)>,
    ) -> Result<(), MemberDiskServiceError> {
        let commits = {
            let disks = self.disks.read().expect("MemberDisk table poisoned");
            let mut commits = Vec::new();
            for (disk, mutation) in mutations {
                let member = disks
                    .get(&disk)
                    .ok_or_else(|| MemberDiskServiceError::UnknownDisk(disk.clone()))?;
                if member.validate(&mutation)? {
                    commits.push(MemberDiskCommit { disk, mutation });
                }
            }
            commits
        };

        if commits.is_empty() {
            return Ok(());
        }

        self.metadata
            .commit(commits.clone())
            .await
            .map_err(MemberDiskServiceError::Metadata)?;

        let mut disks = self.disks.write().expect("MemberDisk table poisoned");
        for commit in commits {
            disks
                .get_mut(&commit.disk)
                .expect("committed MemberDisk disappeared")
                .apply_committed(&commit.mutation);
        }
        Ok(())
    }

    fn validate_command(&self, command: &MemberDiskCommand) -> Result<(), MemberDiskServiceError> {
        for disk in command.disks() {
            self.state(disk)?;
        }
        Ok(())
    }
}

#[async_trait]
impl Service for MemberDiskService {
    type Query = MemberDiskQuery;
    type QueryReply = MemberDiskQueryReply;
    type Command = MemberDiskCommand;
    type CommandReply = MemberDiskReply;
    // MemberDisk commands are batches. Per-disk conflicts are managed by the
    // domain's operation table, so the SDK has no single command key here.
    type Key = ();
    type Error = MemberDiskServiceError;

    fn name(&self) -> &'static str {
        "member-disk"
    }

    // A command can contain several disks. Their overlap is therefore handled
    // by MemberDisk's per-disk operation table, not by the SDK's single-key slot.
    fn command_key(&self, _command: &Self::Command) -> Option<Self::Key> {
        None
    }

    fn admit(
        &self,
        command: &Self::Command,
        _activity: CommandActivity<'_, Self::Command>,
    ) -> Result<CommandDecision<Self::CommandReply>, Self::Error> {
        self.validate_command(command)?;
        if command.disks().is_empty() {
            return Ok(CommandDecision::Complete(MemberDiskReply {
                outcomes: Vec::new(),
            }));
        }
        Ok(CommandDecision::Run)
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
        context: ExecutionContext<Self::Key>,
    ) -> Result<Self::CommandReply, Self::Error> {
        let reply = match command {
            MemberDiskCommand::DiskDown { disks, observed_at } => {
                self.run_disks(OperationKind::Offline, disks, Some(observed_at), &context)
                    .await
            }
            MemberDiskCommand::DiskUp { disks } => {
                self.run_disks(OperationKind::Online, disks, None, &context)
                    .await
            }
            MemberDiskCommand::Shrink { disks } => {
                self.run_disks(OperationKind::Shrink, disks, None, &context)
                    .await
            }
        };
        Ok(reply)
    }

    fn task_snapshots(&self) -> Vec<TaskSnapshot> {
        self.operations.snapshots()
    }
}
