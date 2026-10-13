use super::{
    activities::Activities,
    workflows::{self, DiskWorkflow},
    *,
};
use crate::runtime::{
    IncomingCommand, Lifecycle, NewRuns, Service, ServiceConfig, ServiceInstance, spawn_service,
    workflow::{WorkflowCompletion, WorkflowStatus, Workflows},
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};

#[derive(Debug, Clone)]
pub struct MemberDiskConfig {
    pub recovery_window: Duration,
    pub retry_delay: Duration,
    pub max_retries: u32,
}

impl Default for MemberDiskConfig {
    fn default() -> Self {
        Self {
            recovery_window: Duration::from_secs(30),
            retry_delay: Duration::from_millis(100),
            max_retries: 3,
        }
    }
}

pub struct MemberDiskService {
    activities: Activities,
    workflows: Workflows<DiskWorkflow, MemberDiskReply, MemberDiskError, MemberDiskReply>,
}

impl MemberDiskService {
    pub fn new(
        disks: impl IntoIterator<Item = MemberDisk>,
        metadata: Arc<dyn MemberDiskMetadata>,
        pool_nodes: Arc<dyn PoolNodes>,
        virtual_disks: Arc<dyn VirtualDisks>,
        config: MemberDiskConfig,
    ) -> Result<Self, MemberDiskError> {
        let mut members = BTreeMap::new();
        for disk in disks {
            let id = disk.uuid().clone();
            if members.insert(id.clone(), disk).is_some() {
                return Err(MemberDiskError::InvalidState {
                    disk: id,
                    reason: "duplicate UUID",
                });
            }
        }
        Ok(Self {
            activities: Activities::new(members, metadata, pool_nodes, virtual_disks, config),
            workflows: Workflows::default(),
        })
    }

    pub fn start(self, config: ServiceConfig) -> ServiceInstance<Self> {
        spawn_service(self, config)
    }

    fn command_snapshots(&self) -> Vec<CommandSnapshot> {
        let mut commands = BTreeMap::new();
        for execution in self.workflows.executions() {
            let command = commands
                .entry(execution.command)
                .or_insert(CommandSnapshot {
                    id: execution.command,
                    kind: execution.input.kind(),
                    status: CommandStatus::Accepted,
                    workflows: 0,
                    finished: 0,
                });
            command.workflows += 1;
            if execution.result.is_some() {
                command.finished += 1;
            }
            match execution.status() {
                WorkflowStatus::Cancelling => command.status = CommandStatus::Cancelling,
                WorkflowStatus::Running { .. } if command.status != CommandStatus::Cancelling => {
                    command.status = CommandStatus::Running
                }
                _ => {}
            }
        }
        commands
            .into_values()
            .filter(|c| c.finished < c.workflows)
            .collect()
    }

    fn workflow_snapshots(&self) -> Vec<WorkflowSnapshot> {
        self.workflows
            .executions()
            .map(|execution| WorkflowSnapshot {
                id: execution.id,
                command_id: execution.command,
                disk: execution.input.disk().cloned(),
                status: execution.status(),
                result: execution.result.clone(),
            })
            .collect()
    }
}

impl Service for MemberDiskService {
    type Command = MemberDiskCommand;
    type CommandReply = MemberDiskReply;
    type Query = MemberDiskQuery;
    type QueryReply = MemberDiskQueryReply;
    type Completion = WorkflowCompletion<MemberDiskReply, MemberDiskError>;
    type Error = MemberDiskError;
    type Snapshot = MemberDiskSnapshot;

    fn query(&self, query: Self::Query) -> Result<Self::QueryReply, Self::Error> {
        Ok(match query {
            MemberDiskQuery::Get(disk) => {
                MemberDiskQueryReply::Disk(self.activities.get(&disk).ok())
            }
            MemberDiskQuery::List => MemberDiskQueryReply::Disks(self.activities.list()),
            MemberDiskQuery::InFlightCommands => {
                MemberDiskQueryReply::Commands(self.command_snapshots())
            }
            MemberDiskQuery::Work => MemberDiskQueryReply::Work(self.workflow_snapshots()),
        })
    }

    fn receive_command(
        &mut self,
        command: IncomingCommand<Self::Command, Self::CommandReply, Self::Error>,
    ) {
        let IncomingCommand { id, payload, reply } = command;
        let kind = payload.kind();
        let inputs: Vec<_> = match payload {
            MemberDiskCommand::ApplyEvents(events) => {
                // Global DOWN/Shrink precedence is business policy, not Runtime policy.
                if events
                    .iter()
                    .any(|e| e.state == PhysicalState::Down && self.activities.get(&e.disk).is_ok())
                {
                    self.workflows
                        .cancel_where(|w| matches!(w, DiskWorkflow::Shrink(_)));
                }
                events
                    .into_iter()
                    .map(|e| match e.state {
                        PhysicalState::Down => DiskWorkflow::Offline {
                            disk: e.disk,
                            observed_at: e.observed_at,
                        },
                        PhysicalState::Up => DiskWorkflow::Online(e.disk),
                    })
                    .collect()
            }
            MemberDiskCommand::Shrink(disks) => {
                disks.into_iter().map(DiskWorkflow::Shrink).collect()
            }
            MemberDiskCommand::Allocate(request) => vec![DiskWorkflow::Allocate(request.count)],
            MemberDiskCommand::Release(request) => vec![DiskWorkflow::Release(request.blocks)],
        };
        let inputs: Vec<_> = inputs
            .into_iter()
            .map(|input| match input.disk() {
                Some(disk) if self.activities.get(disk).is_err() => DiskWorkflow::Unknown {
                    disk: disk.clone(),
                    kind,
                },
                _ => input,
            })
            .collect();
        let disks: Vec<_> = inputs.iter().filter_map(|w| w.disk().cloned()).collect();
        self.workflows.submit(id, inputs, reply, move |results| {
            if matches!(kind, CommandKind::DiskEvents | CommandKind::Shrink) {
                let outcomes = disks
                    .into_iter()
                    .zip(results)
                    .map(|(disk, result)| {
                        let result = result.and_then(|reply| match reply {
                            MemberDiskReply::Disks(mut disks) => disks.remove(0).result,
                            _ => Err(MemberDiskError::Internal("wrong workflow output".into())),
                        });
                        DiskOutcome { disk, result }
                    })
                    .collect();
                Ok(MemberDiskReply::Disks(outcomes))
            } else {
                results.into_iter().next().expect("space workflow")
            }
        });
    }

    fn admit(&mut self) {}

    fn start_ready(&mut self, runs: &mut NewRuns<Self::Completion>) {
        let activities = self.activities.clone();
        self.workflows.start_ready(
            runs,
            DiskWorkflow::conflicts,
            DiskWorkflow::preempts,
            move |input, context| workflows::run(input, activities.clone(), context),
        );
    }

    fn finish_running(&mut self, completion: Self::Completion) {
        self.workflows.finish(completion);
    }
    fn cancel_all(&mut self) {
        self.workflows.cancel_where(|_| true);
    }
    fn is_idle(&self) -> bool {
        self.workflows.is_idle()
    }
    fn lifecycle_changed(&mut self, lifecycle: Lifecycle) {
        self.workflows.lifecycle(lifecycle);
    }
    fn progress_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        Some(self.workflows.changed())
    }
    fn snapshot(&self) -> Self::Snapshot {
        MemberDiskSnapshot {
            commands: self.command_snapshots(),
            workflows: self.workflow_snapshots(),
        }
    }
}
