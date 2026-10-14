mod protocol;
#[cfg(test)]
mod tests;
mod workflows;

use crate::member_disk::*;
pub use crate::runtime::workflow::WorkflowStatus;
use crate::runtime::{
    IncomingCommand, Lifecycle, NewRuns, Service,
    workflow::{WorkflowCompletion, Workflows},
};
use crate::vd::VdService;
pub use protocol::*;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use workflows::{Activities, DiskWorkflow};

/// Owns application operations for exactly one Pool. No mutable domain data.
pub struct ProcessManager {
    pub(crate) activities: Activities,
    workflows: Workflows<DiskWorkflow, ProcessReply, MemberDiskError, ProcessReply>,
}

impl ProcessManager {
    pub(crate) fn new(
        disk: DiskService,
        vd: VdService,
        recovery_window: Duration,
        changed: Arc<tokio::sync::Notify>,
    ) -> Self {
        Self {
            activities: Activities {
                disk,
                vd,
                recovery_window,
            },
            workflows: Workflows::with_notify(changed),
        }
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

impl Service for ProcessManager {
    type Command = ProcessCommand;
    type CommandReply = ProcessReply;
    type Query = ProcessQuery;
    type QueryReply = ProcessQueryReply;
    type Completion = WorkflowCompletion<ProcessReply, MemberDiskError>;
    type Error = MemberDiskError;
    type Snapshot = ProcessSnapshot;

    fn query(&self, query: Self::Query) -> Result<Self::QueryReply, Self::Error> {
        Ok(match query {
            ProcessQuery::Get(disk) => {
                ProcessQueryReply::Disk(self.activities.disk.get(&disk).ok())
            }
            ProcessQuery::List => ProcessQueryReply::Disks(self.activities.disk.list()),
            ProcessQuery::InFlightCommands => ProcessQueryReply::Commands(self.command_snapshots()),
            ProcessQuery::Work => ProcessQueryReply::Work(self.workflow_snapshots()),
        })
    }

    fn receive_command(
        &mut self,
        command: IncomingCommand<Self::Command, Self::CommandReply, Self::Error>,
    ) {
        let IncomingCommand { id, payload, reply } = command;
        let kind = payload.kind();
        let inputs: Vec<_> = match payload {
            ProcessCommand::ApplyEvents(events) => {
                // Global DOWN/Shrink precedence is business policy, not Runtime policy.
                if events.iter().any(|e| {
                    e.state == PhysicalState::Down && self.activities.disk.get(&e.disk).is_ok()
                }) {
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
            ProcessCommand::Shrink(disks) => disks.into_iter().map(DiskWorkflow::Shrink).collect(),
            ProcessCommand::Allocate(request) => vec![DiskWorkflow::Allocate(request.count)],
            ProcessCommand::Release(request) => vec![DiskWorkflow::Release(request.blocks)],
        };
        let inputs: Vec<_> = inputs
            .into_iter()
            .map(|input| match input.disk() {
                Some(disk) if self.activities.disk.get(disk).is_err() => DiskWorkflow::Unknown {
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
                            ProcessReply::Disks(mut disks) => disks.remove(0).result,
                            _ => Err(MemberDiskError::Internal("wrong workflow output".into())),
                        });
                        DiskOutcome { disk, result }
                    })
                    .collect();
                Ok(ProcessReply::Disks(outcomes))
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
        self.activities.disk.stop();
        self.activities.vd.stop();
    }
    fn is_idle(&self) -> bool {
        self.workflows.is_idle() && self.activities.disk.is_idle() && self.activities.vd.is_idle()
    }
    fn lifecycle_changed(&mut self, lifecycle: Lifecycle) {
        self.workflows.lifecycle(lifecycle);
        self.activities.disk.lifecycle(lifecycle);
        self.activities.vd.lifecycle(lifecycle);
    }
    fn progress_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        Some(self.workflows.changed())
    }
    fn snapshot(&self) -> Self::Snapshot {
        ProcessSnapshot {
            pool: self.activities.disk.pool_id().to_owned(),
            disk_blocked: self.activities.disk.blocked_resources(),
            vd_blocked: self.activities.vd.blocked_resources(),
            commands: self.command_snapshots(),
            workflows: self.workflow_snapshots(),
        }
    }
}

impl Drop for ProcessManager {
    fn drop(&mut self) {
        self.activities.disk.stop();
        self.activities.vd.stop();
    }
}
