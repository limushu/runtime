use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
    time::Duration,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::runtime::{
    IncomingCommand, NewRuns, Service, ServiceConfig, ServiceInstance, spawn_service,
};

use super::{
    BlockId, CommandKind, DiskEvent, DiskUuid, MemberDisk, MemberDiskChange, MemberDiskCommand,
    MemberDiskCommit, MemberDiskError, MemberDiskMetadata, MemberDiskQuery, MemberDiskQueryReply,
    MemberDiskReply, MemberDiskSnapshot, PhysicalState, PoolNodes, VirtualDisks,
    actions::{self, ActionPorts, AllocationPlan, DiskActionInput, ReleasePlan, WaitInput},
    commands::CommandTable,
    now_millis,
    requests::{
        CommittedChange, RequestCompletion, RequestId, RequestKind, RequestOutput, RequestPhase,
        RequestTable, StepResult,
    },
    state_machine::{self, Action, Decision, DiskIntent},
};

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

/// Owns MemberDisk metadata, command history in flight, and request policy.
///
/// No mutable domain object escapes this service. Async actions return
/// committed changes; the root runtime publishes those changes back into this
/// in-memory view after SDB has accepted them.
pub struct MemberDiskService {
    disks: HashMap<DiskUuid, MemberDisk>,
    commands: CommandTable,
    requests: RequestTable,
    cancel: CancellationToken,
    ports: ActionPorts,
    config: MemberDiskConfig,
}

impl MemberDiskService {
    pub fn new(
        disks: impl IntoIterator<Item = MemberDisk>,
        metadata: Arc<dyn MemberDiskMetadata>,
        pool_nodes: Arc<dyn PoolNodes>,
        virtual_disks: Arc<dyn VirtualDisks>,
        config: MemberDiskConfig,
    ) -> Result<Self, MemberDiskError> {
        let mut members = HashMap::new();
        for disk in disks {
            let id = disk.uuid().clone();
            if members.insert(id.clone(), disk).is_some() {
                return Err(MemberDiskError::InvalidState {
                    disk: id,
                    reason: "duplicate UUID",
                });
            }
        }

        let ports = ActionPorts::new(metadata, pool_nodes, virtual_disks, config.retry_delay);
        Ok(Self {
            disks: members,
            commands: CommandTable::default(),
            requests: RequestTable::default(),
            cancel: CancellationToken::new(),
            ports,
            config,
        })
    }

    pub fn start(self, config: ServiceConfig) -> ServiceInstance<Self> {
        spawn_service(self, config)
    }

    fn enqueue(&mut self, command_id: crate::runtime::CommandId, kind: RequestKind) {
        let disk = kind.disk().cloned();
        let cancel = self
            .commands
            .child_token(command_id)
            .expect("command is registered before its requests");
        let request = self.requests.push(command_id, kind, cancel);
        self.commands.add_request(command_id, request, disk);
    }

    fn cancel_shrink_for_down(&mut self, events: &[DiskEvent]) {
        let contains_known_down = events.iter().any(|event| {
            event.state == PhysicalState::Down && self.disks.contains_key(&event.disk)
        });
        if !contains_known_down {
            return;
        }

        for request in self.commands.cancel_kind(CommandKind::Shrink) {
            self.requests.cancel(request);
        }
    }

    fn admit_waiting(&mut self) {
        self.requests.activate_due_retries(Instant::now());
        let mut waiting = self.requests.waiting_round();

        while let Some(request_id) = waiting.pop_front() {
            let Some(request) = self.requests.get(request_id).cloned() else {
                continue;
            };

            if request.cancel.is_cancelled() {
                self.finish_request(request_id, Err(MemberDiskError::Cancelled));
                continue;
            }

            if let Some(disk) = request.kind.disk()
                && !self.disks.contains_key(disk)
            {
                self.finish_request(request_id, Err(MemberDiskError::UnknownDisk(disk.clone())));
                continue;
            }

            if let RequestKind::Allocate { requested, .. } = &request.kind
                && *requested == 0
            {
                self.finish_request(
                    request_id,
                    Err(MemberDiskError::InvalidRequest(
                        "allocation count must be greater than zero",
                    )),
                );
                continue;
            }

            if self.requests.claim(request_id) {
                self.commands.mark_running(request.command_id);
                continue;
            }

            self.preempt_conflict(&request);
            self.requests.wait(request_id);
        }
    }

    fn preempt_conflict(&mut self, incoming: &super::requests::RequestRecord) {
        match &incoming.kind {
            RequestKind::Disk { disk, intent } => {
                if let Some(allocation) = self.requests.allocation_current() {
                    self.requests.cancel(allocation);
                }

                let Some(current_id) = self.requests.current_for_disk(disk) else {
                    return;
                };
                let Some(current) = self.requests.get(current_id) else {
                    return;
                };
                if current.kind.intent() != Some(intent.kind()) {
                    self.requests.cancel(current_id);
                }
            }
            RequestKind::Allocate { .. } | RequestKind::Release { .. } => {}
        }
    }

    fn collect_ready_actions(&mut self) -> (BTreeMap<Action, Vec<RequestId>>, bool) {
        let mut groups = BTreeMap::new();
        let mut released_object = false;

        for request_id in self.requests.take_ready() {
            if self.requests.is_cancelled(request_id) {
                self.finish_request(request_id, Err(MemberDiskError::Cancelled));
                released_object = true;
                continue;
            }

            match self.next_action(request_id) {
                Ok(Next::Run(action)) => {
                    groups
                        .entry(action)
                        .or_insert_with(Vec::new)
                        .push(request_id);
                }
                Ok(Next::Complete(output)) => {
                    self.finish_request(request_id, Ok(output));
                    released_object = true;
                }
                Err(error) => {
                    self.finish_request(request_id, Err(error));
                    released_object = true;
                }
            }
        }

        (groups, released_object)
    }

    fn next_action(&self, request_id: RequestId) -> Result<Next, MemberDiskError> {
        let request = self
            .requests
            .get(request_id)
            .ok_or_else(|| MemberDiskError::Internal("ready request disappeared".into()))?;

        match &request.kind {
            RequestKind::Allocate {
                requested,
                allocated,
            } if allocated.len() == *requested => {
                Ok(Next::Complete(RequestOutput::Allocated(allocated.clone())))
            }
            RequestKind::Allocate { .. } => Ok(Next::Run(Action::Allocate)),
            RequestKind::Release { remaining } if remaining.is_empty() => {
                Ok(Next::Complete(RequestOutput::Released))
            }
            RequestKind::Release { .. } => Ok(Next::Run(Action::Release)),
            RequestKind::Disk { disk, intent } => {
                let member = self
                    .disks
                    .get(disk)
                    .ok_or_else(|| MemberDiskError::UnknownDisk(disk.clone()))?;

                match state_machine::decide(
                    member,
                    intent,
                    now_millis(),
                    self.config.recovery_window,
                ) {
                    Decision::Run(action) => Ok(Next::Run(action)),
                    Decision::Done => Ok(Next::Complete(RequestOutput::Disk(member.clone()))),
                }
            }
        }
    }

    fn start_action(
        &mut self,
        action: Action,
        requests: Vec<RequestId>,
        runs: &mut NewRuns<Vec<RequestCompletion>>,
    ) {
        self.requests.mark_running(&requests, action);

        match action {
            Action::CloseIo => match self.disk_inputs(&requests) {
                Ok(inputs) => runs.push(actions::close_io(self.ports.clone(), inputs)),
                Err(error) => self.fail_requests(requests, error),
            },
            Action::BringOnline => match self.disk_inputs(&requests) {
                Ok(inputs) => runs.push(actions::bring_online(self.ports.clone(), inputs)),
                Err(error) => self.fail_requests(requests, error),
            },
            Action::DisableAllocation => match self.disk_inputs(&requests) {
                Ok(inputs) => runs.push(actions::disable_allocation(self.ports.clone(), inputs)),
                Err(error) => self.fail_requests(requests, error),
            },
            Action::WaitRecovery => {
                for request in requests {
                    match self.wait_input(request) {
                        Ok(input) => runs.push(actions::wait_recovery(input)),
                        Err(error) => self.finish_request(request, Err(error)),
                    }
                }
            }
            Action::Evacuate => match self.disk_inputs(&requests) {
                Ok(inputs) => {
                    for input in inputs {
                        runs.push(actions::evacuate(self.ports.clone(), input));
                    }
                }
                Err(error) => self.fail_requests(requests, error),
            },
            Action::Allocate => {
                for request in requests {
                    match self.allocation_plan(request) {
                        Ok(plan) => runs.push(actions::allocate(self.ports.clone(), plan)),
                        Err(error) => self.finish_request(request, Err(error)),
                    }
                }
            }
            Action::Release => {
                for request in requests {
                    match self.release_plan(request) {
                        Ok(plan) => runs.push(actions::release(self.ports.clone(), plan)),
                        Err(error) => self.finish_request(request, Err(error)),
                    }
                }
            }
        }
    }

    fn disk_inputs(&self, requests: &[RequestId]) -> Result<Vec<DiskActionInput>, MemberDiskError> {
        requests
            .iter()
            .map(|id| {
                let request = self
                    .requests
                    .get(*id)
                    .ok_or_else(|| MemberDiskError::Internal("ready request disappeared".into()))?;
                let disk = request.kind.disk().ok_or_else(|| {
                    MemberDiskError::Internal("disk action received a non-disk request".into())
                })?;
                Ok(DiskActionInput {
                    request: request.id,
                    disk: disk.clone(),
                    cancel: request.cancel.clone(),
                })
            })
            .collect()
    }

    fn fail_requests(&mut self, requests: Vec<RequestId>, error: MemberDiskError) {
        for request in requests {
            self.finish_request(request, Err(error.clone()));
        }
    }

    fn wait_input(&self, id: RequestId) -> Result<WaitInput, MemberDiskError> {
        let request = self
            .requests
            .get(id)
            .ok_or_else(|| MemberDiskError::Internal("wait request disappeared".into()))?;
        let RequestKind::Disk {
            intent: DiskIntent::Offline { observed_at },
            ..
        } = request.kind
        else {
            return Err(MemberDiskError::Internal(
                "WaitRecovery received a non-offline request".into(),
            ));
        };
        Ok(WaitInput {
            request: id,
            observed_at,
            recovery_window: self.config.recovery_window,
            cancel: request.cancel.clone(),
        })
    }

    fn allocation_plan(&self, id: RequestId) -> Result<AllocationPlan, MemberDiskError> {
        let request = self
            .requests
            .get(id)
            .ok_or_else(|| MemberDiskError::Internal("allocation request disappeared".into()))?;
        let RequestKind::Allocate {
            requested,
            allocated,
        } = &request.kind
        else {
            return Err(MemberDiskError::Internal(
                "Allocate action received another request kind".into(),
            ));
        };

        if allocated.is_empty() {
            let available: usize = self.disks.values().map(MemberDisk::free_blocks).sum();
            if available < *requested {
                return Err(MemberDiskError::NoSpace);
            }
        }

        let remaining = requested.saturating_sub(allocated.len());
        let (disk, block_ids) = self.select_blocks(remaining)?;
        let commit = MemberDiskCommit {
            disk,
            change: MemberDiskChange::Allocate(block_ids),
        };
        self.validate_changes(std::slice::from_ref(&commit))?;
        Ok(AllocationPlan {
            request: id,
            commit,
        })
    }

    fn release_plan(&self, id: RequestId) -> Result<ReleasePlan, MemberDiskError> {
        let request = self
            .requests
            .get(id)
            .ok_or_else(|| MemberDiskError::Internal("release request disappeared".into()))?;
        let RequestKind::Release { remaining } = &request.kind else {
            return Err(MemberDiskError::Internal(
                "Release action received another request kind".into(),
            ));
        };

        let first = remaining.first().ok_or_else(|| {
            MemberDiskError::Internal("completed Release was scheduled again".into())
        })?;
        let disk = first.disk.clone();
        let blocks = remaining
            .iter()
            .filter(|block| block.disk == disk)
            .map(|block| block.block)
            .collect();
        let commit = MemberDiskCommit {
            disk,
            change: MemberDiskChange::Release(blocks),
        };
        self.validate_changes(std::slice::from_ref(&commit))?;
        Ok(ReleasePlan {
            request: id,
            commit,
        })
    }

    fn select_blocks(&self, count: usize) -> Result<(DiskUuid, Vec<BlockId>), MemberDiskError> {
        let mut disks: Vec<_> = self.disks.values().collect();
        disks.sort_by_key(|disk| disk.uuid());

        for disk in disks {
            let blocks = disk.first_free_blocks(count);
            if !blocks.is_empty() {
                return Ok((disk.uuid().clone(), blocks));
            }
        }

        Err(MemberDiskError::NoSpace)
    }

    fn validate_changes(&self, changes: &[MemberDiskCommit]) -> Result<(), MemberDiskError> {
        for commit in changes {
            let disk = self
                .disks
                .get(&commit.disk)
                .ok_or_else(|| MemberDiskError::UnknownDisk(commit.disk.clone()))?;
            disk.validate(&commit.change)?;
        }
        Ok(())
    }

    fn settle(&mut self, completion: RequestCompletion) {
        let Some(request) = self.requests.get(completion.request) else {
            return;
        };
        let action = match request.phase {
            RequestPhase::Running(action) | RequestPhase::Cancelling(Some(action)) => action,
            _ => {
                self.finish_request(
                    completion.request,
                    Err(MemberDiskError::Internal(
                        "completion did not belong to a running request".into(),
                    )),
                );
                return;
            }
        };

        match completion.result {
            StepResult::Progress(changes) => {
                if let Err(error) = self.apply_committed(completion.request, changes) {
                    self.finish_request(completion.request, Err(error));
                } else {
                    match self.next_action(completion.request) {
                        Ok(Next::Complete(output)) => {
                            self.finish_request(completion.request, Ok(output));
                        }
                        Ok(Next::Run(_)) if self.requests.is_cancelled(completion.request) => {
                            self.finish_request(
                                completion.request,
                                Err(MemberDiskError::Cancelled),
                            );
                        }
                        Ok(Next::Run(_)) => self.requests.make_ready(completion.request),
                        Err(error) => self.finish_request(completion.request, Err(error)),
                    }
                }
            }
            StepResult::Retry(error) => {
                if self.requests.is_cancelled(completion.request) {
                    self.finish_request(completion.request, Err(MemberDiskError::Cancelled));
                } else if request.attempts >= self.config.max_retries {
                    self.finish_request(completion.request, Err(error));
                } else {
                    self.requests
                        .retry(completion.request, action, self.config.retry_delay);
                }
            }
            StepResult::Cancelled => {
                self.finish_request(completion.request, Err(MemberDiskError::Cancelled));
            }
            StepResult::Failed(error) => {
                self.finish_request(completion.request, Err(error));
            }
        }
    }

    fn apply_committed(
        &mut self,
        request: RequestId,
        changes: Vec<CommittedChange>,
    ) -> Result<(), MemberDiskError> {
        for committed in &changes {
            self.disks
                .get(&committed.disk)
                .ok_or_else(|| MemberDiskError::UnknownDisk(committed.disk.clone()))?
                .validate(&committed.change)?;
        }

        for committed in &changes {
            self.disks
                .get_mut(&committed.disk)
                .expect("all committed disks were validated")
                .apply_committed(committed.change.clone());
        }
        self.requests.record_committed(request, &changes);
        Ok(())
    }

    fn finish_request(&mut self, id: RequestId, result: Result<RequestOutput, MemberDiskError>) {
        let Some(request) = self.requests.remove(id) else {
            return;
        };
        self.commands.record(request.command_id, id, result);
        self.finish_commands();
    }

    fn finish_commands(&mut self) {
        for command in self.commands.complete_ready() {
            self.commands.finish(command);
        }
    }
}

impl Service for MemberDiskService {
    type Command = MemberDiskCommand;
    type CommandReply = MemberDiskReply;
    type Query = MemberDiskQuery;
    type QueryReply = MemberDiskQueryReply;
    type Completion = Vec<RequestCompletion>;
    type Error = MemberDiskError;
    type Snapshot = MemberDiskSnapshot;

    fn query(&self, query: Self::Query) -> Result<Self::QueryReply, Self::Error> {
        Ok(match query {
            MemberDiskQuery::Get(disk) => {
                MemberDiskQueryReply::Disk(self.disks.get(&disk).cloned())
            }
            MemberDiskQuery::List => {
                let mut disks: Vec<_> = self.disks.values().cloned().collect();
                disks.sort_by(|left, right| left.uuid().cmp(right.uuid()));
                MemberDiskQueryReply::Disks(disks)
            }
            MemberDiskQuery::InFlightCommands => {
                MemberDiskQueryReply::Commands(self.commands.snapshots())
            }
            MemberDiskQuery::Work => MemberDiskQueryReply::Work(self.requests.snapshots()),
        })
    }

    fn receive_command(
        &mut self,
        command: IncomingCommand<Self::Command, Self::CommandReply, Self::Error>,
    ) {
        let IncomingCommand { id, payload, reply } = command;
        let kind = payload.kind();
        let command_cancel = self.cancel.child_token();

        if let MemberDiskCommand::ApplyEvents(events) = &payload {
            self.cancel_shrink_for_down(events);
        }

        self.commands.insert(id, kind, command_cancel, reply);
        match payload {
            MemberDiskCommand::ApplyEvents(events) => {
                for event in events {
                    let intent = match event.state {
                        PhysicalState::Down => DiskIntent::Offline {
                            observed_at: event.observed_at,
                        },
                        PhysicalState::Up => DiskIntent::Online,
                    };
                    self.enqueue(
                        id,
                        RequestKind::Disk {
                            disk: event.disk,
                            intent,
                        },
                    );
                }
            }
            MemberDiskCommand::Shrink(disks) => {
                for disk in disks {
                    self.enqueue(
                        id,
                        RequestKind::Disk {
                            disk,
                            intent: DiskIntent::Shrink,
                        },
                    );
                }
            }
            MemberDiskCommand::Allocate(request) => {
                self.enqueue(
                    id,
                    RequestKind::Allocate {
                        requested: request.count,
                        allocated: Vec::new(),
                    },
                );
            }
            MemberDiskCommand::Release(request) => {
                self.enqueue(
                    id,
                    RequestKind::Release {
                        remaining: request.blocks,
                    },
                );
            }
        }
        self.commands.acknowledge(id);
        self.finish_commands();
    }

    fn admit(&mut self) {
        self.admit_waiting();
    }

    fn start_ready(&mut self, runs: &mut NewRuns<Self::Completion>) {
        loop {
            let (groups, released_object) = self.collect_ready_actions();
            for (action, requests) in groups {
                self.start_action(action, requests, runs);
            }

            if !released_object {
                break;
            }
            self.admit_waiting();
        }
    }

    fn finish_running(&mut self, completions: Self::Completion) {
        for completion in completions {
            self.settle(completion);
        }
    }

    fn cancel_all(&mut self) {
        self.cancel.cancel();
        for request in self.commands.cancel_all() {
            let has_future = matches!(
                self.requests.get(request).map(|request| &request.phase),
                Some(RequestPhase::Running(_) | RequestPhase::Cancelling(Some(_)))
            );
            self.requests.cancel(request);
            if !has_future {
                self.finish_request(request, Err(MemberDiskError::Cancelled));
            }
        }
    }

    fn is_idle(&self) -> bool {
        self.requests.is_empty() && self.commands.is_empty()
    }

    fn next_wakeup(&self) -> Option<Instant> {
        self.requests.next_retry()
    }

    fn snapshot(&self) -> Self::Snapshot {
        MemberDiskSnapshot {
            commands: self.commands.snapshots(),
            requests: self.requests.snapshots(),
        }
    }
}

enum Next {
    Run(Action),
    Complete(RequestOutput),
}
