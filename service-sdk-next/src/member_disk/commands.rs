use std::collections::HashMap;

use tokio_util::sync::CancellationToken;

use crate::runtime::{CommandId, CommandReply};

use super::{
    CommandKind, CommandSnapshot, CommandStatus, DiskOutcome, DiskUuid, MemberDiskError,
    MemberDiskReply,
    requests::{RequestId, RequestOutput},
};

/// Keeps the identity, cancellation root, progress and optional caller reply
/// for every accepted command until all of its child requests have settled.
#[derive(Default)]
pub(crate) struct CommandTable {
    entries: HashMap<CommandId, CommandRecord>,
    order: Vec<CommandId>,
}

struct CommandRecord {
    id: CommandId,
    kind: CommandKind,
    state: CommandStatus,
    requests: Vec<RequestId>,
    request_disks: HashMap<RequestId, DiskUuid>,
    results: HashMap<RequestId, Result<RequestOutput, MemberDiskError>>,
    cancel: CancellationToken,
    reply: CommandReply<MemberDiskReply, MemberDiskError>,
}

impl CommandTable {
    pub fn insert(
        &mut self,
        id: CommandId,
        kind: CommandKind,
        cancel: CancellationToken,
        reply: CommandReply<MemberDiskReply, MemberDiskError>,
    ) {
        self.order.push(id);
        self.entries.insert(
            id,
            CommandRecord {
                id,
                kind,
                state: CommandStatus::Accepted,
                requests: Vec::new(),
                request_disks: HashMap::new(),
                results: HashMap::new(),
                cancel,
                reply,
            },
        );
    }

    pub fn acknowledge(&mut self, id: CommandId) {
        if let Some(command) = self.entries.get_mut(&id) {
            command.reply.accepted(id);
        }
    }

    pub fn add_request(&mut self, command: CommandId, request: RequestId, disk: Option<DiskUuid>) {
        if let Some(command) = self.entries.get_mut(&command) {
            command.requests.push(request);
            if let Some(disk) = disk {
                command.request_disks.insert(request, disk);
            }
        }
    }

    pub fn child_token(&self, command: CommandId) -> Option<CancellationToken> {
        self.entries
            .get(&command)
            .map(|command| command.cancel.child_token())
    }

    pub fn mark_running(&mut self, command: CommandId) {
        if let Some(command) = self.entries.get_mut(&command)
            && command.state != CommandStatus::Cancelling
        {
            command.state = CommandStatus::Running;
        }
    }

    pub fn cancel_kind(&mut self, kind: CommandKind) -> Vec<RequestId> {
        let mut requests = Vec::new();
        for command in self.entries.values_mut() {
            if command.kind == kind && command.state != CommandStatus::Cancelling {
                command.state = CommandStatus::Cancelling;
                command.cancel.cancel();
                requests.extend(command.requests.iter().copied());
            }
        }
        requests
    }

    pub fn cancel_all(&mut self) -> Vec<RequestId> {
        let mut requests = Vec::new();
        for command in self.entries.values_mut() {
            command.state = CommandStatus::Cancelling;
            command.cancel.cancel();
            requests.extend(command.requests.iter().copied());
        }
        requests
    }

    pub fn record(
        &mut self,
        command_id: CommandId,
        request_id: RequestId,
        result: Result<RequestOutput, MemberDiskError>,
    ) {
        if let Some(command) = self.entries.get_mut(&command_id) {
            command.results.insert(request_id, result);
        }
    }

    pub fn complete_ready(&self) -> Vec<CommandId> {
        self.entries
            .values()
            .filter(|command| command.results.len() == command.requests.len())
            .map(|command| command.id)
            .collect()
    }

    pub fn finish(&mut self, id: CommandId) {
        let Some(mut command) = self.entries.remove(&id) else {
            return;
        };
        self.order.retain(|command_id| *command_id != id);
        command.reply.complete(build_reply(&command));
    }

    pub fn snapshots(&self) -> Vec<CommandSnapshot> {
        self.order
            .iter()
            .filter_map(|id| self.entries.get(id))
            .map(|command| CommandSnapshot {
                id: command.id,
                kind: command.kind,
                status: command.state,
                requests: command.requests.len(),
                finished: command.results.len(),
            })
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn build_reply(command: &CommandRecord) -> Result<MemberDiskReply, MemberDiskError> {
    match command.kind {
        CommandKind::DiskEvents | CommandKind::Shrink => disk_reply(command),
        CommandKind::Allocate => allocation_reply(command),
        CommandKind::Release => release_reply(command),
    }
}

fn disk_reply(command: &CommandRecord) -> Result<MemberDiskReply, MemberDiskError> {
    let mut outcomes = Vec::with_capacity(command.requests.len());
    for request in &command.requests {
        let disk = command.request_disks.get(request).cloned().ok_or_else(|| {
            MemberDiskError::Internal("disk request lost its object identity".into())
        })?;
        let result = match command.results.get(request) {
            Some(Ok(RequestOutput::Disk(member))) => Ok(member.clone()),
            Some(Err(error)) => Err(error.clone()),
            Some(Ok(_)) => Err(MemberDiskError::Internal(
                "disk command returned a non-disk result".into(),
            )),
            None => {
                return Err(MemberDiskError::Internal(
                    "command completed before every request had a result".into(),
                ));
            }
        };
        outcomes.push(DiskOutcome { disk, result });
    }
    Ok(MemberDiskReply::Disks(outcomes))
}

fn allocation_reply(command: &CommandRecord) -> Result<MemberDiskReply, MemberDiskError> {
    match command.results.values().next() {
        Some(Ok(RequestOutput::Allocated(blocks))) => {
            Ok(MemberDiskReply::Allocated(blocks.clone()))
        }
        Some(Err(error)) => Err(error.clone()),
        _ => Err(MemberDiskError::Internal(
            "allocation completed without an allocation result".into(),
        )),
    }
}

fn release_reply(command: &CommandRecord) -> Result<MemberDiskReply, MemberDiskError> {
    match command.results.values().next() {
        Some(Ok(RequestOutput::Released)) => Ok(MemberDiskReply::Released),
        Some(Err(error)) => Err(error.clone()),
        _ => Err(MemberDiskError::Internal(
            "release completed without a release result".into(),
        )),
    }
}
