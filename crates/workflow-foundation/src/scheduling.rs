//! Apply one directional business rule: existing task A, incoming task B.
//! No inferred reverse relation, numeric priority or implicit object conflicts.
use crate::TaskId;
use std::collections::{BTreeMap, BTreeSet};

/// Business-neutral scheduling explanation, shared by every observation entry point.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConflictDecision {
    Independent,
    Wait,
    CancelThenWait,
    /// Request a safe stop of the existing task; admit the incoming task after ACK.
    SuspendThenWait,
    Reject(&'static str),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    pub decision: ConflictDecision,
    pub reason: &'static str,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConflictMatch {
    pub existing: TaskId,
    pub conflict: Conflict,
}
pub trait SchedulingPolicy<D>: Send + Sync + 'static {
    /// Pure decision, used both before acceptance and while waiting.
    /// The owner checks ALL existing tasks before applying any side effects.
    fn on_task(&self, existing: &D, incoming: &D) -> Conflict;
}
pub struct Candidate<'a, D> {
    pub id: TaskId,
    pub descriptor: &'a D,
    pub started: bool,
    /// Durable registration has succeeded; an unaccepted queue entry cannot act.
    pub accepted: bool,
    pub paused: bool,
}
#[derive(Default)]
pub struct Plan {
    /// (queue head, current task). Applied only after current enters its body.
    pub cancellations: Vec<(TaskId, TaskId)>,
    pub pauses: BTreeMap<TaskId, BTreeSet<String>>,
    pub blockers: BTreeMap<TaskId, Vec<TaskId>>,
    pub ready: Vec<TaskId>,
}
/// Derive current + queue from explicit conflict rules and creation order.
/// Only the first conflicting successor may influence a current task. Later
/// tasks wait behind that head; independent work remains independently runnable.
/// Requests affecting multiple objects must reach the head on all of them first.
pub fn plan<D: 'static>(policy: &dyn SchedulingPolicy<D>, tasks: &[Candidate<'_, D>]) -> Plan {
    let mut tasks: Vec<_> = tasks.iter().collect();
    tasks.sort_by_key(|task| task.id);
    let mut result = Plan::default();
    let mut heads = vec![None; tasks.len()];
    let mut rules = BTreeMap::new();
    for (i, existing) in tasks.iter().enumerate() {
        result.pauses.insert(existing.id, BTreeSet::new());
        result.blockers.insert(existing.id, vec![]);
        for (j, incoming) in tasks.iter().enumerate().skip(i + 1) {
            let rule = policy.on_task(existing.descriptor, incoming.descriptor);
            if rule.decision != ConflictDecision::Independent {
                heads[i].get_or_insert(j);
                rules.insert((i, j), rule);
            }
        }
    }
    // A task behind another queued task cannot send control to any current.
    let mut at_head: Vec<_> = tasks.iter().map(|t| t.accepted).collect();
    for &(i, j) in rules.keys() {
        let head = heads[i].unwrap();
        if !tasks[i].started
            || (head != j && (!tasks[head].started || !rules.contains_key(&(head, j))))
        {
            at_head[j] = false;
        }
    }
    for (&(i, j), rule) in &rules {
        let existing = tasks[i];
        let incoming = tasks[j];
        let head = heads[i].unwrap();
        if head != j {
            // If the head also conflicts directly, that rule already determines
            // whether to wait for its end or its pause ACK.
            if !rules.contains_key(&(head, j)) {
                result
                    .blockers
                    .get_mut(&incoming.id)
                    .unwrap()
                    .push(tasks[head].id);
            }
            continue;
        }
        match rule.decision {
            ConflictDecision::SuspendThenWait if existing.started && at_head[j] => {
                result
                    .pauses
                    .get_mut(&existing.id)
                    .unwrap()
                    .insert(format!("{}:{}", incoming.id, rule.reason));
                result
                    .blockers
                    .get_mut(&existing.id)
                    .unwrap()
                    .push(incoming.id);
                if !existing.paused {
                    result
                        .blockers
                        .get_mut(&incoming.id)
                        .unwrap()
                        .push(existing.id);
                }
            }
            _ => {
                result
                    .blockers
                    .get_mut(&incoming.id)
                    .unwrap()
                    .push(existing.id);
                if rule.decision == ConflictDecision::CancelThenWait
                    && existing.started
                    && at_head[j]
                {
                    result.cancellations.push((incoming.id, existing.id));
                }
            }
        }
    }
    for blockers in result.blockers.values_mut() {
        blockers.sort();
        blockers.dedup();
    }
    for task in tasks {
        if task.accepted && !task.started && result.blockers[&task.id].is_empty() {
            result.ready.push(task.id);
        }
    }
    result
}
