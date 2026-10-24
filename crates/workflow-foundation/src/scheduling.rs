//! Pure scheduling relations. No knowledge of pools, migration or topology.
use crate::*;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Relation {
    Independent,
    Exclusive,
    YieldTo(&'static str),
}
pub trait SchedulingPolicy<D>: Send + Sync + 'static {
    fn admission(&self, incoming: &D, existing: &D) -> Conflict;
    fn relation(&self, task: &D, other: &D) -> Relation;
    /// Queued precedence; equal priorities preserve FIFO. Yield targets should
    /// outrank the tasks yielding to them and intervening queued competitors.
    fn priority(&self, _task: &D) -> i32 {
        0
    }
}
pub struct Candidate<'a, D> {
    pub id: TaskId,
    pub descriptor: &'a D,
    pub started: bool,
    pub paused: bool,
}
#[derive(Default)]
pub struct Plan {
    pub pauses: BTreeMap<TaskId, BTreeSet<String>>,
    pub blockers: BTreeMap<TaskId, Vec<TaskId>>,
    pub ready: Vec<TaskId>,
}
/// Equal priority uses input FIFO order. Yield is directional; policies must not introduce cycles.
/// Apply pause requests, then recompute with fresh ACKs before opening start gates.
pub fn plan<D: 'static>(policy: &dyn SchedulingPolicy<D>, tasks: &[Candidate<'_, D>]) -> Plan {
    let mut result = Plan::default();
    for (index, task) in tasks.iter().enumerate() {
        let pauses = result.pauses.entry(task.id).or_default();
        let blockers = result.blockers.entry(task.id).or_default();
        for (other_index, other) in tasks.iter().enumerate() {
            if index == other_index {
                continue;
            }
            let relation = policy.relation(task.descriptor, other.descriptor);
            let reverse = policy.relation(other.descriptor, task.descriptor);
            let blocked = if let Relation::YieldTo(reason) = relation {
                pauses.insert(format!("{}:{reason}", other.id));
                true
            } else if matches!(reverse, Relation::YieldTo(_)) {
                other.started && !other.paused
            } else if relation == Relation::Exclusive || reverse == Relation::Exclusive {
                let before = policy.priority(other.descriptor) > policy.priority(task.descriptor)
                    || (policy.priority(other.descriptor) == policy.priority(task.descriptor)
                        && other_index < index);
                other.started || (!task.started && before)
            } else {
                false
            };
            if blocked {
                blockers.push(other.id);
            }
        }
        if !task.started && blockers.is_empty() {
            result.ready.push(task.id);
        }
    }
    result
}
