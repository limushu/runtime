//! Generic lifecycle of rule-produced waits. No storage operation kinds belong here.
use super::{
    registry::{TaskRecord, TaskRegistry},
    submission::Submission,
    *,
};

struct Entry {
    record: Arc<TaskRecord>,
    phase: Phase,
}
enum Phase {
    /// Registered from storage, awaiting a factory or business reconciliation.
    Recovering,
    Active {
        control: RunControl,
        start: Option<StartPermit>,
    },
    /// Unknown effects retain ownership and any suspension they requested.
    Fenced,
}
struct Relation {
    requester: TaskId,
    predecessor: TaskId,
    decision: ConflictDecision,
    reason: &'static str,
}
pub(super) struct State {
    entries: BTreeMap<TaskId, Entry>,
    relations: Vec<Relation>,
    rules: Arc<dyn TaskRules>,
    registry: Arc<Mutex<TaskRegistry>>,
    history_limit: usize,
}
impl State {
    pub fn new(
        rules: Arc<dyn TaskRules>,
        registry: Arc<Mutex<TaskRegistry>>,
        history_limit: usize,
    ) -> Self {
        Self {
            entries: BTreeMap::new(),
            relations: vec![],
            rules,
            registry,
            history_limit,
        }
    }
    fn inspect(&self, operation: &PoolOperation) -> Outcome<Vec<ConflictMatch>> {
        let mut conflicts = vec![];
        for (id, entry) in &self.entries {
            let conflict = self.rules.conflict(operation, &entry.record.operation);
            if let ConflictDecision::Reject(reason) = conflict.decision {
                return Err(crate::support::reject(reason));
            }
            if conflict.decision != ConflictDecision::Independent {
                if matches!(entry.phase, Phase::Recovering | Phase::Fenced) {
                    return Err(crate::support::reject(
                        "unfinished task requires recovery or reconciliation",
                    ));
                }
                conflicts.push(ConflictMatch {
                    existing: *id,
                    conflict,
                });
            }
        }
        Ok(conflicts)
    }
    fn link(&mut self, id: TaskId, conflicts: &[ConflictMatch]) {
        self.relations
            .extend(conflicts.iter().map(|found| Relation {
                requester: id,
                predecessor: found.existing,
                decision: found.conflict.decision.clone(),
                reason: found.conflict.reason,
            }));
    }
    /// Register the whole recovery set before reconstructing any execution. These are
    /// ordinary logical tasks; a missing Future is an explicit lifecycle state.
    pub fn restore(&mut self, tasks: Vec<Arc<TaskRecord>>) {
        for record in tasks {
            let operation = &record.operation;
            let conflicts: Vec<_> = self
                .entries
                .iter()
                .filter_map(|(id, entry)| {
                    let conflict = self.rules.conflict(operation, &entry.record.operation);
                    (conflict.decision != ConflictDecision::Independent).then_some(ConflictMatch {
                        existing: *id,
                        conflict,
                    })
                })
                .collect();
            self.link(record.id, &conflicts);
            self.registry
                .lock()
                .unwrap()
                .records
                .insert(record.id, record.clone());
            self.entries.insert(
                record.id,
                Entry {
                    record,
                    phase: Phase::Recovering,
                },
            );
        }
    }
    /// Admission is synchronous: validate every rule before creating or cancelling work.
    pub fn admit(
        &mut self,
        submission: Submission,
        driver: &mut WorkflowDriver,
        closed: bool,
        limit: usize,
    ) -> Option<Box<dyn FnOnce() + Send>> {
        self.reap();
        let restoring = submission.reconstructing;
        let decision = if closed && !submission.administrative {
            Err(Failure::Unavailable)
        } else if self
            .entries
            .values()
            .filter(|e| matches!(e.phase, Phase::Active { .. }))
            .count()
            >= limit
        {
            Err(Failure::Busy)
        } else if restoring {
            match self.entries.get(&submission.task_id) {
                Some(Entry {
                    phase: Phase::Recovering | Phase::Fenced,
                    ..
                }) => {
                    // Existing records must still obey the current rules. A rule
                    // rejecting reconstruction is reported, never silently queued.
                    match self.relations.iter().find_map(|relation| {
                        if relation.requester == submission.task_id
                            && let ConflictDecision::Reject(reason) = relation.decision
                        {
                            Some(reason)
                        } else {
                            None
                        }
                    }) {
                        Some(reason) => Err(crate::support::reject(reason)),
                        None => Ok(vec![]),
                    }
                }
                _ => Err(crate::support::reject(
                    "task is not awaiting reconstruction",
                )),
            }
        } else {
            self.inspect(&submission.operation)
        };
        let conflicts = match decision {
            Ok(conflicts) => conflicts,
            Err(error) => {
                (submission.accept)(Err(error));
                return None;
            }
        };
        let accepted = (submission.accept)(Ok(driver))?;
        let record = if restoring {
            self.entries[&submission.task_id].record.clone()
        } else {
            let record = TaskRecord::new(
                submission.task_id,
                submission.name,
                submission.operation,
                TaskStatus::Queued,
                None,
            );
            self.link(submission.task_id, &conflicts);
            self.registry
                .lock()
                .unwrap()
                .records
                .insert(submission.task_id, record.clone());
            record
        };
        record.attach(
            accepted.control.clone(),
            driver
                .view()
                .observe_execution(accepted.control.id)
                .expect("new execution is retained"),
            conflicts,
        );
        self.entries.insert(
            submission.task_id,
            Entry {
                record,
                phase: Phase::Active {
                    control: accepted.control,
                    start: Some(accepted.start),
                },
            },
        );
        self.pump();
        Some(accepted.publish)
    }
    pub fn cancel_inactive(&mut self, id: TaskId) {
        if let Some(entry) = self.entries.remove(&id) {
            entry.record.cancel_inactive();
            self.registry
                .lock()
                .unwrap()
                .retain_history(id, self.history_limit);
        }
        self.relations
            .retain(|r| r.requester != id && r.predecessor != id);
    }
    /// A terminal result releases relations only when external effects are known.
    pub fn reap(&mut self) {
        let terminal: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(id, entry)| match &entry.phase {
                Phase::Active { control, .. }
                    if matches!(control.state(), RunState::Completed | RunState::Failed(_)) =>
                {
                    Some((*id, control.state()))
                }
                _ => None,
            })
            .collect();
        for (id, state) in terminal {
            if matches!(state, RunState::Failed(Failure::Uncertain(_))) {
                self.entries.get_mut(&id).unwrap().phase = Phase::Fenced;
            } else {
                self.entries.remove(&id).unwrap().record.set_waits(vec![]);
                self.relations
                    .retain(|r| r.requester != id && r.predecessor != id);
                self.registry
                    .lock()
                    .unwrap()
                    .retain_history(id, self.history_limit);
            }
        }
    }
    pub fn snapshot(&self) -> SchedulingSnapshot {
        let mut snapshot = SchedulingSnapshot::default();
        for entry in self.entries.values() {
            match &entry.phase {
                Phase::Recovering => snapshot.recovering += 1,
                Phase::Fenced => snapshot.fenced += 1,
                Phase::Active { start: Some(_), .. } => snapshot.queued += 1,
                Phase::Active { start: None, .. } => snapshot.running += 1,
            }
        }
        snapshot
    }
    /// Re-evaluate stateful conditions after each driver/control event. Never await a
    /// pause here: the Driver must keep polling to deliver the acknowledgement.
    pub fn pump(&mut self) {
        let suspended: BTreeSet<_> = self
            .relations
            .iter()
            .filter(|r| r.decision == ConflictDecision::SuspendThenWait)
            .map(|r| r.predecessor)
            .collect();
        for (id, entry) in &self.entries {
            if let Phase::Active { control, .. } = &entry.phase {
                let _ = control.set_system_pause(suspended.contains(id));
            }
        }
        let mut waits: BTreeMap<TaskId, Vec<TaskWait>> = BTreeMap::new();
        for relation in &self.relations {
            let predecessor = &self.entries[&relation.predecessor];
            let unresolved = matches!(predecessor.phase, Phase::Recovering | Phase::Fenced);
            let condition = if unresolved {
                WaitCondition::Reconciliation
            } else {
                WaitCondition::Completion
            };
            match relation.decision {
                ConflictDecision::SuspendThenWait => {
                    waits
                        .entry(relation.predecessor)
                        .or_default()
                        .push(TaskWait {
                            task: relation.requester,
                            condition: WaitCondition::Release,
                            reason: relation.reason,
                        });
                    let safe = match &predecessor.phase {
                        Phase::Active { control, start } => {
                            start.is_some() || control.pause_acknowledged()
                        }
                        _ => false,
                    };
                    if !safe {
                        waits.entry(relation.requester).or_default().push(TaskWait {
                            task: relation.predecessor,
                            condition: if unresolved {
                                condition
                            } else {
                                WaitCondition::SafePause
                            },
                            reason: relation.reason,
                        });
                    }
                }
                ConflictDecision::CancelThenWait
                | ConflictDecision::Wait
                | ConflictDecision::Reject(_) => {
                    waits.entry(relation.requester).or_default().push(TaskWait {
                        task: relation.predecessor,
                        condition,
                        reason: relation.reason,
                    });
                    if relation.decision == ConflictDecision::CancelThenWait
                        && let Phase::Active { control, .. } = &predecessor.phase
                        && !control.cancellation_requested()
                    {
                        control.cancel();
                    }
                }
                ConflictDecision::Independent => unreachable!("independent tasks have no relation"),
            }
        }
        let fences: BTreeSet<_> = self
            .entries
            .iter()
            .filter_map(|(id, entry)| matches!(entry.phase, Phase::Fenced).then_some(*id))
            .collect();
        for (id, entry) in &mut self.entries {
            let pending = waits.remove(id).unwrap_or_default();
            let fenced = pending.iter().any(|wait| {
                wait.condition == WaitCondition::Reconciliation && fences.contains(&wait.task)
            });
            let ready = pending.is_empty();
            let changed = entry.record.set_waits(pending);
            if let Phase::Active { control, start } = &mut entry.phase {
                if changed {
                    control.recheck();
                }
                if fenced {
                    if let Some(permit) = start.take() {
                        permit.reject(crate::support::reject(
                            "predecessor requires reconciliation",
                        ));
                    }
                } else if ready && let Some(permit) = start.take() {
                    permit.start();
                }
            }
        }
    }
}
