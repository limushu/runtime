use super::machine::{
    MemberDiskEffect, MemberDiskInput, MemberDiskMachine, MemberDiskTransition,
    MemberDiskWorkflowKind,
};
use super::model::{
    AllocationState, BlkSize, MemberDiskPatch, MemberDiskRecord, MemberDiskSnapshot,
    MemberDiskState, MembershipState, PhysicalState,
};
use crate::kernel::BlkId;
use control_runtime::{RuntimeError, RuntimeResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MemberDiskActivity {
    Idle,
    Busy {
        current: MemberDiskWorkflowKind,
        replacement: Option<MemberDiskWorkflowKind>,
    },
}

impl MemberDiskActivity {
    fn target(&self) -> Option<MemberDiskWorkflowKind> {
        match self {
            Self::Idle => None,
            Self::Busy {
                current,
                replacement,
            } => replacement.or(Some(*current)),
        }
    }

    fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MemberDiskActorDecision {
    Start,
    Join,
    Replace { cause: &'static str },
    Complete(MemberDiskSnapshot),
    Reject(&'static str),
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedMemberDiskMutation {
    expected_revision: u64,
    record: MemberDiskRecord,
    physical: PhysicalState,
    transition: MemberDiskTransition,
    persist_record: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedRecordUpdate {
    expected_revision: u64,
    record: MemberDiskRecord,
}

impl PlannedRecordUpdate {
    pub(crate) fn record(&self) -> &MemberDiskRecord {
        &self.record
    }
}

impl PlannedMemberDiskMutation {
    pub(crate) fn record(&self) -> &MemberDiskRecord {
        &self.record
    }

    pub(crate) fn requires_persistence(&self) -> bool {
        self.persist_record
    }
}

/// One logical MemberDisk actor. It owns the complete domain entity and the
/// latest DiskMap observation. It is driven by the MemberDisk service root
/// task; it does not create a Tokio task or mailbox per disk.
#[derive(Debug, Clone)]
pub(crate) struct MemberDiskActor {
    record: MemberDiskRecord,
    physical: PhysicalState,
}

impl MemberDiskActor {
    pub(crate) fn restore(record: MemberDiskRecord) -> Self {
        Self {
            record,
            physical: PhysicalState::Down,
        }
    }

    pub(crate) fn snapshot(&self) -> MemberDiskSnapshot {
        let block_size = BlkSize::for_capacity(self.record.spec().capacity);
        MemberDiskSnapshot {
            spec: self.record.spec().clone(),
            physical_state: self.physical,
            allocation_state: self.record.allocation_state(),
            membership_state: self.record.membership_state(),
            operational_state: self.operational_state(),
            block_size,
            total_blocks: self.record.allocation().total_blocks(),
            allocated_blocks: self.record.allocation().allocated_blocks(),
            revision: self.record.revision(),
        }
    }

    pub(crate) fn operational_state(&self) -> MemberDiskState {
        MemberDiskState::project(
            self.physical,
            self.record.allocation_state(),
            self.record.membership_state(),
        )
    }

    pub(crate) fn plan_patch(&self, patch: MemberDiskPatch) -> PlannedRecordUpdate {
        let mut record = self.record.clone();
        record.apply_patch(patch);
        PlannedRecordUpdate {
            expected_revision: self.record.revision(),
            record,
        }
    }

    pub(crate) fn plan_allocate(&self) -> RuntimeResult<(PlannedRecordUpdate, BlkId)> {
        if !matches!(self.record.allocation_state(), AllocationState::Active)
            || !matches!(self.physical, PhysicalState::Up)
            || !matches!(self.record.membership_state(), MembershipState::Member)
        {
            return Err(RuntimeError::Rejected(
                "member disk is not currently allocatable".into(),
            ));
        }
        let mut record = self.record.clone();
        let blk = record.allocate_one()?;
        Ok((
            PlannedRecordUpdate {
                expected_revision: self.record.revision(),
                record,
            },
            blk,
        ))
    }

    pub(crate) fn plan_release(&self, blk: &BlkId) -> RuntimeResult<PlannedRecordUpdate> {
        let mut record = self.record.clone();
        record.release(blk)?;
        Ok(PlannedRecordUpdate {
            expected_revision: self.record.revision(),
            record,
        })
    }

    pub(crate) fn commit_record(&mut self, update: PlannedRecordUpdate) -> RuntimeResult<()> {
        if self.record.revision() != update.expected_revision {
            return Err(RuntimeError::Cancelled);
        }
        self.record = update.record;
        Ok(())
    }

    pub(crate) fn can_delete(&self) -> bool {
        matches!(self.record.membership_state(), MembershipState::Removed)
            && self.record.allocation().allocated_blocks() == 0
    }

    pub(crate) fn admit_physical(
        &mut self,
        physical: PhysicalState,
        activity: MemberDiskActivity,
    ) -> RuntimeResult<MemberDiskActorDecision> {
        let expected_kind = match physical {
            PhysicalState::Down => MemberDiskWorkflowKind::Offline,
            PhysicalState::Up => MemberDiskWorkflowKind::Online,
        };
        if activity.target() == Some(expected_kind) {
            self.physical = physical;
            return Ok(MemberDiskActorDecision::Join);
        }

        let mutation = self.plan(MemberDiskInput::Physical(physical))?;
        let effect = mutation.transition.effect.clone();
        self.commit(mutation)?;
        Ok(match effect {
            MemberDiskEffect::AlreadySatisfied => {
                MemberDiskActorDecision::Complete(self.snapshot())
            }
            MemberDiskEffect::Reject(reason) => MemberDiskActorDecision::Reject(reason),
            MemberDiskEffect::RunWorkflow(_) if activity.is_idle() => {
                MemberDiskActorDecision::Start
            }
            MemberDiskEffect::RunWorkflow(MemberDiskWorkflowKind::Offline) => {
                MemberDiskActorDecision::Replace {
                    cause: "DiskMap observed Down; replace the active Online intent",
                }
            }
            MemberDiskEffect::RunWorkflow(MemberDiskWorkflowKind::Online) => {
                MemberDiskActorDecision::Replace {
                    cause: "DiskMap observed Up; settle the active Offline intent",
                }
            }
            MemberDiskEffect::RunWorkflow(MemberDiskWorkflowKind::Metadata) => {
                return Err(RuntimeError::Internal(
                    "physical fact produced metadata workflow".into(),
                ))
            }
        })
    }

    pub(crate) fn plan(&self, input: MemberDiskInput) -> RuntimeResult<PlannedMemberDiskMutation> {
        let transition = MemberDiskMachine::transition(self.operational_state(), input)?;
        let mut record = self.record.clone();
        let mut physical = self.physical;
        let persist_record = match input {
            MemberDiskInput::Physical(value) => {
                physical = value;
                false
            }
            MemberDiskInput::DrainStarted => {
                record.set_allocation_state(AllocationState::Inactive);
                true
            }
            MemberDiskInput::DrainCompleted => {
                record.set_membership_state(MembershipState::Removed);
                true
            }
            MemberDiskInput::OnlineSettled => {
                record.set_allocation_state(AllocationState::Active);
                true
            }
        };
        let projected = MemberDiskState::project(
            physical,
            record.allocation_state(),
            record.membership_state(),
        );
        if projected != transition.to {
            return Err(RuntimeError::Internal(format!(
                "statechart projected {:?}, entity mutation projected {projected:?}",
                transition.to
            )));
        }
        Ok(PlannedMemberDiskMutation {
            expected_revision: self.record.revision(),
            record,
            physical,
            transition,
            persist_record,
        })
    }

    pub(crate) fn commit(
        &mut self,
        mutation: PlannedMemberDiskMutation,
    ) -> RuntimeResult<MemberDiskTransition> {
        if self.record.revision() != mutation.expected_revision {
            return Err(RuntimeError::Cancelled);
        }
        let from = self.operational_state();
        self.record = mutation.record;
        if matches!(mutation.transition.input, MemberDiskInput::Physical(_)) {
            self.physical = mutation.physical;
        }
        let mut transition = mutation.transition;
        transition.from = from;
        transition.to = self.operational_state();
        Ok(transition)
    }
}

#[cfg(test)]
mod tests {
    use super::super::model::{MediaClass, MemberDiskSpec};
    use super::*;
    use crate::kernel::{ByteCount, MemberDiskId, PhysicalDiskId, PoolId, TierId};

    fn actor() -> MemberDiskActor {
        let spec = MemberDiskSpec::new(
            MemberDiskId::new("md-1"),
            PhysicalDiskId::new("pd-1"),
            PoolId::new("pool-1"),
            TierId::new("tier-1"),
            MediaClass::new("ssd"),
            ByteCount::new(1024 * 1024 * 1024),
            Vec::new(),
        );
        MemberDiskActor::restore(MemberDiskRecord::new(spec))
    }

    #[test]
    fn a_stable_record_commit_survives_a_new_physical_observation() {
        let mut actor = actor();
        assert_eq!(
            actor
                .admit_physical(PhysicalState::Down, MemberDiskActivity::Idle)
                .unwrap(),
            MemberDiskActorDecision::Start
        );
        let drain_started = actor.plan(MemberDiskInput::DrainStarted).unwrap();

        assert!(matches!(
            actor
                .admit_physical(
                    PhysicalState::Up,
                    MemberDiskActivity::Busy {
                        current: MemberDiskWorkflowKind::Offline,
                        replacement: None,
                    },
                )
                .unwrap(),
            MemberDiskActorDecision::Replace { .. }
        ));

        let transition = actor.commit(drain_started).unwrap();
        assert_eq!(transition.to, MemberDiskState::Ui);
        assert_eq!(actor.snapshot().physical_state, PhysicalState::Up);
        assert_eq!(actor.snapshot().allocation_state, AllocationState::Inactive);
    }
}
