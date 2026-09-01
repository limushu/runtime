use super::model::{AllocationState, MemberDiskState, MembershipState, PhysicalState};
use control_runtime::{RuntimeError, RuntimeResult, Transition, TransitionEffect};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskWorkflowKind {
    Offline,
    Online,
    Metadata,
}

/// Facts and stable workflow progress understood by the MemberDisk state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskEvent {
    Physical(PhysicalState),
    DrainStarted,
    DrainCompleted,
    OnlineSettled,
}

/// The concrete mutation required to make the MemberDisk object match the
/// state transition. The model applies this after the pure machine decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskChange {
    ObservePhysical(PhysicalState),
    SetAllocation(AllocationState),
    SetMembership(MembershipState),
}

pub type MemberDiskEffect = TransitionEffect<MemberDiskWorkflowKind>;
pub type MemberDiskDecision = Transition<MemberDiskState, MemberDiskWorkflowKind, MemberDiskChange>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskTransition {
    pub from: MemberDiskState,
    pub event: MemberDiskEvent,
    pub to: MemberDiskState,
    pub effect: MemberDiskEffect,
}

/// Pure business state machine. It owns no entity, actor, Future or lock.
pub struct MemberDiskMachine;

impl MemberDiskMachine {
    pub fn transition(
        state: MemberDiskState,
        event: MemberDiskEvent,
    ) -> RuntimeResult<MemberDiskDecision> {
        use AllocationState::{Active, Inactive};
        use MemberDiskEvent::{DrainCompleted, DrainStarted, OnlineSettled, Physical};
        use MemberDiskState::{Da, Di, Removed, Ua, Ui};
        use MemberDiskWorkflowKind::{Offline, Online};
        use MembershipState::Removed as RemovedMembership;
        use PhysicalState::{Down, Up};

        let transition = match (state, event) {
            (Ua, Physical(Down)) => Transition::to(Da)
                .change(MemberDiskChange::ObservePhysical(Down))
                .ensure(Offline),
            (Da, Physical(Down)) => Transition::to(Da)
                .change(MemberDiskChange::ObservePhysical(Down))
                .ensure(Offline),
            (Di, Physical(Down)) => Transition::to(Di)
                .change(MemberDiskChange::ObservePhysical(Down))
                .ensure(Offline),
            (Ui, Physical(Down)) => Transition::to(Di)
                .change(MemberDiskChange::ObservePhysical(Down))
                .ensure(Offline),
            (Removed, Physical(Down)) => {
                Transition::to(Removed).change(MemberDiskChange::ObservePhysical(Down))
            }

            (Ua, Physical(Up)) => Transition::to(Ua).change(MemberDiskChange::ObservePhysical(Up)),
            (Da, Physical(Up)) => Transition::to(Ua)
                .change(MemberDiskChange::ObservePhysical(Up))
                .ensure(Online),
            (Di, Physical(Up)) => Transition::to(Ui)
                .change(MemberDiskChange::ObservePhysical(Up))
                .ensure(Online),
            (Ui, Physical(Up)) => Transition::to(Ui)
                .change(MemberDiskChange::ObservePhysical(Up))
                .ensure(Online),
            (Removed, Physical(Up)) => Transition::to(Removed)
                .change(MemberDiskChange::ObservePhysical(Up))
                .reject("removed member disk requires an explicit rejoin operation"),

            (Da, DrainStarted) => {
                Transition::to(Di).change(MemberDiskChange::SetAllocation(Inactive))
            }
            (Di, DrainStarted) => Transition::to(Di),
            (Di, DrainCompleted) => {
                Transition::to(Removed).change(MemberDiskChange::SetMembership(RemovedMembership))
            }
            (Ui, OnlineSettled) => {
                Transition::to(Ua).change(MemberDiskChange::SetAllocation(Active))
            }
            (Ua, OnlineSettled) => Transition::to(Ua),

            _ => {
                return Err(RuntimeError::InvalidState(format!(
                    "no member disk transition for state={state:?}, event={event:?}"
                )))
            }
        };
        Ok(transition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_map_fact_describes_state_change_and_object_mutation() {
        let transition = MemberDiskMachine::transition(
            MemberDiskState::Ua,
            MemberDiskEvent::Physical(PhysicalState::Down),
        )
        .unwrap();
        let (next, effect, change) = transition.into_parts();
        assert_eq!(next, MemberDiskState::Da);
        assert_eq!(
            effect,
            MemberDiskEffect::Ensure(MemberDiskWorkflowKind::Offline)
        );
        assert_eq!(
            change,
            Some(MemberDiskChange::ObservePhysical(PhysicalState::Down))
        );
    }

    #[test]
    fn illegal_progress_is_rejected() {
        assert!(MemberDiskMachine::transition(
            MemberDiskState::Ua,
            MemberDiskEvent::DrainCompleted,
        )
        .is_err());
    }
}
