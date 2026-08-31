use super::model::{MemberDiskState, PhysicalState};
use control_runtime::{RuntimeError, RuntimeResult, Transition, TransitionEffect};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskWorkflowKind {
    Offline,
    Online,
    Metadata,
}

/// Inputs consumed by the MemberDisk statechart.
///
/// Physical facts use the exact `PhysicalState` delivered by DiskMap. The
/// remaining variants are internal, stable workflow progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskInput {
    Physical(PhysicalState),
    DrainStarted,
    DrainCompleted,
    OnlineSettled,
}

pub type MemberDiskEffect = TransitionEffect<MemberDiskWorkflowKind>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskTransition {
    pub from: MemberDiskState,
    pub input: MemberDiskInput,
    pub to: MemberDiskState,
    pub effect: MemberDiskEffect,
}

impl MemberDiskTransition {
    fn from_decision(
        from: MemberDiskState,
        input: MemberDiskInput,
        decision: Transition<MemberDiskState, MemberDiskWorkflowKind>,
    ) -> Self {
        let (to, effect) = decision.into_parts();
        Self {
            from,
            input,
            to,
            effect,
        }
    }
}

/// Pure business statechart. It projects legal state movement but owns no
/// entity data, mailbox, Tokio task or cancellation token.
pub struct MemberDiskMachine;

impl MemberDiskMachine {
    pub fn transition(
        state: MemberDiskState,
        input: MemberDiskInput,
    ) -> RuntimeResult<MemberDiskTransition> {
        use MemberDiskInput::{DrainCompleted, DrainStarted, OnlineSettled, Physical};
        use MemberDiskState::{Da, Di, Removed, Ua, Ui};
        use MemberDiskWorkflowKind::{Offline, Online};
        use PhysicalState::{Down, Up};

        let decision = match (state, input) {
            (Ua, Physical(Down)) => Transition::to(Da).ensure(Offline),
            (Da, Physical(Down)) => Transition::to(Da).ensure(Offline),
            (Di, Physical(Down)) => Transition::to(Di).ensure(Offline),
            (Ui, Physical(Down)) => Transition::to(Di).ensure(Offline),
            (Removed, Physical(Down)) => Transition::to(Removed),

            (Ua, Physical(Up)) => Transition::to(Ua),
            (Da, Physical(Up)) => Transition::to(Ua).ensure(Online),
            (Di, Physical(Up)) => Transition::to(Ui).ensure(Online),
            (Ui, Physical(Up)) => Transition::to(Ui).ensure(Online),
            (Removed, Physical(Up)) => Transition::to(Removed)
                .reject("removed member disk requires an explicit rejoin operation"),

            (Da, DrainStarted) => Transition::to(Di).ensure(Offline),
            (Di, DrainStarted) => Transition::to(Di).ensure(Offline),
            (Di, DrainCompleted) => Transition::to(Removed),
            (Ui, OnlineSettled) => Transition::to(Ua),
            (Ua, OnlineSettled) => Transition::to(Ua),

            _ => {
                return Err(RuntimeError::InvalidState(format!(
                    "no member disk transition for state={state:?}, input={input:?}"
                )))
            }
        };
        Ok(MemberDiskTransition::from_decision(state, input, decision))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disk_map_fact_is_the_statechart_input() {
        let transition = MemberDiskMachine::transition(
            MemberDiskState::Ua,
            MemberDiskInput::Physical(PhysicalState::Down),
        )
        .unwrap();
        assert_eq!(transition.to, MemberDiskState::Da);
        assert_eq!(
            transition.effect,
            MemberDiskEffect::Ensure(MemberDiskWorkflowKind::Offline)
        );
    }

    #[test]
    fn illegal_progress_is_rejected() {
        assert!(MemberDiskMachine::transition(
            MemberDiskState::Ua,
            MemberDiskInput::DrainCompleted,
        )
        .is_err());
    }
}
