use super::model::{MemberDiskState, PhysicalState};
use control_runtime::{RuntimeError, RuntimeResult};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskEffect {
    RunWorkflow(MemberDiskWorkflowKind),
    AlreadySatisfied,
    Reject(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskTransition {
    pub from: MemberDiskState,
    pub input: MemberDiskInput,
    pub to: MemberDiskState,
    pub effect: MemberDiskEffect,
}

impl MemberDiskTransition {
    fn change(
        from: MemberDiskState,
        input: MemberDiskInput,
        to: MemberDiskState,
        effect: MemberDiskEffect,
    ) -> Self {
        Self {
            from,
            input,
            to,
            effect,
        }
    }

    fn same(state: MemberDiskState, input: MemberDiskInput, effect: MemberDiskEffect) -> Self {
        Self::change(state, input, state, effect)
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
        use MemberDiskEffect::{AlreadySatisfied, Reject, RunWorkflow};
        use MemberDiskInput::{DrainCompleted, DrainStarted, OnlineSettled, Physical};
        use MemberDiskState::{Da, Di, Removed, Ua, Ui};
        use MemberDiskWorkflowKind::{Offline, Online};
        use PhysicalState::{Down, Up};

        let transition = match (state, input) {
            (Ua, Physical(Down)) => {
                MemberDiskTransition::change(Ua, Physical(Down), Da, RunWorkflow(Offline))
            }
            (Da, Physical(Down)) => {
                MemberDiskTransition::same(Da, Physical(Down), RunWorkflow(Offline))
            }
            (Di, Physical(Down)) => {
                MemberDiskTransition::same(Di, Physical(Down), RunWorkflow(Offline))
            }
            (Ui, Physical(Down)) => {
                MemberDiskTransition::change(Ui, Physical(Down), Di, RunWorkflow(Offline))
            }
            (Removed, Physical(Down)) => {
                MemberDiskTransition::same(Removed, Physical(Down), AlreadySatisfied)
            }

            (Ua, Physical(Up)) => MemberDiskTransition::same(Ua, Physical(Up), AlreadySatisfied),
            (Da, Physical(Up)) => {
                MemberDiskTransition::change(Da, Physical(Up), Ua, RunWorkflow(Online))
            }
            (Di, Physical(Up)) => {
                MemberDiskTransition::change(Di, Physical(Up), Ui, RunWorkflow(Online))
            }
            (Ui, Physical(Up)) => MemberDiskTransition::same(Ui, Physical(Up), RunWorkflow(Online)),
            (Removed, Physical(Up)) => MemberDiskTransition::same(
                Removed,
                Physical(Up),
                Reject("removed member disk requires an explicit rejoin operation"),
            ),

            (Da, DrainStarted) => {
                MemberDiskTransition::change(Da, DrainStarted, Di, RunWorkflow(Offline))
            }
            (Di, DrainStarted) => {
                MemberDiskTransition::same(Di, DrainStarted, RunWorkflow(Offline))
            }
            (Di, DrainCompleted) => {
                MemberDiskTransition::change(Di, DrainCompleted, Removed, AlreadySatisfied)
            }
            (Ui, OnlineSettled) => {
                MemberDiskTransition::change(Ui, OnlineSettled, Ua, AlreadySatisfied)
            }
            (Ua, OnlineSettled) => MemberDiskTransition::same(Ua, OnlineSettled, AlreadySatisfied),

            _ => {
                return Err(RuntimeError::InvalidState(format!(
                    "no member disk transition for state={state:?}, input={input:?}"
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
    fn disk_map_fact_is_the_statechart_input() {
        let transition = MemberDiskMachine::transition(
            MemberDiskState::Ua,
            MemberDiskInput::Physical(PhysicalState::Down),
        )
        .unwrap();
        assert_eq!(transition.to, MemberDiskState::Da);
        assert_eq!(
            transition.effect,
            MemberDiskEffect::RunWorkflow(MemberDiskWorkflowKind::Offline)
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
