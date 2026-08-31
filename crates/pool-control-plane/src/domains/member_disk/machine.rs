use super::protocol::MemberDiskState;
use control_runtime::{RuntimeError, RuntimeResult};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskWorkflowKind {
    Offline,
    Online,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskEvent {
    DiskDown,
    DiskUp,
    BeginDrain,
    DrainCompleted,
    OnlineSettled,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskEffect {
    Continue,
    Complete,
    Reject(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberDiskTransition {
    pub from: MemberDiskState,
    pub event: MemberDiskEvent,
    pub to: MemberDiskState,
    pub effect: MemberDiskEffect,
}

impl MemberDiskTransition {
    fn new(
        from: MemberDiskState,
        event: MemberDiskEvent,
        to: MemberDiskState,
        effect: MemberDiskEffect,
    ) -> Self {
        Self {
            from,
            event,
            to,
            effect,
        }
    }
}

/// Pure business statechart for one MemberDisk.
///
/// It knows nothing about Runtime phases, tasks or cancellation. The service
/// combines this transition with the current ObjectSlot view during admission.
pub struct MemberDiskMachine;

impl MemberDiskMachine {
    pub fn transition(
        state: MemberDiskState,
        event: MemberDiskEvent,
    ) -> RuntimeResult<MemberDiskTransition> {
        use MemberDiskEffect::{Complete, Continue, Reject};
        use MemberDiskEvent::{BeginDrain, DiskDown, DiskUp, DrainCompleted, OnlineSettled};
        use MemberDiskState::{Da, Di, Removed, Ua, Ui};

        let transition = match (state, event) {
            (Ua, DiskDown) => Self::change(Ua, DiskDown, Da, Continue),
            (Da, DiskDown) => Self::same(Da, DiskDown, Continue),
            (Di, DiskDown) => Self::same(Di, DiskDown, Continue),
            (Ui, DiskDown) => Self::change(Ui, DiskDown, Di, Continue),
            (Removed, DiskDown) => Self::same(Removed, DiskDown, Complete),

            (Ua, DiskUp) => Self::same(Ua, DiskUp, Complete),
            (Da, DiskUp) => Self::change(Da, DiskUp, Ua, Continue),
            (Di, DiskUp) => Self::change(Di, DiskUp, Ui, Continue),
            (Ui, DiskUp) => Self::same(Ui, DiskUp, Continue),
            (Removed, DiskUp) => Self::same(
                Removed,
                DiskUp,
                Reject("removed disk requires an explicit rejoin operation"),
            ),

            (Da, BeginDrain) => Self::change(Da, BeginDrain, Di, Continue),
            (Di, BeginDrain) => Self::same(Di, BeginDrain, Continue),
            (Di, DrainCompleted) => Self::change(Di, DrainCompleted, Removed, Continue),
            (Ui, OnlineSettled) => Self::change(Ui, OnlineSettled, Ua, Continue),
            (Ua, OnlineSettled) => Self::same(Ua, OnlineSettled, Continue),

            _ => {
                return Err(RuntimeError::InvalidState(format!(
                    "no disk transition for state={state:?}, event={event:?}"
                )))
            }
        };
        Ok(transition)
    }

    fn change(
        from: MemberDiskState,
        event: MemberDiskEvent,
        to: MemberDiskState,
        effect: MemberDiskEffect,
    ) -> MemberDiskTransition {
        MemberDiskTransition::new(from, event, to, effect)
    }

    fn same(
        state: MemberDiskState,
        event: MemberDiskEvent,
        effect: MemberDiskEffect,
    ) -> MemberDiskTransition {
        Self::change(state, event, state, effect)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_updates_business_state_without_runtime_knowledge() {
        let transition =
            MemberDiskMachine::transition(MemberDiskState::Di, MemberDiskEvent::DiskUp).unwrap();
        assert_eq!(transition.to, MemberDiskState::Ui);
        assert_eq!(transition.effect, MemberDiskEffect::Continue);
    }

    #[test]
    fn repeated_fact_is_idempotent() {
        let transition =
            MemberDiskMachine::transition(MemberDiskState::Di, MemberDiskEvent::DiskDown).unwrap();
        assert_eq!(transition.to, MemberDiskState::Di);
    }

    #[test]
    fn illegal_internal_event_is_rejected() {
        assert!(MemberDiskMachine::transition(
            MemberDiskState::Ua,
            MemberDiskEvent::DrainCompleted,
        )
        .is_err());
    }
}
