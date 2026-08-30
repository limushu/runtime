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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberDiskActivity {
    Idle,
    Pending(MemberDiskWorkflowKind),
    Running(MemberDiskWorkflowKind),
    Cancelling {
        current: MemberDiskWorkflowKind,
        next: MemberDiskWorkflowKind,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelMode {
    /// Do not drop the old Future. Propagate cancellation and wait until its
    /// downstream work reports a stable result before starting the replacement.
    SettleThenStop,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemberDiskEffect {
    None,
    Start(MemberDiskWorkflowKind),
    Join,
    Replace {
        next: MemberDiskWorkflowKind,
        mode: CancelMode,
        cause: &'static str,
    },
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

/// Pure statechart for one MemberDisk.
///
/// This type owns no metadata, lock, Router, or Future. It is the reviewable
/// business transition table; DiskService applies the returned transition.
pub struct MemberDiskMachine;

impl MemberDiskMachine {
    pub fn transition(
        state: MemberDiskState,
        event: MemberDiskEvent,
        activity: MemberDiskActivity,
    ) -> RuntimeResult<MemberDiskTransition> {
        use MemberDiskActivity::{Cancelling, Idle, Pending, Running};
        use MemberDiskEffect::{Complete, Join, None, Reject, Start};
        use MemberDiskEvent::{BeginDrain, DiskDown, DiskUp, DrainCompleted, OnlineSettled};
        use MemberDiskState::{Da, Di, Removed, Ua, Ui};
        use MemberDiskWorkflowKind::{Offline, Online};

        let transition = match (state, event, activity) {
            // Same intent is idempotent: the Runtime joins the existing result.
            (
                state,
                DiskDown,
                Running(Offline) | Pending(Offline) | Cancelling { next: Offline, .. },
            ) => Self::same(state, DiskDown, Join),
            (
                state,
                DiskUp,
                Running(Online) | Pending(Online) | Cancelling { next: Online, .. },
            ) => Self::same(state, DiskUp, Join),

            // Opposite intent is reversible, but only after the old workflow
            // and its downstream calls have reached a stable boundary.
            (Ua, DiskDown, Running(Online) | Pending(Online) | Cancelling { next: Online, .. }) => {
                Self::replace(Ua, DiskDown, Da, Offline)
            }
            (Ui, DiskDown, Running(Online) | Pending(Online) | Cancelling { next: Online, .. }) => {
                Self::replace(Ui, DiskDown, Di, Offline)
            }
            (Da, DiskDown, Running(Online) | Pending(Online) | Cancelling { next: Online, .. }) => {
                Self::replace(Da, DiskDown, Da, Offline)
            }
            (Di, DiskDown, Running(Online) | Pending(Online) | Cancelling { next: Online, .. }) => {
                Self::replace(Di, DiskDown, Di, Offline)
            }
            (
                Removed,
                DiskDown,
                Running(Online) | Pending(Online) | Cancelling { next: Online, .. },
            ) => MemberDiskTransition::new(Removed, DiskDown, Removed, Complete),

            (
                Da,
                DiskUp,
                Running(Offline) | Pending(Offline) | Cancelling { next: Offline, .. },
            ) => Self::replace(Da, DiskUp, Ua, Online),
            (
                Di,
                DiskUp,
                Running(Offline) | Pending(Offline) | Cancelling { next: Offline, .. },
            ) => Self::replace(Di, DiskUp, Ui, Online),
            (
                Ua,
                DiskUp,
                Running(Offline) | Pending(Offline) | Cancelling { next: Offline, .. },
            ) => Self::replace(Ua, DiskUp, Ua, Online),
            (
                Ui,
                DiskUp,
                Running(Offline) | Pending(Offline) | Cancelling { next: Offline, .. },
            ) => Self::replace(Ui, DiskUp, Ui, Online),
            (
                Removed,
                DiskUp,
                Running(Offline) | Pending(Offline) | Cancelling { next: Offline, .. },
            ) => MemberDiskTransition::new(
                Removed,
                DiskUp,
                Removed,
                Reject("removed disk requires an explicit rejoin operation"),
            ),

            // External events admitted while the object has no active workflow.
            (Ua, DiskDown, Idle) => MemberDiskTransition::new(Ua, DiskDown, Da, Start(Offline)),
            (Da, DiskDown, Idle) => MemberDiskTransition::new(Da, DiskDown, Da, Start(Offline)),
            (Di, DiskDown, Idle) => MemberDiskTransition::new(Di, DiskDown, Di, Start(Offline)),
            (Ui, DiskDown, Idle) => MemberDiskTransition::new(Ui, DiskDown, Di, Start(Offline)),
            (Removed, DiskDown, Idle) => {
                MemberDiskTransition::new(Removed, DiskDown, Removed, Complete)
            }

            (Ua, DiskUp, Idle) => MemberDiskTransition::new(Ua, DiskUp, Ua, Complete),
            (Da, DiskUp, Idle) => MemberDiskTransition::new(Da, DiskUp, Ua, Start(Online)),
            (Di, DiskUp, Idle) => MemberDiskTransition::new(Di, DiskUp, Ui, Start(Online)),
            (Ui, DiskUp, Idle) => MemberDiskTransition::new(Ui, DiskUp, Ui, Start(Online)),
            (Removed, DiskUp, Idle) => MemberDiskTransition::new(
                Removed,
                DiskUp,
                Removed,
                Reject("removed disk requires an explicit rejoin operation"),
            ),

            // Internal workflow events. They advance metadata but do not ask
            // the Runtime to create another workflow.
            (Da, BeginDrain, Running(Offline)) => {
                MemberDiskTransition::new(Da, BeginDrain, Di, None)
            }
            (Di, BeginDrain, Running(Offline)) => Self::same(Di, BeginDrain, None),
            (Di, DrainCompleted, Running(Offline)) => {
                MemberDiskTransition::new(Di, DrainCompleted, Removed, None)
            }
            (Ui, OnlineSettled, Running(Online)) => {
                MemberDiskTransition::new(Ui, OnlineSettled, Ua, None)
            }
            (Ua, OnlineSettled, Running(Online)) => Self::same(Ua, OnlineSettled, None),

            _ => {
                return Err(RuntimeError::InvalidState(format!(
                    "no disk transition for state={state:?}, event={event:?}, activity={activity:?}"
                )))
            }
        };
        Ok(transition)
    }

    fn same(
        state: MemberDiskState,
        event: MemberDiskEvent,
        effect: MemberDiskEffect,
    ) -> MemberDiskTransition {
        MemberDiskTransition::new(state, event, state, effect)
    }

    fn replace(
        from: MemberDiskState,
        event: MemberDiskEvent,
        to: MemberDiskState,
        next: MemberDiskWorkflowKind,
    ) -> MemberDiskTransition {
        let cause = match next {
            MemberDiskWorkflowKind::Offline => "disk went down while online intent was active",
            MemberDiskWorkflowKind::Online => {
                "disk recovered; stop draining after a stable boundary"
            }
        };
        MemberDiskTransition::new(
            from,
            event,
            to,
            MemberDiskEffect::Replace {
                next,
                mode: CancelMode::SettleThenStop,
                cause,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_replaces_offline_after_a_stable_boundary() {
        let transition = MemberDiskMachine::transition(
            MemberDiskState::Di,
            MemberDiskEvent::DiskUp,
            MemberDiskActivity::Running(MemberDiskWorkflowKind::Offline),
        )
        .unwrap();
        assert_eq!(transition.to, MemberDiskState::Ui);
        assert!(matches!(
            transition.effect,
            MemberDiskEffect::Replace {
                next: MemberDiskWorkflowKind::Online,
                mode: CancelMode::SettleThenStop,
                ..
            }
        ));
    }

    #[test]
    fn duplicate_intent_joins_without_changing_state() {
        let transition = MemberDiskMachine::transition(
            MemberDiskState::Di,
            MemberDiskEvent::DiskDown,
            MemberDiskActivity::Running(MemberDiskWorkflowKind::Offline),
        )
        .unwrap();
        assert_eq!(transition.to, MemberDiskState::Di);
        assert_eq!(transition.effect, MemberDiskEffect::Join);
    }

    #[test]
    fn illegal_internal_event_is_rejected() {
        assert!(MemberDiskMachine::transition(
            MemberDiskState::Ua,
            MemberDiskEvent::DrainCompleted,
            MemberDiskActivity::Running(MemberDiskWorkflowKind::Offline),
        )
        .is_err());
    }
}
