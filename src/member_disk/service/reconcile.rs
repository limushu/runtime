use super::{MemberDiskService, MemberDiskServiceError};
use crate::member_disk::{DiskUuid, MemberDiskEvent, MemberDiskState, PhysicalState};
use tokio_util::sync::CancellationToken;

/// Whether one explicit state transition completed or the active event has
/// already reached a stable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReconcileResult {
    Transitioned { action: &'static str },
    Stable,
}

/// The complete read-only state used to select one MemberDisk transition.
///
/// Both fields come from MemberDisk metadata. Cross-domain conditions such as
/// BG references are read only by transitions that need them, so an unrelated
/// VDM outage cannot block the mandatory DOWN transition. This value is never
/// persisted as a second state-machine copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ReconcileState {
    member: MemberDiskState,
    shrinking: bool,
}

impl MemberDiskService {
    /// Selects and executes exactly one transition. The runtime calls this
    /// again after `Transitioned`, using freshly read authoritative state.
    pub(super) async fn reconcile_once(
        &self,
        event: &MemberDiskEvent,
        operation_cancel: &CancellationToken,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        let disk = event.disk();
        let start = self.reconcile_state(disk).await?;

        match start.member {
            MemberDiskState::UpActive => self.reconcile_up_active(disk, event, start).await,
            MemberDiskState::UpInactive => {
                self.reconcile_up_inactive(disk, event, start, operation_cancel)
                    .await
            }
            MemberDiskState::DownActive => {
                self.reconcile_down_active(disk, event, start, operation_cancel)
                    .await
            }
            MemberDiskState::DownInactive => {
                self.reconcile_down_inactive(disk, event, start, operation_cancel)
                    .await
            }
            MemberDiskState::Removed => {
                self.reconcile_removed(disk, event, start, operation_cancel)
                    .await
            }
        }
    }

    #[allow(clippy::match_same_arms)]
    async fn reconcile_up_active(
        &self,
        disk: &DiskUuid,
        event: &MemberDiskEvent,
        start: ReconcileState,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        use MemberDiskState::{DownActive, UpInactive};
        use ReconcileResult::Stable;

        match start.shrinking {
            false => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => {
                    self.set_disk_down(disk).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "set disk DOWN",
                        ReconcileState {
                            member: DownActive,
                            ..start
                        },
                    )
                    .await
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => Ok(Stable),
                MemberDiskEvent::Shrink { .. } => {
                    self.request_shrink(disk).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "record Shrink intent",
                        ReconcileState {
                            shrinking: true,
                            ..start
                        },
                    )
                    .await
                }
            },
            true => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => {
                    // DOWN is a mandatory safety transition during Shrink.
                    self.set_disk_down(disk).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "set disk DOWN",
                        ReconcileState {
                            member: DownActive,
                            ..start
                        },
                    )
                    .await
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                }
                | MemberDiskEvent::Shrink { .. } => {
                    self.disable_allocation(disk).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "disable allocation",
                        ReconcileState {
                            member: UpInactive,
                            ..start
                        },
                    )
                    .await
                }
            },
        }
    }

    #[allow(clippy::match_same_arms)]
    async fn reconcile_up_inactive(
        &self,
        disk: &DiskUuid,
        event: &MemberDiskEvent,
        start: ReconcileState,
        operation_cancel: &CancellationToken,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        use MemberDiskState::{DownInactive, UpActive};

        match start.shrinking {
            false => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => {
                    self.set_disk_down(disk).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "set disk DOWN",
                        ReconcileState {
                            member: DownInactive,
                            ..start
                        },
                    )
                    .await
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => {
                    self.open_and_serve(disk, operation_cancel).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "open and publish disk UP",
                        ReconcileState {
                            member: UpActive,
                            ..start
                        },
                    )
                    .await
                }
                MemberDiskEvent::Shrink { .. } => {
                    self.request_shrink(disk).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "record Shrink intent",
                        ReconcileState {
                            shrinking: true,
                            ..start
                        },
                    )
                    .await
                }
            },
            true => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => {
                    self.set_disk_down(disk).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "set disk DOWN",
                        ReconcileState {
                            member: DownInactive,
                            ..start
                        },
                    )
                    .await
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                }
                | MemberDiskEvent::Shrink { .. } => {
                    self.continue_shrink_while_up(disk, event, start, operation_cancel)
                        .await
                }
            },
        }
    }

    async fn continue_shrink_while_up(
        &self,
        disk: &DiskUuid,
        event: &MemberDiskEvent,
        start: ReconcileState,
        operation_cancel: &CancellationToken,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        use MemberDiskState::DownInactive;

        if self.virtual_disks.has_references(disk).await? {
            self.evacuate(disk, operation_cancel).await?;
            return self.finish_evacuation_transition(disk, start, event).await;
        }

        self.set_disk_down(disk).await?;
        self.finish_transition(
            disk,
            start,
            event,
            "set disk DOWN",
            ReconcileState {
                member: DownInactive,
                ..start
            },
        )
        .await
    }

    #[allow(clippy::match_same_arms)]
    async fn reconcile_down_active(
        &self,
        disk: &DiskUuid,
        event: &MemberDiskEvent,
        start: ReconcileState,
        operation_cancel: &CancellationToken,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        use MemberDiskState::{DownInactive, UpActive};

        match start.shrinking {
            false => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    observed_at,
                    ..
                } => {
                    self.wait_then_disable(disk, *observed_at, operation_cancel)
                        .await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "wait recovery window and disable allocation",
                        ReconcileState {
                            member: DownInactive,
                            ..start
                        },
                    )
                    .await
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => {
                    self.open_and_serve(disk, operation_cancel).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "open and publish disk UP",
                        ReconcileState {
                            member: UpActive,
                            ..start
                        },
                    )
                    .await
                }
                MemberDiskEvent::Shrink { .. } => {
                    self.request_shrink(disk).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "record Shrink intent",
                        ReconcileState {
                            shrinking: true,
                            ..start
                        },
                    )
                    .await
                }
            },
            true => match event {
                MemberDiskEvent::PhysicalChanged { .. } | MemberDiskEvent::Shrink { .. } => {
                    self.disable_allocation(disk).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "disable allocation",
                        ReconcileState {
                            member: DownInactive,
                            ..start
                        },
                    )
                    .await
                }
            },
        }
    }

    #[allow(clippy::match_same_arms)]
    async fn reconcile_down_inactive(
        &self,
        disk: &DiskUuid,
        event: &MemberDiskEvent,
        start: ReconcileState,
        operation_cancel: &CancellationToken,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        use MemberDiskState::UpActive;

        match start.shrinking {
            false => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => {
                    self.continue_removal_while_down(disk, event, start, operation_cancel)
                        .await
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => {
                    self.open_and_serve(disk, operation_cancel).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "open and publish disk UP",
                        ReconcileState {
                            member: UpActive,
                            ..start
                        },
                    )
                    .await
                }
                MemberDiskEvent::Shrink { .. } => {
                    self.request_shrink(disk).await?;
                    self.finish_transition(
                        disk,
                        start,
                        event,
                        "record Shrink intent",
                        ReconcileState {
                            shrinking: true,
                            ..start
                        },
                    )
                    .await
                }
            },
            true => match event {
                MemberDiskEvent::PhysicalChanged { .. } | MemberDiskEvent::Shrink { .. } => {
                    self.continue_removal_while_down(disk, event, start, operation_cancel)
                        .await
                }
            },
        }
    }

    async fn continue_removal_while_down(
        &self,
        disk: &DiskUuid,
        event: &MemberDiskEvent,
        start: ReconcileState,
        operation_cancel: &CancellationToken,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        use MemberDiskState::Removed;

        if self.virtual_disks.has_references(disk).await? {
            self.evacuate(disk, operation_cancel).await?;
            return self.finish_evacuation_transition(disk, start, event).await;
        }

        self.remove(disk).await?;
        self.finish_transition(
            disk,
            start,
            event,
            "remove MemberDisk",
            ReconcileState {
                member: Removed,
                ..start
            },
        )
        .await
    }

    #[allow(clippy::match_same_arms)]
    async fn reconcile_removed(
        &self,
        disk: &DiskUuid,
        event: &MemberDiskEvent,
        start: ReconcileState,
        operation_cancel: &CancellationToken,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        use MemberDiskState::UpActive;
        use ReconcileResult::Stable;

        match event {
            MemberDiskEvent::PhysicalChanged {
                state: PhysicalState::Down,
                ..
            }
            | MemberDiskEvent::Shrink { .. } => Ok(Stable),
            MemberDiskEvent::PhysicalChanged {
                state: PhysicalState::Up,
                ..
            } => {
                self.open_and_serve(disk, operation_cancel).await?;
                self.finish_transition(
                    disk,
                    start,
                    event,
                    "rejoin, open and publish disk UP",
                    ReconcileState {
                        member: UpActive,
                        shrinking: false,
                    },
                )
                .await
            }
        }
    }

    pub(super) async fn reconcile_state(
        &self,
        disk: &DiskUuid,
    ) -> Result<ReconcileState, MemberDiskServiceError> {
        let member = self.get_member(disk).await?;

        Ok(ReconcileState {
            member: member.state(),
            shrinking: member.shrink_requested(),
        })
    }

    /// Verifies the cross-domain finish state of the directly awaited VDM
    /// evacuation without making VDM part of unrelated transitions.
    async fn finish_evacuation_transition(
        &self,
        disk: &DiskUuid,
        start: ReconcileState,
        event: &MemberDiskEvent,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        let actual = self.reconcile_state(disk).await?;
        let has_bg_references = self.virtual_disks.has_references(disk).await?;

        if actual != start || has_bg_references {
            return Err(MemberDiskServiceError::TransitionIncomplete(format!(
                "MemberDisk {disk}: {start:?} --{event:?} / evacuate BG references--> expected {start:?} with no VDM references, got {actual:?} with has_bg_references={has_bg_references}"
            )));
        }

        Ok(ReconcileResult::Transitioned {
            action: "evacuate BG references",
        })
    }

    /// Verifies the declared finish state after the branch directly awaits its
    /// business action.
    async fn finish_transition(
        &self,
        disk: &DiskUuid,
        start: ReconcileState,
        event: &MemberDiskEvent,
        action_name: &'static str,
        finish: ReconcileState,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        let actual = self.reconcile_state(disk).await?;
        if actual != finish {
            return Err(MemberDiskServiceError::TransitionIncomplete(format!(
                "MemberDisk {disk}: {start:?} --{event:?} / {action_name}--> expected {finish:?}, got {actual:?}"
            )));
        }

        Ok(ReconcileResult::Transitioned {
            action: action_name,
        })
    }
}
