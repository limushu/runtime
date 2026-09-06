use super::{MemberDiskService, MemberDiskServiceError};
use crate::member_disk::{DiskUuid, MemberDiskEvent, MemberDiskState, PhysicalState};
use tokio_util::sync::CancellationToken;

/// Whether the active event advanced the object or reached its target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReconcileResult {
    Progressed,
    Stable,
}

impl MemberDiskService {
    /// Routes the active event to the complete rule set for the current state.
    pub(super) async fn reconcile_once(
        &self,
        event: &MemberDiskEvent,
        operation_cancel: &CancellationToken,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        let disk = event.disk();
        let member = self.get_member(disk).await?;
        let shrink = member.shrink_requested();

        match member.state() {
            MemberDiskState::UpActive => self.reconcile_up_active(disk, event, shrink).await,
            MemberDiskState::UpInactive => {
                self.reconcile_up_inactive(disk, event, shrink, operation_cancel)
                    .await
            }
            MemberDiskState::DownActive => {
                self.reconcile_down_active(disk, event, shrink, operation_cancel)
                    .await
            }
            MemberDiskState::DownInactive => {
                self.reconcile_down_inactive(disk, event, shrink, operation_cancel)
                    .await
            }
            MemberDiskState::Removed => {
                self.reconcile_removed(disk, event, shrink, operation_cancel)
                    .await
            }
        }
    }

    #[allow(clippy::match_same_arms)]
    async fn reconcile_up_active(
        &self,
        disk: &DiskUuid,
        event: &MemberDiskEvent,
        shrink: bool,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        use ReconcileResult::{Progressed, Stable};

        match shrink {
            false => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => {
                    self.set_disk_down(disk).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => Ok(Stable),
                MemberDiskEvent::Shrink { .. } => {
                    self.request_shrink(disk).await?;
                    Ok(Progressed)
                }
            },
            true => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => {
                    // DOWN remains a mandatory safety boundary during Shrink.
                    self.set_disk_down(disk).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => {
                    self.disable_allocation(disk).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::Shrink { .. } => {
                    self.disable_allocation(disk).await?;
                    Ok(Progressed)
                }
            },
        }
    }

    #[allow(clippy::match_same_arms)]
    async fn reconcile_up_inactive(
        &self,
        disk: &DiskUuid,
        event: &MemberDiskEvent,
        shrink: bool,
        operation_cancel: &CancellationToken,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        use ReconcileResult::Progressed;

        match shrink {
            false => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => {
                    self.set_disk_down(disk).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => {
                    self.open_and_serve(disk, operation_cancel).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::Shrink { .. } => {
                    self.request_shrink(disk).await?;
                    Ok(Progressed)
                }
            },
            true => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => {
                    // Stop IO before continuing the planned removal.
                    self.set_disk_down(disk).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => {
                    self.evacuate_online_disk(disk, operation_cancel).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::Shrink { .. } => {
                    self.evacuate_online_disk(disk, operation_cancel).await?;
                    Ok(Progressed)
                }
            },
        }
    }

    #[allow(clippy::match_same_arms)]
    async fn reconcile_down_active(
        &self,
        disk: &DiskUuid,
        event: &MemberDiskEvent,
        shrink: bool,
        operation_cancel: &CancellationToken,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        use ReconcileResult::Progressed;

        match shrink {
            false => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    observed_at,
                    ..
                } => {
                    self.wait_then_disable(disk, *observed_at, operation_cancel)
                        .await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => {
                    self.open_and_serve(disk, operation_cancel).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::Shrink { .. } => {
                    self.request_shrink(disk).await?;
                    Ok(Progressed)
                }
            },
            true => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => {
                    self.disable_allocation(disk).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => {
                    self.disable_allocation(disk).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::Shrink { .. } => {
                    self.disable_allocation(disk).await?;
                    Ok(Progressed)
                }
            },
        }
    }

    #[allow(clippy::match_same_arms)]
    async fn reconcile_down_inactive(
        &self,
        disk: &DiskUuid,
        event: &MemberDiskEvent,
        shrink: bool,
        operation_cancel: &CancellationToken,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        use ReconcileResult::Progressed;

        match shrink {
            false => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => {
                    self.evacuate_down_disk(disk, operation_cancel).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => {
                    self.open_and_serve(disk, operation_cancel).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::Shrink { .. } => {
                    self.request_shrink(disk).await?;
                    Ok(Progressed)
                }
            },
            true => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => {
                    self.evacuate_down_disk(disk, operation_cancel).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => {
                    self.evacuate_down_disk(disk, operation_cancel).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::Shrink { .. } => {
                    self.evacuate_down_disk(disk, operation_cancel).await?;
                    Ok(Progressed)
                }
            },
        }
    }

    #[allow(clippy::match_same_arms)]
    async fn reconcile_removed(
        &self,
        disk: &DiskUuid,
        event: &MemberDiskEvent,
        shrink: bool,
        operation_cancel: &CancellationToken,
    ) -> Result<ReconcileResult, MemberDiskServiceError> {
        use ReconcileResult::{Progressed, Stable};

        match shrink {
            false => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => Ok(Stable),
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => {
                    self.open_and_serve(disk, operation_cancel).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::Shrink { .. } => Ok(Stable),
            },
            true => match event {
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Down,
                    ..
                } => Ok(Stable),
                MemberDiskEvent::PhysicalChanged {
                    state: PhysicalState::Up,
                    ..
                } => {
                    // UP after terminal removal is an explicit Rejoin and
                    // clears the persisted Shrink intent on commit.
                    self.open_and_serve(disk, operation_cancel).await?;
                    Ok(Progressed)
                }
                MemberDiskEvent::Shrink { .. } => Ok(Stable),
            },
        }
    }
}
