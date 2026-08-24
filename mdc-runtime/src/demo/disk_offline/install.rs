use crate::{RuntimeError, register_handlers};

use super::super::{DiskOfflineRequest, OfflineDispatched, blueprint::DemoBlueprint};
use super::handlers::{handle_disk_offline, handle_offline_dispatched};

/// The only mapping source for the disk-offline feature.
pub(crate) fn install(blueprint: &mut DemoBlueprint) -> Result<(), RuntimeError> {
    register_handlers!(blueprint.event, {
        DiskOfflineRequest => handle_disk_offline,
        OfflineDispatched => handle_offline_dispatched
    })?;
    blueprint
        .event
        .on_activity(|state, _| state.activity_changes += 1);
    Ok(())
}
