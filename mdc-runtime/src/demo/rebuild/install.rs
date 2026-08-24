use crate::{RuntimeError, register_handlers};

use super::super::{
    BgFinished, CampaignFinished, CancelBgSent, CancelRebuildRequest, DiskResolved, QueryRebuild,
    ResumeRebuildRequest, StartRebuildRequest, SuspendRebuildRequest, blueprint::DemoBlueprint,
};
use super::handlers::*;

/// The complete RebuildService message map. There is no second `match` in the service.
pub(crate) fn install(blueprint: &mut DemoBlueprint) -> Result<(), RuntimeError> {
    register_handlers!(blueprint.rebuild, {
        StartRebuildRequest => handle_start,
        SuspendRebuildRequest => handle_suspend,
        ResumeRebuildRequest => handle_resume,
        CancelRebuildRequest => handle_cancel,
        DiskResolved => handle_disk_resolved,
        BgFinished => handle_bg_finished,
        CampaignFinished => handle_campaign_finished,
        CancelBgSent => handle_cancel_bg_sent,
        QueryRebuild => handle_query
    })?;
    blueprint
        .rebuild
        .on_activity(|state, _| state.activity_changes += 1);
    Ok(())
}
