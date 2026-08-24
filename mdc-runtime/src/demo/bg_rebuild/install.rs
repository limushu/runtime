use crate::{RuntimeError, register_handlers};

use super::super::{
    BgWorkflowFinished, CancelBgRebuild, DoBgRebuild, QueryBg, blueprint::DemoBlueprint,
};
use super::handlers::*;

/// The complete BgService message map.
pub(crate) fn install(blueprint: &mut DemoBlueprint) -> Result<(), RuntimeError> {
    register_handlers!(blueprint.bg, {
        DoBgRebuild => handle_rebuild,
        CancelBgRebuild => handle_cancel,
        BgWorkflowFinished => handle_finished,
        QueryBg => handle_query
    })?;
    blueprint
        .bg
        .on_activity(|state, _| state.activity_changes += 1);
    Ok(())
}
