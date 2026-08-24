use crate::{
    CancelMode, CancelReason, RunOutcome, RuntimeError, ServiceContext, TaskKey, TaskOutcome,
    TaskVisibility,
};

use super::super::{
    BgWorkflowFinished, CancelBgRebuild, DemoError, DemoMessage, DoBgRebuild, QueryBg, ServiceKind,
    state::BgState,
};
use super::workflow;

type BgService = ServiceContext<ServiceKind, DemoMessage, BgState>;

pub fn handle_rebuild(service: &mut BgService, request: DoBgRebuild) -> Result<(), RuntimeError> {
    let bg = request.bg;
    let key = bg_key(&bg);
    if service.task_is_running(&key) {
        let _ = request.completed.send(Err(DemoError::Runtime(format!(
            "BG already rebuilding: {bg}"
        ))));
        return Ok(());
    }
    let backend = service.state().backend.clone();
    let output_bg = bg.clone();
    let completed = request.completed;
    let outcome = service.run(
        key,
        format!("rebuild BG: {bg}"),
        TaskVisibility::Internal,
        move |_| workflow::rebuild(backend, bg),
        move |outcome| {
            DemoMessage::BgWorkflowFinished(BgWorkflowFinished {
                bg: output_bg,
                completed,
                outcome,
            })
        },
    );
    debug_assert!(matches!(outcome, RunOutcome::Started(_)));
    Ok(())
}

pub fn handle_cancel(
    service: &mut BgService,
    request: CancelBgRebuild,
) -> Result<(), RuntimeError> {
    service.cancel_task(
        &bg_key(&request.bg),
        CancelMode::Force,
        CancelReason::NoLongerNeeded,
    );
    Ok(())
}

pub fn handle_finished(
    service: &mut BgService,
    event: BgWorkflowFinished,
) -> Result<(), RuntimeError> {
    let result = match event.outcome {
        TaskOutcome::Completed(()) => Ok(()),
        TaskOutcome::Failed(error) => Err(DemoError::Runtime(error.to_string())),
        TaskOutcome::Cancelled(_) => {
            service.state().backend.record_cancelled(event.bg);
            Err(DemoError::Cancelled)
        }
    };
    let _ = event.completed.send(result);
    Ok(())
}

pub fn handle_query(service: &mut BgService, query: QueryBg) -> Result<(), RuntimeError> {
    let _ = query.completed.send(service.state().backend.snapshot());
    Ok(())
}

fn bg_key(bg: &super::super::BgId) -> TaskKey {
    TaskKey::new(format!("bg-workflow/{bg}"))
}
