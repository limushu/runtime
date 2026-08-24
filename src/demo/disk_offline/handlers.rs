use crate::{RunOutcome, RuntimeError, ServiceContext, TaskKey, TaskVisibility};

use super::super::{
    DemoMessage, DiskOfflineRequest, OfflineDispatched, ServiceKind, state::EventState,
};
use super::workflow;

pub fn handle_disk_offline(
    service: &mut ServiceContext<ServiceKind, DemoMessage, EventState>,
    request: DiskOfflineRequest,
) -> Result<(), RuntimeError> {
    let task_key = TaskKey::new(format!("dispatch/disk-offline/{}", request.disk));
    if service.task_is_running(&task_key) {
        let _ = request
            .completed
            .send(Err(super::super::DemoError::AlreadyRebuilding(
                request.disk,
            )));
        return Ok(());
    }
    let disk = request.disk;
    let output_disk = disk.clone();
    let completed = request.completed;
    let outcome = service.run(
        task_key,
        format!("dispatch disk offline: {disk}"),
        TaskVisibility::Internal,
        move |context| workflow::dispatch_start(context, disk),
        move |outcome| {
            DemoMessage::OfflineDispatched(OfflineDispatched {
                disk: output_disk,
                completed,
                outcome,
            })
        },
    );
    debug_assert!(matches!(outcome, RunOutcome::Started(_)));
    Ok(())
}

pub fn handle_offline_dispatched(
    _service: &mut ServiceContext<ServiceKind, DemoMessage, EventState>,
    event: OfflineDispatched,
) -> Result<(), RuntimeError> {
    let result = match event.outcome {
        crate::TaskOutcome::Completed(result) => result,
        crate::TaskOutcome::Failed(error) => {
            Err(super::super::DemoError::Runtime(error.to_string()))
        }
        crate::TaskOutcome::Cancelled(_) => Err(super::super::DemoError::Cancelled),
    };
    let _ = event.completed.send(result);
    Ok(())
}
