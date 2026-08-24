use std::collections::HashSet;

use crate::{
    CancelMode, CancelReason, RunOutcome, RuntimeError, ServiceContext, TaskKey, TaskOutcome,
    TaskVisibility,
};

use super::super::{
    BgFinished, BgId, CampaignFinished, CancelBgSent, CancelRebuildRequest, DemoError, DemoMessage,
    DiskId, DiskResolved, QueryRebuild, RebuildSnapshot, RebuildStatus, ResumeRebuildRequest,
    ServiceKind, StartRebuildRequest, SuspendRebuildRequest,
    state::{BgJob, BgPhase, DiskJob, DiskPhase, RebuildState},
};
use super::workflow;

type RebuildService = ServiceContext<ServiceKind, DemoMessage, RebuildState>;

pub fn handle_start(
    service: &mut RebuildService,
    request: StartRebuildRequest,
) -> Result<(), RuntimeError> {
    if service.state().disks.contains_key(&request.disk) {
        let _ = request
            .completed
            .send(Err(DemoError::AlreadyRebuilding(request.disk)));
        return Ok(());
    }

    let disk = request.disk;
    service.state_mut().disks.insert(
        disk.clone(),
        DiskJob {
            phase: DiskPhase::Resolving,
            pending: HashSet::new(),
            completed: Some(request.completed),
        },
    );
    ensure_campaign(service);

    let catalog = service.state().catalog.clone();
    let output_disk = disk.clone();
    service.run(
        resolve_key(&disk),
        format!("resolve disk to BGs: {disk}"),
        TaskVisibility::Internal,
        move |_| workflow::resolve_disk(catalog, disk),
        move |outcome| {
            DemoMessage::DiskResolved(DiskResolved {
                disk: output_disk,
                outcome,
            })
        },
    );
    Ok(())
}

pub fn handle_disk_resolved(
    service: &mut RebuildService,
    event: DiskResolved,
) -> Result<(), RuntimeError> {
    let disk = event.disk;
    if !service.state().disks.contains_key(&disk) {
        return Ok(());
    }

    match event.outcome {
        TaskOutcome::Completed(items) => {
            let bgs: HashSet<_> = items.into_iter().collect();
            if bgs.is_empty() {
                complete_disk(service, &disk, Ok(RebuildStatus::Completed));
                finish_campaign_if_idle(service);
                return Ok(());
            }
            {
                let state = service.state_mut();
                let job = state.disks.get_mut(&disk).expect("checked above");
                job.phase = DiskPhase::Active;
                job.pending = bgs.clone();
                for bg in bgs {
                    if let Some(existing) = state.bgs.get_mut(&bg) {
                        existing.owners.insert(disk.clone());
                    } else {
                        state.bgs.insert(
                            bg.clone(),
                            BgJob {
                                phase: BgPhase::Queued,
                                owners: HashSet::from([disk.clone()]),
                            },
                        );
                        state.queue.push_back(bg);
                    }
                }
            }
            refill(service);
        }
        TaskOutcome::Failed(error) => {
            complete_disk(service, &disk, Err(workflow::runtime_error(&error)));
            finish_campaign_if_idle(service);
        }
        TaskOutcome::Cancelled(_) => {
            complete_disk(service, &disk, Err(DemoError::Cancelled));
            finish_campaign_if_idle(service);
        }
    }
    Ok(())
}

pub fn handle_bg_finished(
    service: &mut RebuildService,
    event: BgFinished,
) -> Result<(), RuntimeError> {
    let Some(bg_job) = service.state_mut().bgs.remove(&event.bg) else {
        return Ok(());
    };
    service.state_mut().running = service.state().running.saturating_sub(1);

    let result = match event.outcome {
        TaskOutcome::Completed(()) => Ok(RebuildStatus::Completed),
        TaskOutcome::Failed(error) => Err(workflow::runtime_error(&error)),
        TaskOutcome::Cancelled(_) => Err(DemoError::Cancelled),
    };
    for disk in bg_job.owners {
        let completed = {
            let Some(job) = service.state_mut().disks.get_mut(&disk) else {
                continue;
            };
            job.pending.remove(&event.bg);
            job.pending.is_empty() || result.is_err()
        };
        if completed {
            complete_disk(service, &disk, result.clone());
            detach_disk(service, &disk);
        }
    }

    if service.state().suspended
        && service.state().running == 0
        && let Some(reply) = service.state_mut().suspend_reply.take()
    {
        let _ = reply.send(Ok(()));
    }
    refill(service);
    finish_campaign_if_idle(service);
    Ok(())
}

pub fn handle_suspend(
    service: &mut RebuildService,
    request: SuspendRebuildRequest,
) -> Result<(), RuntimeError> {
    service.state_mut().suspended = true;
    if service.state().running == 0 {
        let _ = request.completed.send(Ok(()));
    } else if service.state().suspend_reply.is_some() {
        let _ = request
            .completed
            .send(Err(DemoError::Runtime("suspend already pending".into())));
    } else {
        service.state_mut().suspend_reply = Some(request.completed);
    }
    Ok(())
}

pub fn handle_resume(
    service: &mut RebuildService,
    request: ResumeRebuildRequest,
) -> Result<(), RuntimeError> {
    service.state_mut().suspended = false;
    if let Some(waiting) = service.state_mut().suspend_reply.take() {
        let _ = waiting.send(Err(DemoError::Runtime(
            "resumed before suspend settled".into(),
        )));
    }
    let _ = request.completed.send(Ok(()));
    refill(service);
    Ok(())
}

pub fn handle_cancel(
    service: &mut RebuildService,
    request: CancelRebuildRequest,
) -> Result<(), RuntimeError> {
    let disk = request.disk;
    let Some(mut job) = service.state_mut().disks.remove(&disk) else {
        let _ = request.completed.send(Err(DemoError::NotRebuilding(disk)));
        return Ok(());
    };
    if let Some(completed) = job.completed.take() {
        let _ = completed.send(Err(DemoError::Cancelled));
    }
    if job.phase == DiskPhase::Resolving {
        service.cancel_task(
            &resolve_key(&disk),
            CancelMode::Force,
            CancelReason::NoLongerNeeded,
        );
    }
    detach_disk(service, &disk);
    let _ = request.completed.send(Ok(()));
    refill(service);
    finish_campaign_if_idle(service);
    Ok(())
}

pub fn handle_campaign_finished(
    service: &mut RebuildService,
    _event: CampaignFinished,
) -> Result<(), RuntimeError> {
    service.state_mut().campaign_stop = None;
    if !service.state().disks.is_empty() || !service.state().bgs.is_empty() {
        ensure_campaign(service);
    }
    Ok(())
}

pub fn handle_cancel_bg_sent(
    _service: &mut RebuildService,
    _event: CancelBgSent,
) -> Result<(), RuntimeError> {
    Ok(())
}

pub fn handle_query(service: &mut RebuildService, query: QueryRebuild) -> Result<(), RuntimeError> {
    let state = service.state();
    let _ = query.completed.send(RebuildSnapshot {
        suspended: state.suspended,
        resolving_disks: state
            .disks
            .values()
            .filter(|job| job.phase == DiskPhase::Resolving)
            .count(),
        active_disks: state
            .disks
            .values()
            .filter(|job| job.phase == DiskPhase::Active)
            .count(),
        queued_bgs: state
            .bgs
            .values()
            .filter(|job| job.phase == BgPhase::Queued)
            .count(),
        running_bgs: state.running,
        campaigns_started: state.campaigns_started,
        bg_delegations_started: state.bg_delegations_started,
    });
    Ok(())
}

fn ensure_campaign(service: &mut RebuildService) {
    if service.state().campaign_stop.is_some() {
        return;
    }
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    service.state_mut().campaign_stop = Some(stop_tx);
    let outcome = service.run(
        TaskKey::new("rebuild/campaign"),
        "pool rebuild campaign",
        TaskVisibility::Public,
        move |_| workflow::wait_campaign(stop_rx),
        move |outcome| DemoMessage::CampaignFinished(CampaignFinished { outcome }),
    );
    if matches!(outcome, RunOutcome::Started(_)) {
        service.state_mut().campaigns_started += 1;
    }
}

fn refill(service: &mut RebuildService) {
    loop {
        let next = {
            let state = service.state_mut();
            if state.suspended || state.running >= state.window {
                None
            } else {
                let bg = state.queue.pop_front();
                if let Some(bg) = &bg {
                    if let Some(job) = state.bgs.get_mut(bg) {
                        job.phase = BgPhase::Running;
                    }
                    state.running += 1;
                    state.bg_delegations_started += 1;
                }
                bg
            }
        };
        let Some(bg) = next else {
            break;
        };
        let output_bg = bg.clone();
        service.run(
            bg_key(&bg),
            format!("delegate BG rebuild: {bg}"),
            TaskVisibility::Internal,
            move |context| workflow::rebuild_bg(context, bg),
            move |outcome| {
                DemoMessage::BgFinished(BgFinished {
                    bg: output_bg,
                    outcome,
                })
            },
        );
    }
}

fn complete_disk(
    service: &mut RebuildService,
    disk: &DiskId,
    result: Result<RebuildStatus, DemoError>,
) {
    if let Some(mut job) = service.state_mut().disks.remove(disk)
        && let Some(completed) = job.completed.take()
    {
        let _ = completed.send(result);
    }
}

fn detach_disk(service: &mut RebuildService, disk: &DiskId) {
    let mut cancel = Vec::new();
    let mut remove = Vec::new();
    for (bg, job) in &mut service.state_mut().bgs {
        job.owners.remove(disk);
        if job.owners.is_empty() {
            match job.phase {
                BgPhase::Queued => remove.push(bg.clone()),
                BgPhase::Running => cancel.push(bg.clone()),
            }
        }
    }
    if !remove.is_empty() {
        let removed: HashSet<_> = remove.iter().cloned().collect();
        service.state_mut().queue.retain(|bg| !removed.contains(bg));
        for bg in remove {
            service.state_mut().bgs.remove(&bg);
        }
    }
    for bg in cancel {
        send_cancel_bg(service, bg);
    }
}

fn send_cancel_bg(service: &mut RebuildService, bg: BgId) {
    let output_bg = bg.clone();
    service.run(
        TaskKey::new(format!("cancel-bg/{bg}")),
        format!("cancel orphan BG: {bg}"),
        TaskVisibility::Internal,
        move |context| workflow::cancel_bg(context, bg),
        move |outcome| {
            DemoMessage::CancelBgSent(CancelBgSent {
                bg: output_bg,
                outcome,
            })
        },
    );
}

fn finish_campaign_if_idle(service: &mut RebuildService) {
    if service.state().disks.is_empty()
        && service.state().bgs.is_empty()
        && let Some(stop) = &service.state().campaign_stop
    {
        let _ = stop.send(true);
    }
}

fn resolve_key(disk: &DiskId) -> TaskKey {
    TaskKey::new(format!("resolve/{disk}"))
}

fn bg_key(bg: &BgId) -> TaskKey {
    TaskKey::new(format!("bg/{bg}"))
}
