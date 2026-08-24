use tokio::sync::watch;

use crate::{RuntimeError, TaskContext};

use super::super::{BgId, DemoCatalog, DemoError, DemoMessage, DiskId, DoBgRebuild, ServiceKind};

pub async fn resolve_disk(catalog: DemoCatalog, disk: DiskId) -> Result<Vec<BgId>, RuntimeError> {
    Ok(catalog.resolve(&disk).await)
}

pub async fn rebuild_bg(
    context: TaskContext<ServiceKind, DemoMessage>,
    bg: BgId,
) -> Result<(), RuntimeError> {
    let (completed, ticket) = crate::request_channel();
    context
        .send(ServiceKind::Bg, DoBgRebuild { bg, completed })
        .await?;
    match ticket.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(RuntimeError::TaskFailed(error.to_string())),
        Err(_) => Err(RuntimeError::ChannelClosed("BG completion".into())),
    }
}

pub async fn wait_campaign(mut stop: watch::Receiver<bool>) -> Result<(), RuntimeError> {
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        stop.changed()
            .await
            .map_err(|_| RuntimeError::ChannelClosed("campaign stop".into()))?;
    }
}

pub async fn cancel_bg(
    context: TaskContext<ServiceKind, DemoMessage>,
    bg: BgId,
) -> Result<(), RuntimeError> {
    context
        .send(ServiceKind::Bg, super::super::CancelBgRebuild { bg })
        .await
}

pub fn runtime_error(error: &RuntimeError) -> DemoError {
    DemoError::Runtime(error.to_string())
}
