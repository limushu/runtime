use crate::{RuntimeError, TaskContext};

use super::super::{DemoMessage, DiskId, RebuildResult, ServiceKind, StartRebuildRequest};

pub async fn dispatch_start(
    context: TaskContext<ServiceKind, DemoMessage>,
    disk: DiskId,
) -> Result<RebuildResult, RuntimeError> {
    let (completed, ticket) = crate::request_channel();
    context
        .send(
            ServiceKind::Rebuild,
            StartRebuildRequest { disk, completed },
        )
        .await?;
    ticket
        .await
        .map_err(|_| RuntimeError::ChannelClosed("rebuild completion".into()))
}
