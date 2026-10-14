//! VD capability adapter, not an implementation of BG scheduling or recovery.
use crate::{
    member_disk::{admission::Admission, *},
    runtime::Lifecycle,
};
use std::sync::Arc;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct VdService {
    pool: Arc<str>,
    identity: Arc<()>,
    provider: Arc<dyn VirtualDisks>,
    admission: Admission,
}

impl VdService {
    pub(crate) fn new(
        pool: Arc<str>,
        identity: Arc<()>,
        provider: Arc<dyn VirtualDisks>,
        changed: Arc<Notify>,
    ) -> Self {
        Self {
            pool,
            identity,
            provider,
            admission: Admission::new(changed),
        }
    }
    pub(crate) fn lifecycle(&self, state: Lifecycle) {
        self.admission.lifecycle(state);
    }
    pub(crate) fn stop(&self) {
        self.admission.stop();
    }
    pub(crate) fn is_idle(&self) -> bool {
        self.admission.is_idle()
    }
    pub fn blocked_resources(&self) -> Vec<Option<DiskUuid>> {
        self.admission.blocked()
    }

    pub async fn evacuate(
        &self,
        ticket: DrainTicket,
        caller: &CancellationToken,
    ) -> Result<DrainProof, MemberDiskError> {
        if ticket.pool != self.pool || !Arc::ptr_eq(&ticket.identity, &self.identity) {
            return Err(MemberDiskError::InvalidRequest(
                "drain ticket belongs to another Pool instance",
            ));
        }
        let mut reservation = self
            .admission
            .acquire(Some(ticket.disk.clone()), caller)
            .await?;
        let cancel = reservation.cancel.clone();
        let result = {
            let action = self.provider.evacuate(&ticket.disk, &cancel);
            tokio::pin!(action);
            tokio::select! {
                biased;
                result = &mut action => result,
                _ = caller.cancelled() => { cancel.cancel(); action.await },
            }
        };
        let result = if cancel.is_cancelled() {
            Err(MemberDiskError::Cancelled)
        } else {
            result.map_err(|error| MemberDiskError::NeedsRecovery(error.0))
        };
        reservation.settled(&result);
        result?;
        // Provider success must guarantee a persistent reference barrier, not
        // merely observe zero references. The local ticket also checks identity/version.
        Ok(DrainProof { ticket })
    }
}
