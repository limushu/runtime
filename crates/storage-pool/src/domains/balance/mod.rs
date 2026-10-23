mod model;
pub(crate) mod runtime;
pub use model::*;
pub(crate) enum BalanceCommand {
    Evacuate {
        disk: crate::DiskId,
        id: EvacuationId,
    },
    Cancel {
        disk: crate::DiskId,
        id: EvacuationId,
    },
}
