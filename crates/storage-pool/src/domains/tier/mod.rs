pub(crate) mod runtime;
mod service;
use crate::{PoolId, TierId};
pub(crate) use service::{TierCommand, TierReply, TierService};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Tier {
    pub id: TierId,
    pub pool: PoolId,
    pub revision: u64,
}
