use super::*;
use crate::{Outcome, metadata::*, support::reject};
use std::sync::Arc;
pub(crate) enum TierCommand {
    Create(TierId),
    Inspect(TierId),
    Delete(TierId),
}
pub(crate) enum TierReply {
    Tier(Tier),
    Deleted,
}
pub(crate) struct TierService {
    pool: PoolId,
    metadata: Arc<dyn MetadataStore>,
}
impl TierService {
    pub fn new(pool: PoolId, metadata: Arc<dyn MetadataStore>) -> Self {
        Self { pool, metadata }
    }
    pub async fn execute(&self, request: TierCommand) -> Outcome<TierReply> {
        let snapshot = self.metadata.snapshot(&self.pool).await?;
        match request {
            TierCommand::Create(id) => {
                if snapshot.retired_tiers.contains(&id) {
                    return Err(reject("tier identity is retired"));
                }
                if let Some(tier) = snapshot.tiers.iter().find(|t| t.id == id) {
                    return Ok(TierReply::Tier(tier.clone()));
                }
                let tier = Tier {
                    id,
                    pool: self.pool.clone(),
                    revision: 0,
                };
                self.metadata
                    .commit(&self.pool, MetadataChange::CreateTier(tier.clone()))
                    .await?;
                Ok(TierReply::Tier(tier))
            }
            TierCommand::Inspect(id) => Ok(TierReply::Tier(
                snapshot
                    .tiers
                    .iter()
                    .find(|t| t.id == id)
                    .cloned()
                    .ok_or_else(|| reject("unknown tier"))?,
            )),
            TierCommand::Delete(id) => {
                if snapshot.retired_tiers.contains(&id) {
                    return Ok(TierReply::Deleted);
                }
                let tier = snapshot
                    .tiers
                    .iter()
                    .find(|t| t.id == id)
                    .ok_or_else(|| reject("unknown tier"))?;
                self.metadata
                    .commit(
                        &self.pool,
                        MetadataChange::DeleteTier {
                            id,
                            expected_revision: tier.revision,
                        },
                    )
                    .await?;
                Ok(TierReply::Deleted)
            }
        }
    }
}
