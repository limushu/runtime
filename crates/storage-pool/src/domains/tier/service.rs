use super::*;
use crate::{Outcome, ports::*, support::reject};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};

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
    tiers: Mutex<BTreeMap<TierId, Tier>>,
    deleted: Mutex<BTreeSet<TierId>>,
}
impl TierService {
    pub fn new(pool: PoolId, metadata: Arc<dyn MetadataStore>) -> Self {
        Self {
            pool,
            metadata,
            tiers: Mutex::default(),
            deleted: Mutex::default(),
        }
    }
    pub(crate) fn restore(&self, tiers: Vec<Tier>) {
        *self.tiers.lock().unwrap() = tiers.into_iter().map(|t| (t.id.clone(), t)).collect();
    }
    pub async fn execute(&self, request: TierCommand) -> Outcome<TierReply> {
        match request {
            TierCommand::Create(id) => {
                if self.deleted.lock().unwrap().contains(&id) {
                    return Err(reject("tier identity is retired"));
                }
                if let Some(tier) = self.tiers.lock().unwrap().get(&id).cloned() {
                    return Ok(TierReply::Tier(tier));
                }
                let tier = Tier {
                    id: id.clone(),
                    pool: self.pool.clone(),
                    revision: 0,
                };
                self.metadata
                    .commit(&self.pool, MetadataChange::CreateTier(tier.clone()))
                    .await?;
                self.tiers.lock().unwrap().insert(id, tier.clone());
                Ok(TierReply::Tier(tier))
            }
            TierCommand::Inspect(id) => Ok(TierReply::Tier(self.get(&id)?)),
            TierCommand::Delete(id) => {
                if self.deleted.lock().unwrap().contains(&id) {
                    return Ok(TierReply::Deleted);
                }
                let tier = self.get(&id)?;
                self.metadata
                    .commit(
                        &self.pool,
                        MetadataChange::DeleteTier {
                            id: id.clone(),
                            expected_revision: tier.revision,
                        },
                    )
                    .await?;
                self.tiers.lock().unwrap().remove(&id);
                self.deleted.lock().unwrap().insert(id);
                Ok(TierReply::Deleted)
            }
        }
    }
    fn get(&self, id: &TierId) -> Outcome<Tier> {
        self.tiers
            .lock()
            .unwrap()
            .get(id)
            .cloned()
            .ok_or_else(|| reject("unknown tier"))
    }
    pub fn query(&self, id: Option<TierId>) -> Outcome<Vec<Tier>> {
        match id {
            Some(id) => Ok(vec![self.get(&id)?]),
            None => Ok(self.tiers.lock().unwrap().values().cloned().collect()),
        }
    }
}
