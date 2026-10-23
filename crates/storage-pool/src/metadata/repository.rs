use super::{state::State, *};
use crate::{support::reject, *};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

/// Versioned entity records, ownership indexes and receipt records over any transactional KV.
/// Each pool revision guards predicate reads (including empty prefix scans), preventing phantoms.
pub struct KvMetadata {
    kv: Arc<dyn KvStore>,
}
fn hex(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn prefix(pool: &PoolId) -> Vec<u8> {
    format!("runtime/metadata/v1/pools/{}/", hex(pool.as_str())).into_bytes()
}
fn owner(disk: &DiskId) -> Vec<u8> {
    format!("runtime/metadata/v1/disk-owners/{}/", hex(disk.as_str())).into_bytes()
}
fn encode(value: &impl Serialize) -> Outcome<Vec<u8>> {
    serde_json::to_vec(value).map_err(|e| Failure::Failed(format!("encode metadata: {e}")))
}
fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Outcome<T> {
    serde_json::from_slice(bytes).map_err(|e| Failure::Failed(format!("decode metadata: {e}")))
}
impl KvMetadata {
    pub fn new(kv: Arc<dyn KvStore>) -> Self {
        Self { kv }
    }
}

fn load(pool: &PoolId, records: &KvSnapshot) -> Outcome<State> {
    let prefix = prefix(pool);
    let mut state = State::default();
    for (key, value) in records.iter().filter(|(k, _)| k.starts_with(&prefix)) {
        let suffix =
            std::str::from_utf8(&key[prefix.len()..]).map_err(|e| reject(e.to_string()))?;
        match suffix.split('/').next().unwrap_or("") {
            "revision" => {}
            "tier" => {
                let t: Tier = decode(value)?;
                if t.pool != *pool {
                    return Err(reject("tier pool mismatch"));
                }
                state.tiers.insert((pool.clone(), t.id.clone()), t);
            }
            "disk" => {
                let d: MemberDisk = decode(value)?;
                if d.pool != *pool {
                    return Err(reject("disk pool mismatch"));
                }
                state.disks.insert((pool.clone(), d.id.clone()), d);
            }
            "volume" => {
                let v: VirtualDisk = decode(value)?;
                if v.pool != *pool {
                    return Err(reject("volume pool mismatch"));
                }
                state.volumes.insert((pool.clone(), v.id.clone()), v);
            }
            "deleted-volume" => {
                let v: VirtualDisk = decode(value)?;
                if v.pool != *pool {
                    return Err(reject("deleted volume pool mismatch"));
                }
                state.retired_volumes.insert((pool.clone(), v.id.clone()));
                state
                    .deleted_volumes
                    .insert((pool.clone(), v.id.clone()), v);
            }
            "retired-tier" => {
                state.retired_tiers.insert((pool.clone(), decode(value)?));
            }
            "retired-disk" => {
                state.retired_disks.insert((pool.clone(), decode(value)?));
            }
            "allocation" => {
                let a: AllocationOwner = decode(value)?;
                state
                    .allocations
                    .insert((pool.clone(), a.disk, a.blk), a.volume);
            }
            "released" => {
                let a: AllocationOwner = decode(value)?;
                state
                    .released
                    .insert((pool.clone(), a.disk, a.blk, a.volume));
            }
            _ => return Err(reject("unsupported metadata record")),
        }
    }
    // A noncanonical key could otherwise hide a duplicate entity from compare-and-write.
    let canonical = records_for(pool, &state)?;
    if records
        .iter()
        .filter(|(k, _)| {
            k.starts_with(&prefix) && k.as_slice() != [prefix.as_slice(), b"revision"].concat()
        })
        .any(|(k, v)| canonical.get(k) != Some(v))
    {
        return Err(reject("noncanonical metadata record"));
    }
    Ok(state)
}
fn records_for(pool: &PoolId, state: &State) -> Outcome<KvSnapshot> {
    let prefix = String::from_utf8(prefix(pool)).unwrap();
    let mut records = KvSnapshot::new();
    let mut put = |suffix: String, value: Vec<u8>| {
        records.insert(format!("{prefix}{suffix}").into_bytes(), value);
    };
    for t in state.tiers.values() {
        put(format!("tier/{}", hex(t.id.as_str())), encode(t)?);
    }
    for d in state.disks.values() {
        put(format!("disk/{}", hex(d.id.as_str())), encode(d)?);
    }
    for v in state.volumes.values() {
        put(format!("volume/{}", hex(v.id.as_str())), encode(v)?);
    }
    for v in state.deleted_volumes.values() {
        put(format!("deleted-volume/{}", hex(v.id.as_str())), encode(v)?);
    }
    for (_, id) in &state.retired_tiers {
        put(format!("retired-tier/{}", hex(id.as_str())), encode(id)?);
    }
    for (_, id) in &state.retired_disks {
        put(format!("retired-disk/{}", hex(id.as_str())), encode(id)?);
    }
    for ((_, disk, blk), volume) in &state.allocations {
        put(
            format!("allocation/{}/{blk:08x}", hex(disk.as_str())),
            encode(&AllocationOwner {
                disk: disk.clone(),
                blk: *blk,
                volume: volume.clone(),
            })?,
        );
    }
    for (_, disk, blk, volume) in &state.released {
        put(
            format!(
                "released/{}/{blk:08x}/{}",
                hex(disk.as_str()),
                hex(volume.as_str())
            ),
            encode(&AllocationOwner {
                disk: disk.clone(),
                blk: *blk,
                volume: volume.clone(),
            })?,
        );
    }
    Ok(records)
}
#[async_trait]
impl MetadataStore for KvMetadata {
    async fn snapshot(&self, pool: &PoolId) -> Outcome<MetadataSnapshot> {
        let records = self.kv.snapshot(&[prefix(pool)]).await?;
        let state = load(pool, &records)?;
        Ok(MetadataSnapshot {
            tiers: state.tiers.into_values().collect(),
            disks: state.disks.into_values().collect(),
            volumes: state.volumes.into_values().collect(),
            allocations: state
                .allocations
                .into_iter()
                .map(|((_, disk, blk), volume)| AllocationOwner { disk, blk, volume })
                .collect(),
            retired_tiers: state.retired_tiers.into_iter().map(|(_, id)| id).collect(),
            retired_disks: state.retired_disks.into_iter().map(|(_, id)| id).collect(),
            deleted_volumes: state.deleted_volumes.into_values().collect(),
            released: state
                .released
                .into_iter()
                .map(|(_, disk, blk, volume)| AllocationOwner { disk, blk, volume })
                .collect(),
        })
    }
    async fn commit(&self, pool: &PoolId, change: MetadataChange) -> Outcome<()> {
        // Only a confirmed CAS miss may retry. Revalidation retains the caller's entity
        // revisions, so unrelated changes can proceed but stale business mutations fail.
        for _ in 0..16 {
            if self.commit_once(pool, &change).await? {
                return Ok(());
            }
        }
        Err(reject("metadata contention; re-read before retry"))
    }
}
impl KvMetadata {
    async fn commit_once(&self, pool: &PoolId, change: &MetadataChange) -> Outcome<bool> {
        let disk_owner = match change {
            MetadataChange::AddDisk(disk) => Some((disk.id.clone(), true)),
            MetadataChange::RemoveDisk { disk, .. } | MetadataChange::MarkRemoved { disk, .. } => {
                Some((disk.clone(), false))
            }
            _ => None,
        };
        let mut prefixes = vec![prefix(pool)];
        if let Some((disk, _)) = &disk_owner {
            prefixes.push(owner(disk));
        }
        let records = self.kv.snapshot(&prefixes).await?;
        let mut state = load(pool, &records)?;
        let before = records_for(pool, &state)?;
        state.apply(pool, change)?;
        let after = records_for(pool, &state)?;
        let revision_key = [prefix(pool), b"revision".to_vec()].concat();
        let revision: u64 = records
            .get(&revision_key)
            .map(|b| decode(b))
            .transpose()?
            .unwrap_or(0);
        let next = revision
            .checked_add(1)
            .ok_or_else(|| reject("metadata revision exhausted"))?;
        let mut tx = KvTransaction {
            checks: vec![KvCheck {
                key: revision_key.clone(),
                expected: records.get(&revision_key).cloned(),
            }],
            writes: vec![],
        };
        for key in before
            .keys()
            .chain(after.keys())
            .collect::<std::collections::BTreeSet<_>>()
        {
            if before.get(key) != after.get(key) {
                tx.writes.push(KvWrite {
                    key: key.clone(),
                    value: after.get(key).cloned(),
                });
            }
        }
        if let Some((disk, add)) = disk_owner {
            let key = owner(&disk);
            let expected = records.get(&key).cloned();
            if expected
                .as_ref()
                .is_some_and(|v| v.as_slice() != pool.as_str().as_bytes())
            {
                return Err(reject("physical disk belongs to another pool"));
            }
            tx.checks.push(KvCheck {
                key: key.clone(),
                expected,
            });
            tx.writes.push(KvWrite {
                key,
                value: add.then(|| pool.as_str().as_bytes().to_vec()),
            });
        }
        tx.writes.push(KvWrite {
            key: revision_key,
            value: Some(encode(&next)?),
        });
        self.kv.transact(tx).await
    }
}
