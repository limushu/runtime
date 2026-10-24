//! One task identity, independent of persistence and execution attempts.
use crate::{Failure, Outcome};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{
    fmt,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

/// Fixed capacity keeps IDs cheap to copy without interning untrusted persisted strings.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct Business {
    bytes: [u8; 96],
    len: u8,
}
impl Business {
    fn new(name: &str) -> Outcome<Self> {
        if name.is_empty()
            || name.len() > 96
            || !name
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
        {
            return Err(Failure::Rejected(
                "business key must be 1..96 ASCII letters/digits/._-".into(),
            ));
        }
        let mut bytes = [0; 96];
        bytes[..name.len()].copy_from_slice(name.as_bytes());
        Ok(Self {
            bytes,
            len: name.len() as u8,
        })
    }
    fn name(&self) -> &str {
        std::str::from_utf8(&self.bytes[..self.len as usize]).unwrap()
    }
}

/// Ordered by creation epoch and globally allocated sequence, never by the display string.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TaskId {
    pub epoch: u64,
    pub sequence: u64,
    pub pool: u128,
    business: Business,
}
impl TaskId {
    pub fn business(&self) -> &str {
        self.business.name()
    }
}
impl fmt::Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{:032x}/{}/{}",
            self.epoch,
            self.pool,
            self.business(),
            self.sequence
        )
    }
}
impl fmt::Debug for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl FromStr for TaskId {
    type Err = Failure;
    fn from_str(s: &str) -> Outcome<Self> {
        let invalid = || Failure::Rejected("invalid task id".into());
        let parts: Vec<_> = s.split('/').collect();
        if parts.len() != 4 {
            return Err(invalid());
        }
        let result = Self {
            epoch: parts[0].parse().map_err(|_| invalid())?,
            pool: u128::from_str_radix(parts[1], 16).map_err(|_| invalid())?,
            business: Business::new(parts[2])?,
            sequence: parts[3].parse().map_err(|_| invalid())?,
        };
        if result.sequence == 0 || result.pool == 0 {
            return Err(invalid());
        }
        Ok(result)
    }
}
impl Serialize for TaskId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}
impl<'de> Deserialize<'de> for TaskId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        String::deserialize(d)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

/// The election authority supplies a fresh durable epoch. Share ONE allocator across pools.
/// Recreating an allocator with a used epoch is forbidden, including after process restart.
#[derive(Clone)]
pub struct TaskIds(Arc<Generation>);
struct Generation {
    epoch: u64,
    next: AtomicU64,
    active: AtomicBool,
    cancelled: tokio_util::sync::CancellationToken,
}
impl TaskIds {
    pub fn for_leader(epoch: u64) -> Outcome<Self> {
        if epoch == 0 {
            return Err(Failure::Rejected("leader epoch must be nonzero".into()));
        }
        Ok(Self::new(epoch))
    }
    fn new(epoch: u64) -> Self {
        Self(Arc::new(Generation {
            epoch,
            next: AtomicU64::new(1),
            active: AtomicBool::new(true),
            cancelled: tokio_util::sync::CancellationToken::new(),
        }))
    }
    pub(crate) fn local() -> Self {
        Self::new(0)
    }
    pub fn epoch(&self) -> u64 {
        self.0.epoch
    }
    pub fn revoke(&self) {
        self.0.active.store(false, Ordering::SeqCst);
        self.0.cancelled.cancel();
    }
    pub(crate) fn cancellation(&self) -> tokio_util::sync::CancellationToken {
        self.0.cancelled.child_token()
    }
    pub fn active(&self) -> bool {
        self.0.active.load(Ordering::SeqCst)
    }
    pub(crate) fn allocate(&self, pool: u128, business: &str) -> Outcome<TaskId> {
        let business = Business::new(business)?;
        if !self.active() {
            return Err(Failure::Unavailable);
        }
        let sequence = self
            .0
            .next
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_add(1))
            .map_err(|_| Failure::Rejected("task sequence exhausted".into()))?;
        if !self.active() {
            return Err(Failure::Unavailable);
        }
        Ok(TaskId {
            epoch: self.epoch(),
            sequence,
            pool,
            business,
        })
    }
}

/// Compatibility spelling; this is the SAME identity, not an execution ID.
pub type WorkflowId = TaskId;
/// Legacy invalid/local ID constructor, retained for old diagnostic callers.
#[allow(non_snake_case)]
pub fn WorkflowId(sequence: u64) -> TaskId {
    TaskId {
        epoch: 0,
        sequence,
        pool: 0,
        business: Business::new("legacy").unwrap(),
    }
}
