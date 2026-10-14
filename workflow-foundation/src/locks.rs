//! One arbitration table for both logical workflow ownership and activity resources.
use crate::{Access, Failure, ObjectKey, Outcome, ResourceAccess};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Workflow,
    Activity,
}
struct Claims {
    kind: Kind,
    access: Vec<ResourceAccess>,
}
impl Claims {
    fn overlaps(&self, other: &Self, fenced: bool) -> bool {
        self.kind == other.kind
            && self.access.iter().any(|a| {
                other.access.iter().any(|b| {
                    a.object == b.object
                        && (fenced || a.access == Access::Write || b.access == Access::Write)
                })
            })
    }
}
pub(crate) struct Request {
    id: u64,
    claims: Claims,
    reply: oneshot::Sender<Outcome<Permit>>,
    cancel: CancellationToken,
}
pub(crate) enum Event {
    Acquire(Request),
    Release { id: u64, uncertain: bool },
    Withdraw(u64),
}

#[derive(Clone)]
pub(crate) struct Broker {
    tx: mpsc::UnboundedSender<Event>,
    next: Arc<AtomicU64>,
}
impl Broker {
    pub fn new() -> (Self, mpsc::UnboundedReceiver<Event>) {
        let (tx, rx) = mpsc::unbounded_channel();
        (
            Self {
                tx,
                next: Arc::new(AtomicU64::new(1)),
            },
            rx,
        )
    }
    pub async fn acquire(
        &self,
        kind: Kind,
        access: Vec<ResourceAccess>,
        cancel: &CancellationToken,
    ) -> Outcome<Permit> {
        let mut normalized: BTreeMap<ObjectKey, Access> = BTreeMap::new();
        for access in access {
            access.object.validate()?;
            normalized
                .entry(access.object)
                .and_modify(|a| {
                    if access.access == Access::Write {
                        *a = Access::Write;
                    }
                })
                .or_insert(access.access);
        }
        let access = normalized
            .into_iter()
            .map(|(object, access)| ResourceAccess { object, access })
            .collect();
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (reply, response) = oneshot::channel();
        self.tx
            .send(Event::Acquire(Request {
                id,
                claims: Claims { kind, access },
                reply,
                cancel: cancel.clone(),
            }))
            .map_err(|_| Failure::Unavailable)?;
        let mut pending = Pending {
            id: Some(id),
            broker: self.clone(),
        };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err(Failure::Cancelled),
            result = response => {
                pending.id = None;
                result.unwrap_or(Err(Failure::Unavailable))
            }
        };
        result
    }
}
struct Pending {
    id: Option<u64>,
    broker: Broker,
}
impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            let _ = self.broker.tx.send(Event::Withdraw(id));
        }
    }
}

/// Follows actual execution, not the lifetime of the response receiver.
pub(crate) struct Permit {
    id: u64,
    broker: Broker,
    uncertain: bool,
}
impl Permit {
    pub fn started(&mut self) {
        self.uncertain = true;
    }
    pub fn settled<T>(&mut self, result: &Outcome<T>) {
        self.uncertain = result.as_ref().is_err_and(|e| e.uncertain());
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        let _ = self.broker.tx.send(Event::Release {
            id: self.id,
            uncertain: self.uncertain,
        });
    }
}

#[derive(Default)]
pub(crate) struct LockTable {
    held: BTreeMap<u64, Claims>,
    waiting: VecDeque<Request>,
    fences: Vec<Claims>,
}
impl LockTable {
    pub fn counts(&self) -> (usize, usize, usize) {
        (self.waiting.len(), self.held.len(), self.fences.len())
    }
    pub fn event(&mut self, event: Event, broker: &Broker) {
        match event {
            Event::Acquire(request) => self.waiting.push_back(request),
            Event::Withdraw(id) => self.waiting.retain(|r| r.id != id),
            Event::Release { id, uncertain } => {
                if let Some(claims) = self.held.remove(&id)
                    && uncertain
                    && !claims.access.is_empty()
                {
                    self.fences.push(claims);
                }
            }
        }
        self.pump(broker);
    }
    fn pump(&mut self, broker: &Broker) {
        let mut i = 0;
        while i < self.waiting.len() {
            let r = &self.waiting[i];
            if r.reply.is_closed() || r.cancel.is_cancelled() {
                let r = self.waiting.remove(i).unwrap();
                let _ = r.reply.send(Err(Failure::Cancelled));
                continue;
            }
            if self.fences.iter().any(|c| c.overlaps(&r.claims, true)) {
                let r = self.waiting.remove(i).unwrap();
                // Rejected waiters have caused no effects. Do not turn this
                // into a new uncertain execution and spread its ownership fence.
                let _ = r.reply.send(Err(Failure::Rejected(
                    "object requires reconciliation".into(),
                )));
                continue;
            }
            if self.held.values().any(|c| c.overlaps(&r.claims, false))
                || self
                    .waiting
                    .iter()
                    .take(i)
                    .any(|earlier| earlier.claims.overlaps(&r.claims, false))
            {
                i += 1;
                continue;
            }
            let r = self.waiting.remove(i).unwrap();
            self.held.insert(r.id, r.claims);
            let _ = r.reply.send(Ok(Permit {
                id: r.id,
                broker: broker.clone(),
                uncertain: false,
            }));
        }
    }
}
