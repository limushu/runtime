//! Application-owned conflict policy; the runtime only executes its decision.
use std::{any::Any, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdmissionDecision {
    /// Normal object arbitration: overlapping work queues; disjoint work proceeds.
    Queue,
    /// Ask the existing execution to stop. Object ownership is NOT released early.
    CancelExisting,
    Reject(String),
}
pub trait AdmissionPolicy<D>: Send + Sync + 'static {
    /// Pure, synchronous, nonblocking. No mutation, IO, or task scheduling here.
    fn decide(&self, incoming: &D, existing: &D) -> AdmissionDecision;
}

type Decide = dyn Fn(&dyn Any) -> AdmissionDecision + Send + Sync;
pub(crate) struct Admission {
    descriptor: Box<dyn Any + Send + Sync>,
    decide: Box<Decide>,
}
impl Admission {
    pub fn new<D: Clone + Send + Sync + 'static, P: AdmissionPolicy<D>>(
        descriptor: D,
        policy: Arc<P>,
    ) -> Self {
        let incoming = descriptor.clone();
        Self {
            descriptor: Box::new(descriptor),
            decide: Box::new(move |existing| {
                existing
                    .downcast_ref::<D>()
                    .map_or(AdmissionDecision::Queue, |existing| {
                        policy.decide(&incoming, existing)
                    })
            }),
        }
    }
    pub fn decide(&self, existing: &Self) -> AdmissionDecision {
        (self.decide)(existing.descriptor.as_ref())
    }
}
