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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdmissionEvaluation {
    pub decision: AdmissionDecision,
    pub rule: Option<String>,
}
impl AdmissionEvaluation {
    pub fn named(rule: impl Into<String>, decision: AdmissionDecision) -> Self {
        Self {
            rule: Some(rule.into()),
            decision,
        }
    }
}
pub trait AdmissionPolicy<D>: Send + Sync + 'static {
    /// Pure, synchronous, nonblocking. No mutation, IO, or task scheduling here.
    fn decide(&self, incoming: &D, existing: &D) -> AdmissionDecision;
    /// Optional structured explanation, evaluated once at the serialized admission point.
    fn evaluate(&self, incoming: &D, existing: &D) -> AdmissionEvaluation {
        AdmissionEvaluation {
            decision: self.decide(incoming, existing),
            rule: None,
        }
    }
}

type Decide = dyn Fn(&dyn Any) -> AdmissionEvaluation + Send + Sync;
pub(crate) struct Admission {
    descriptor: Box<dyn Any + Send + Sync>,
    decide: Box<Decide>,
}
impl Admission {
    pub fn new<D: Clone + Send + Sync + 'static, P: AdmissionPolicy<D> + ?Sized>(
        descriptor: D,
        policy: Arc<P>,
    ) -> Self {
        let incoming = descriptor.clone();
        Self {
            descriptor: Box::new(descriptor),
            decide: Box::new(move |existing| {
                existing.downcast_ref::<D>().map_or(
                    AdmissionEvaluation::named(
                        "different-descriptor-type",
                        AdmissionDecision::Queue,
                    ),
                    |existing| policy.evaluate(&incoming, existing),
                )
            }),
        }
    }
    pub fn evaluate(&self, existing: &Self) -> AdmissionEvaluation {
        (self.decide)(existing.descriptor.as_ref())
    }
}
