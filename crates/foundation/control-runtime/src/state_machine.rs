use std::sync::Arc;

/// A domain state transition and the workflow, if any, needed to converge to it.
///
/// This type contains no actor, Future, channel, task or cancellation state.
/// Domain state machines describe the next projection, one domain-defined
/// object change and the desired workflow. The service runtime implements
/// start, join and cooperative replacement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transition<S, W, C = ()> {
    next: S,
    effect: TransitionEffect<W>,
    change: Option<C>,
}

impl<S, W, C> Transition<S, W, C> {
    pub fn to(next: S) -> Self {
        Self {
            next,
            effect: TransitionEffect::None,
            change: None,
        }
    }

    pub fn change(mut self, change: C) -> Self {
        self.change = Some(change);
        self
    }

    pub fn ensure(mut self, workflow: W) -> Self {
        self.effect = TransitionEffect::Ensure(workflow);
        self
    }

    pub fn reject(mut self, reason: impl Into<Arc<str>>) -> Self {
        self.effect = TransitionEffect::Reject(reason.into());
        self
    }

    pub fn next(&self) -> &S {
        &self.next
    }

    pub fn effect(&self) -> &TransitionEffect<W> {
        &self.effect
    }

    pub fn into_parts(self) -> (S, TransitionEffect<W>, Option<C>) {
        (self.next, self.effect, self.change)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransitionEffect<W> {
    None,
    Ensure(W),
    Reject(Arc<str>),
}
