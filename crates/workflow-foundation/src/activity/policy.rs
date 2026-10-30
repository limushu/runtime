use crate::{Failure, Outcome};
use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backoff {
    Fixed(Duration),
    /// Double the delay after each retry, capped at maximum.
    Exponential {
        initial: Duration,
        maximum: Duration,
    },
}
/// Only Retry decisions consume the budget; suspension continuations do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub backoff: Backoff,
}
impl Default for RetryPolicy {
    fn default() -> Self {
        Self::fixed(0, Duration::ZERO)
    }
}
impl RetryPolicy {
    pub const fn fixed(max_retries: u32, delay: Duration) -> Self {
        Self {
            max_retries,
            backoff: Backoff::Fixed(delay),
        }
    }
    pub const fn exponential(max_retries: u32, initial: Duration, maximum: Duration) -> Self {
        Self {
            max_retries,
            backoff: Backoff::Exponential { initial, maximum },
        }
    }
    pub(crate) fn validate(self) -> Outcome<()> {
        if let Backoff::Exponential { initial, maximum } = self.backoff
            && (initial.is_zero() || initial > maximum)
        {
            return Err(Failure::Rejected(
                "invalid exponential retry backoff".into(),
            ));
        }
        Ok(())
    }
    /// One-based retry ordinal, independent of execution/continuation count.
    pub(crate) fn delay(self, retry: u32) -> Duration {
        match self.backoff {
            Backoff::Fixed(delay) => delay,
            Backoff::Exponential { initial, maximum } => {
                let mut delay = initial;
                let mut remaining = retry.saturating_sub(1);
                while remaining > 0 && delay < maximum {
                    delay = delay.saturating_mul(2).min(maximum);
                    remaining -= 1;
                }
                delay
            }
        }
    }
}
