/*
 * This file is partially copied from [tokio_retry](https://github.com/srijs/rust-tokio-retry/blob/master/src/strategy/exponential_backoff.rs)
 * and modified as it seems silly to pull-in a new dependency for a single file.
 * */
use rand::Rng;
use std::time::Duration;

/// A retry strategy driven by exponential back-off.
///
/// The power corresponds to the number of past attempts.
#[derive(Debug, Clone)]
pub struct ExponentialBackoff {
    current: u64,
    base: u64,
    max_delay: Option<Duration>,
}

impl ExponentialBackoff {
    /// Constructs a new exponential back-off strategy,
    /// given a base duration in milliseconds.
    ///
    /// The resulting duration is calculated by taking the base to the `n`-th power,
    /// where `n` denotes the number of past attempts.
    pub fn from_millis(base: u64) -> ExponentialBackoff {
        ExponentialBackoff {
            current: base,
            base,
            max_delay: None,
        }
    }

    pub fn with_limit(mut self, limit: Duration) -> Self {
        self.max_delay = Some(limit);
        self
    }

    pub fn success(&mut self) {
        self.current = self.base;
    }
}

impl Iterator for ExponentialBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        let duration = Duration::from_millis(self.current);

        // check if we reached max delay
        if let Some(ref max_delay) = self.max_delay {
            if duration > *max_delay {
                return Some(*max_delay);
            }
        }

        // calculate next delay
        if let Some(next) = self.current.checked_mul(self.base) {
            self.current = next;
        } else {
            self.current = u64::MAX;
        }

        Some(duration)
    }
}

pub struct RandomBackoff {
    min: Duration,
    max: Duration,
}

impl RandomBackoff {
    pub fn new(min: Duration, max: Duration) -> RandomBackoff {
        RandomBackoff { min, max }
    }
}

impl Iterator for RandomBackoff {
    type Item = Duration;

    fn next(&mut self) -> Option<Duration> {
        let mut rng = rand::thread_rng();
        let range = self.max - self.min;
        let duration = rng.gen_range(0..range.as_millis()) + self.min.as_millis();
        Some(Duration::from_millis(duration as u64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_some_exponential_base_10() {
        let mut s = ExponentialBackoff::from_millis(10);

        assert_eq!(s.next(), Some(Duration::from_millis(10)));
        assert_eq!(s.next(), Some(Duration::from_millis(100)));
        assert_eq!(s.next(), Some(Duration::from_millis(1000)));
    }

    #[test]
    fn returns_some_exponential_base_2() {
        let mut s = ExponentialBackoff::from_millis(2);

        assert_eq!(s.next(), Some(Duration::from_millis(2)));
        assert_eq!(s.next(), Some(Duration::from_millis(4)));
        assert_eq!(s.next(), Some(Duration::from_millis(8)));
    }

    #[test]
    fn saturates_at_maximum_value() {
        let mut s = ExponentialBackoff::from_millis(u64::MAX - 1);

        assert_eq!(s.next(), Some(Duration::from_millis(u64::MAX - 1)));
        assert_eq!(s.next(), Some(Duration::from_millis(u64::MAX)));
        assert_eq!(s.next(), Some(Duration::from_millis(u64::MAX)));
    }

    #[test]
    fn limits_backoff() {
        let mut s = ExponentialBackoff::from_millis(10).with_limit(Duration::from_millis(100));

        assert_eq!(s.next(), Some(Duration::from_millis(10)));
        assert_eq!(s.next(), Some(Duration::from_millis(100)));
        assert_eq!(s.next(), Some(Duration::from_millis(100)));
        assert_eq!(s.next(), Some(Duration::from_millis(100)));
    }
}
