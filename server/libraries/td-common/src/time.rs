//
// Copyright 2024 Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use lazy_static::lazy_static;
use std::marker::PhantomData;
use std::sync::Mutex;

/// Type holder for [`UniqueUtc::now_millis`] function.
pub struct UniqueUtc {
    p: PhantomData<()>,
}

impl UniqueUtc {
    /// Returns a `DateTime<Utc>` with millisecond precision which corresponds to the current date
    /// and time in UTC ensuring that it never returns the same datetime more than once.
    ///
    /// It has millisecond precision as its main use is to generate timestamps for database entries
    /// using SQL time types.
    ///
    /// To achieve the uniqueness, without actually sleeping until the next millisecond, it
    /// artificially fast forwards the time to the next millisecond if the current time is the same
    /// as the last time it was called. This is done by keeping track of the last time it was called.
    /// As son as the current time is greater than the last time it was called, it resets the time
    /// to the current time, thus correcting itself to the current time.
    pub fn now_millis() -> DateTime<Utc> {
        let now = ELASTIC_UNIQUE_EPOCH.now();
        DateTime::<Utc>::from_timestamp_millis(now).unwrap()
    }
}

/// It keeps track of the next millisecond to be used a current time if the current time in millisecond
/// has not changed.
#[derive(Debug)]
struct ElasticUniqueEpoch {
    next: i64,
}

impl ElasticUniqueEpoch {
    /// Returns the current time in milliseconds since the Unix epoch.
    fn real_now() -> i64 {
        Utc::now().timestamp_millis()
    }

    /// Creates a new instance of [`ElasticUniqueEpoch`].
    fn new() -> Self {
        Self {
            next: Self::real_now(),
        }
    }

    /// Computes the unique current time in milliseconds since the Unix epoch using the provided
    /// clock function to get the real current time.
    ///
    /// This function only exists to enable testing.
    #[inline]
    fn now_with_clock(&mut self, clock: fn() -> i64) -> i64 {
        let now = clock();
        if now > self.next {
            self.next = now + 1;
            now
        } else {
            let now = self.next;
            self.next = now + 1;
            now
        }
    }

    /// Returns the current unique time in milliseconds since the Unix epoch.
    #[inline]
    fn now(&mut self) -> i64 {
        self.now_with_clock(Self::real_now)
    }
}

/// Thread safe version of [`ElasticUniqueEpoch`].
struct ThreadSafeElasticUniqueEpoch {
    eue: Mutex<ElasticUniqueEpoch>,
}

impl ThreadSafeElasticUniqueEpoch {
    /// Creates a new instance of [`ThreadSafeElasticUniqueEpoch`].
    fn new() -> Self {
        Self {
            eue: Mutex::new(ElasticUniqueEpoch::new()),
        }
    }

    /// Returns the current unique time in milliseconds since the Unix epoch.
    #[inline]
    fn now(&self) -> i64 {
        self.eue.lock().unwrap().now()
    }
}

// Singleton instance of [`ThreadSafeElasticUniqueEpoch`] used by [`UniqueUtc::now`].
lazy_static! {
    static ref ELASTIC_UNIQUE_EPOCH: ThreadSafeElasticUniqueEpoch =
        ThreadSafeElasticUniqueEpoch::new();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_eu_time() {
        let mut time = ElasticUniqueEpoch { next: 0 };
        assert_eq!(time.now_with_clock(|| 0), 0);
        assert_eq!(time.now_with_clock(|| 0), 1);
        assert_eq!(time.now_with_clock(|| 0), 2);
        assert_eq!(time.now_with_clock(|| 1), 3);
        assert_eq!(time.now_with_clock(|| 1), 4);
        assert_eq!(time.now_with_clock(|| 4), 5);
        assert_eq!(time.now_with_clock(|| 6), 6);
    }

    #[test]
    fn test_eut_now() {
        let mut times = Vec::with_capacity(1000);
        for _ in 0..1000 {
            times.push(UniqueUtc::now_millis().timestamp_millis());
        }
        let mut uniq = HashSet::new();
        let uniq = times.into_iter().all(|t| uniq.insert(t));
        assert!(uniq);
    }
}
