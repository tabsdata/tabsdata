//
// Copyright 2026 Tabsdata Inc.
//

//! [`Summary`]: counts gathered between two summary log lines.

use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Counts events as they happen and hands them over once `every` has passed
/// since the last hand-over, for the caller to log as one line.
pub struct Summary<C> {
    every: Duration,
    state: Mutex<(Instant, C)>,
}

impl<C: Default> Summary<C> {
    pub fn new(every: Duration) -> Self {
        Self {
            every,
            state: Mutex::new((Instant::now(), C::default())),
        }
    }

    /// Counts an event, and returns every count since the last hand-over
    /// once a summary is due.
    pub fn record(&self, event: impl FnOnce(&mut C)) -> Option<C> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let (since, counts) = &mut *state;
        event(counts);
        if since.elapsed() < self.every {
            return None;
        }
        *since = Instant::now();
        Some(std::mem::take(counts))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_are_handed_over_once_due_and_start_again() {
        let summary = Summary::<u32>::new(Duration::ZERO);
        assert_eq!(summary.record(|c| *c += 2), Some(2));
        assert_eq!(summary.record(|c| *c += 1), Some(1));
    }

    #[test]
    fn counts_are_kept_until_due() {
        let summary = Summary::<u32>::new(Duration::from_secs(3600));
        assert_eq!(summary.record(|c| *c += 1), None);
        assert_eq!(summary.record(|c| *c += 1), None);
    }
}
