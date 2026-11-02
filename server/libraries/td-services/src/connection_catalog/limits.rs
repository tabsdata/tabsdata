//
// Copyright 2026 Tabsdata Inc.
//

//! Per-user probe submissions per minute. In-process, so each apiserver
//! replica counts its own. A submission locks only its connection, so
//! concurrent submissions on different connections can briefly overshoot the
//! per-project queue cap; the concurrency caps hold under the dispatcher's
//! claim lock.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use td_objects::types::basic::UserId;

const WINDOW: Duration = Duration::from_secs(60);

pub struct SubmitLimiter {
    per_window: u32,
    windows: Mutex<HashMap<UserId, (Instant, u32)>>,
}

impl SubmitLimiter {
    pub fn new(per_window: u32) -> Self {
        Self {
            per_window,
            windows: Mutex::new(HashMap::new()),
        }
    }

    /// Counts a submission, or returns how long until the user may submit
    /// again.
    pub fn admit(&self, user: &UserId) -> Result<(), Duration> {
        self.admit_at(user, Instant::now())
    }

    fn admit_at(&self, user: &UserId, now: Instant) -> Result<(), Duration> {
        let mut windows = self.windows.lock().unwrap_or_else(|e| e.into_inner());
        windows.retain(|_, (start, _)| now.duration_since(*start) < WINDOW);
        let (start, count) = windows.entry(*user).or_insert((now, 0));
        if *count >= self.per_window {
            return Err(WINDOW.saturating_sub(now.duration_since(*start)));
        }
        *count += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_user_past_the_budget_waits_for_the_window_to_roll() {
        let limiter = SubmitLimiter::new(2);
        let (user, other) = (UserId::default(), UserId::default());
        let start = Instant::now();
        assert!(limiter.admit_at(&user, start).is_ok());
        assert!(limiter.admit_at(&user, start).is_ok());
        let retry = limiter
            .admit_at(&user, start + Duration::from_secs(10))
            .unwrap_err();
        assert_eq!(retry, Duration::from_secs(50));
        assert!(limiter.admit_at(&other, start).is_ok());
        assert!(limiter.admit_at(&user, start + WINDOW).is_ok());
    }
}
