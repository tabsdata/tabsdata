//
// Copyright 2026 Tabsdata Inc.
//

use crate::knob::{count_knob, seconds_knob};

#[derive(Debug, Clone, Copy, Default)]
pub struct SchedulerConfig {
    pub check_frequency: SchedulerCheckFrequency,
    pub max_functions_per_tick: MaxFunctionsPerTick,
    pub tick_timeout: SchedulerTickTimeout,
}

seconds_knob!(
    /// Failover polling interval for the scheduler loops (the timer cannot
    /// be disabled).
    SchedulerCheckFrequency,
    60
);

seconds_knob!(
    /// Budget for one scheduler tick (the request or dispatch service call).
    /// On expiry the tick is cancelled and rolled back — external side
    /// effects are post-commit, so a cancelled tick leaves nothing behind
    /// and the next tick retries.
    SchedulerTickTimeout,
    10
);

count_knob!(
    /// Upper bound on function runs dispatched per scheduler tick — bounds
    /// both the tick transaction's size and the post-commit worker-message
    /// burst. A tick that hits the cap re-notifies the scheduler, so the
    /// backlog drains in back-to-back capped ticks (oldest runs first)
    /// instead of waiting for the failover timer.
    MaxFunctionsPerTick,
    50
);

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_scheduler_check_frequency_from_secs() {
        assert_eq!(
            SchedulerCheckFrequency::from_secs(30).0,
            Duration::from_secs(30)
        );
        assert_eq!(
            SchedulerCheckFrequency::from_secs(0).0,
            SchedulerCheckFrequency::DEFAULT
        );
        assert_eq!(
            SchedulerCheckFrequency::from_secs(-1).0,
            SchedulerCheckFrequency::DEFAULT
        );
    }

    #[test]
    fn test_scheduler_tick_timeout_from_secs() {
        assert_eq!(
            SchedulerTickTimeout::from_secs(30).0,
            Duration::from_secs(30)
        );
        assert_eq!(
            SchedulerTickTimeout::from_secs(0).0,
            SchedulerTickTimeout::DEFAULT
        );
        assert_eq!(
            SchedulerTickTimeout::from_secs(-1).0,
            SchedulerTickTimeout::DEFAULT
        );
    }

    #[test]
    fn test_max_functions_per_tick_from_count() {
        assert_eq!(MaxFunctionsPerTick::from_count(10).0, 10);
        assert_eq!(
            MaxFunctionsPerTick::from_count(0).0,
            MaxFunctionsPerTick::DEFAULT
        );
        assert_eq!(
            MaxFunctionsPerTick::from_count(-1).0,
            MaxFunctionsPerTick::DEFAULT
        );
    }
}
