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
    /// Budget for one project's scheduler pass (and one connection-probe
    /// dispatch): a timeout cancels the pass, rolls it back and raises the
    /// project's budget by this much, up to ten times this.
    SchedulerTickTimeout,
    10
);

count_knob!(
    /// Function runs dispatched per project pass, oldest first. A pass that
    /// hits the cap runs again.
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
