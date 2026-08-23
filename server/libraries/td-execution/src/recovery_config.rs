//
// Copyright 2026 Tabsdata Inc.
//

use crate::knob::{count_knob, seconds_knob};
use crate::scheduler_config::SchedulerCheckFrequency;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default)]
pub struct RecoveryConfig {
    pub check_frequency: SchedulerCheckFrequency,
    pub tick_timeout: RecoveryTickTimeout,
    pub stuck_requested_timeout: StuckRequestedTimeout,
    pub dispatch_grace: DispatchGrace,
    pub max_pre_running_time: MaxPreRunningTime,
    pub max_running_time: MaxRunningTime,
    pub max_completion_time: MaxCompletionTime,
    pub max_recoveries_per_tick: MaxRecoveriesPerTick,
}

seconds_knob!(
    /// Budget for one recovery tick. Recovery lists orchestrator workloads
    /// and acts on the divergent ones, so it needs a larger budget than the
    /// request tick.
    RecoveryTickTimeout,
    60
);

seconds_knob!(
    /// How long a run may sit in `Requested` before recovery auto-recovers
    /// it (the sweep cannot be disabled — dispatch relies on it to replace
    /// runs whose pod was never created). The cutoff must comfortably exceed
    /// worst-case pod startup under a deep dispatch queue: a shorter value
    /// declares still-queued pods dead and re-dispatches, duplicating their
    /// work.
    StuckRequestedTimeout,
    30 * 60
);

seconds_knob!(
    /// How old a dispatched attempt must be before recovery may judge a
    /// missing workload as dead. Worker workloads are created after the
    /// dispatch tick commits, so a recovery pass can observe a committed run
    /// whose workload is moments away from existing.
    DispatchGrace,
    120
);

seconds_knob!(
    /// How long a function-run workload may sit before its run reports
    /// `Running` (scheduling, image pulls, infra init steps) before recovery
    /// kills the attempt. Must comfortably exceed worst-case cold-node image
    /// pulls.
    MaxPreRunningTime,
    10 * 60
);

seconds_knob!(
    /// How long a function-run workload may stay in its completion-report
    /// step (run finished, terminal callback not landed) before recovery
    /// kills the attempt.
    MaxCompletionTime,
    10 * 60
);

count_knob!(
    /// Upper bound on recovery actions of each kind per tick — bounds the
    /// tick transaction and paces kill/create storms (the first post-upgrade
    /// tick can find everything divergent at once); the remainder recovers
    /// on the next ticks.
    MaxRecoveriesPerTick,
    50
);

/// Optional cap on a function run's execution time (from its `Running`
/// report). Disabled by default — legitimate runtimes vary too widely for a
/// universal kill-timer. Serdes as whole seconds; `<= 0` keeps it disabled.
#[derive(Debug, Clone, Copy, Default)]
pub struct MaxRunningTime(pub Option<Duration>);

impl MaxRunningTime {
    pub fn from_secs(secs: i64) -> Self {
        Self(
            u64::try_from(secs)
                .ok()
                .filter(|s| *s > 0)
                .map(Duration::from_secs),
        )
    }
}

impl Serialize for MaxRunningTime {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (self.0.map(|d| d.as_secs() as i64).unwrap_or(0)).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MaxRunningTime {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_secs(i64::deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stuck_requested_timeout_from_secs() {
        assert_eq!(
            StuckRequestedTimeout::from_secs(30).0,
            Duration::from_secs(30)
        );
        assert_eq!(
            StuckRequestedTimeout::from_secs(0).0,
            StuckRequestedTimeout::DEFAULT
        );
        assert_eq!(
            StuckRequestedTimeout::from_secs(-1).0,
            StuckRequestedTimeout::DEFAULT
        );
    }

    #[test]
    fn test_max_recoveries_per_tick_from_count() {
        assert_eq!(MaxRecoveriesPerTick::from_count(10).0, 10);
        assert_eq!(
            MaxRecoveriesPerTick::from_count(0).0,
            MaxRecoveriesPerTick::DEFAULT
        );
    }

    #[test]
    fn test_recovery_knobs_from_secs() {
        assert_eq!(
            RecoveryTickTimeout::from_secs(30).0,
            Duration::from_secs(30)
        );
        assert_eq!(
            RecoveryTickTimeout::from_secs(0).0,
            RecoveryTickTimeout::DEFAULT
        );
        assert_eq!(DispatchGrace::from_secs(-1).0, DispatchGrace::DEFAULT);
        assert_eq!(
            MaxPreRunningTime::from_secs(0).0,
            MaxPreRunningTime::DEFAULT
        );
        assert_eq!(MaxRunningTime::from_secs(0).0, None);
        assert_eq!(
            MaxRunningTime::from_secs(3600).0,
            Some(Duration::from_secs(3600))
        );
        assert_eq!(
            MaxCompletionTime::from_secs(-5).0,
            MaxCompletionTime::DEFAULT
        );
    }
}
