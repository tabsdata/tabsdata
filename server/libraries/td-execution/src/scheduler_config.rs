//
// Copyright 2026 Tabsdata Inc.
//

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default)]
pub struct SchedulerConfig {
    pub check_frequency: SchedulerCheckFrequency,
    pub stuck_requested_timeout: StuckRequestedTimeout,
    pub max_functions_per_tick: MaxFunctionsPerTick,
    pub tick_timeout: SchedulerTickTimeout,
}

/// Failover polling interval for the scheduler loops. Serdes as whole
/// seconds; `<= 0` falls back to the default (the timer cannot be disabled).
#[derive(Debug, Clone, Copy)]
pub struct SchedulerCheckFrequency(pub Duration);

impl Default for SchedulerCheckFrequency {
    fn default() -> Self {
        Self(Self::DEFAULT)
    }
}

impl SchedulerCheckFrequency {
    pub const DEFAULT: Duration = Duration::from_secs(60);

    pub fn from_secs(secs: i64) -> Self {
        u64::try_from(secs)
            .ok()
            .filter(|s| *s > 0)
            .map(Duration::from_secs)
            .map(Self)
            .unwrap_or_default()
    }
}

impl Serialize for SchedulerCheckFrequency {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (self.0.as_secs() as i64).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SchedulerCheckFrequency {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_secs(i64::deserialize(deserializer)?))
    }
}

/// How long a run may sit in `Requested` before the scheduler auto-recovers
/// it. Serdes as whole seconds; `<= 0` falls back to the default (the sweep
/// cannot be disabled — dispatch relies on it to replace runs whose pod was
/// never created). The cutoff must comfortably exceed worst-case pod startup
/// under a deep dispatch queue: a shorter value declares still-queued pods
/// dead and re-dispatches, duplicating their work.
#[derive(Debug, Clone, Copy)]
pub struct StuckRequestedTimeout(pub Duration);

impl Default for StuckRequestedTimeout {
    fn default() -> Self {
        Self(Self::DEFAULT)
    }
}

impl StuckRequestedTimeout {
    pub const DEFAULT: Duration = Duration::from_secs(30 * 60);

    pub fn from_secs(secs: i64) -> Self {
        u64::try_from(secs)
            .ok()
            .filter(|s| *s > 0)
            .map(Duration::from_secs)
            .map(Self)
            .unwrap_or_default()
    }
}

impl Serialize for StuckRequestedTimeout {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (self.0.as_secs() as i64).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StuckRequestedTimeout {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_secs(i64::deserialize(deserializer)?))
    }
}

/// Budget for one scheduler tick (the request or dispatch service call). On
/// expiry the tick is cancelled and rolled back — external side effects are
/// post-commit, so a cancelled tick leaves nothing behind and the next tick
/// retries. Serdes as whole seconds; `<= 0` falls back to the default.
#[derive(Debug, Clone, Copy)]
pub struct SchedulerTickTimeout(pub Duration);

impl Default for SchedulerTickTimeout {
    fn default() -> Self {
        Self(Self::DEFAULT)
    }
}

impl SchedulerTickTimeout {
    pub const DEFAULT: Duration = Duration::from_secs(10);

    pub fn from_secs(secs: i64) -> Self {
        u64::try_from(secs)
            .ok()
            .filter(|s| *s > 0)
            .map(Duration::from_secs)
            .map(Self)
            .unwrap_or_default()
    }
}

impl Serialize for SchedulerTickTimeout {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (self.0.as_secs() as i64).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SchedulerTickTimeout {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_secs(i64::deserialize(deserializer)?))
    }
}

/// Upper bound on function runs dispatched per scheduler tick — bounds both
/// the tick transaction's size and the post-commit worker-message burst. A
/// tick that hits the cap re-notifies the scheduler, so the backlog drains in
/// back-to-back capped ticks (oldest runs first) instead of waiting for the
/// failover timer. Serdes as a whole count; `<= 0` falls back to the default.
#[derive(Debug, Clone, Copy)]
pub struct MaxFunctionsPerTick(pub u64);

impl Default for MaxFunctionsPerTick {
    fn default() -> Self {
        Self(Self::DEFAULT)
    }
}

impl MaxFunctionsPerTick {
    pub const DEFAULT: u64 = 50;

    pub fn from_count(count: i64) -> Self {
        u64::try_from(count)
            .ok()
            .filter(|c| *c > 0)
            .map(Self)
            .unwrap_or_default()
    }
}

impl Serialize for MaxFunctionsPerTick {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        (self.0 as i64).serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MaxFunctionsPerTick {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_count(i64::deserialize(deserializer)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
