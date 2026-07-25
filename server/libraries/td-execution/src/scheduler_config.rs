//
// Copyright 2026 Tabsdata Inc.
//

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default)]
pub struct SchedulerConfig {
    pub check_frequency: SchedulerCheckFrequency,
    pub stuck_requested_timeout: StuckRequestedTimeout,
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
/// it. Serdes as whole seconds; `<= 0` (`None`) disables the sweep.
#[derive(Debug, Clone, Copy)]
pub struct StuckRequestedTimeout(pub Option<Duration>);

impl Default for StuckRequestedTimeout {
    fn default() -> Self {
        Self(Some(Self::DEFAULT))
    }
}

impl StuckRequestedTimeout {
    pub const DEFAULT: Duration = Duration::from_secs(120);

    pub fn from_secs(secs: i64) -> Self {
        Self(
            u64::try_from(secs)
                .ok()
                .filter(|s| *s > 0)
                .map(Duration::from_secs),
        )
    }
}

impl Serialize for StuckRequestedTimeout {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
            .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StuckRequestedTimeout {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from_secs(i64::deserialize(deserializer)?))
    }
}
