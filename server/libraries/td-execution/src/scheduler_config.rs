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
/// it. Serdes as whole seconds; `<= 0` falls back to the default (the sweep
/// cannot be disabled — dispatch relies on it to replace runs whose pod was
/// never created).
#[derive(Debug, Clone, Copy)]
pub struct StuckRequestedTimeout(pub Duration);

impl Default for StuckRequestedTimeout {
    fn default() -> Self {
        Self(Self::DEFAULT)
    }
}

impl StuckRequestedTimeout {
    pub const DEFAULT: Duration = Duration::from_secs(120);

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
