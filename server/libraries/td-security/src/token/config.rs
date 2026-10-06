//
// Copyright 2026 Tabsdata Inc.
//

//! Deployment policy for service tokens.

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// No default: a deployment without a ceiling issues no tokens.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum MaxTokenDays {
    /// `-1`: no ceiling, and never-expiring tokens are permitted.
    Unbounded,
    Days(u32),
}

impl MaxTokenDays {
    pub fn permits_never(&self) -> bool {
        matches!(self, MaxTokenDays::Unbounded)
    }
}

impl Serialize for MaxTokenDays {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            MaxTokenDays::Unbounded => (-1i64).serialize(serializer),
            MaxTokenDays::Days(days) => i64::from(*days).serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for MaxTokenDays {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match i64::deserialize(deserializer)? {
            -1 => Ok(MaxTokenDays::Unbounded),
            days if days > 0 => u32::try_from(days).map(MaxTokenDays::Days).map_err(|_| {
                D::Error::custom(format!(
                    "max_days is too large to be a number of days: {days}"
                ))
            }),
            days => Err(D::Error::custom(format!(
                "max_days is a positive number of days, or -1 for no ceiling: {days}"
            ))),
        }
    }
}

/// Without a ceiling no token can be created; issued ones keep working.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceTokenConfig {
    #[serde(default)]
    pub max_days: Option<MaxTokenDays>,
}

#[cfg(test)]
mod tests {
    use super::{MaxTokenDays, ServiceTokenConfig};

    #[test]
    fn test_absent_by_default() {
        assert!(ServiceTokenConfig::default().max_days.is_none());
    }

    #[test]
    fn test_max_days_reads_the_documented_values() {
        let unbounded: ServiceTokenConfig = serde_yaml::from_str("max_days: -1").unwrap();
        assert_eq!(unbounded.max_days, Some(MaxTokenDays::Unbounded));
        let bounded: ServiceTokenConfig = serde_yaml::from_str("max_days: 90").unwrap();
        assert_eq!(bounded.max_days, Some(MaxTokenDays::Days(90)));
    }

    #[test]
    fn test_max_days_refuses_values_that_mean_nothing() {
        for value in ["max_days: 0", "max_days: -2"] {
            let refused = serde_yaml::from_str::<ServiceTokenConfig>(value);
            assert!(refused.is_err(), "{value} must not deserialize");
        }
    }

    #[test]
    fn test_max_days_round_trips() {
        for days in [MaxTokenDays::Unbounded, MaxTokenDays::Days(1)] {
            let rendered = serde_yaml::to_string(&days).unwrap();
            assert_eq!(
                serde_yaml::from_str::<MaxTokenDays>(&rendered).unwrap(),
                days
            );
        }
    }

    #[test]
    fn test_only_an_unbounded_ceiling_permits_never_expiring() {
        assert!(MaxTokenDays::Unbounded.permits_never());
        assert!(!MaxTokenDays::Days(90).permits_never());
    }
}
