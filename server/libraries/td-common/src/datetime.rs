//
// Copyright 2025 Tabsdata Inc.
//

use crate::time::UniqueUtc;
use chrono::{DateTime, Utc};
use std::fmt::Display;
use std::ops::Deref;
use std::str::FromStr;
use td_error::td_error;

#[td_error]
pub enum TimestampError {
    #[error("Invalid timestamp: {0}")]
    InvalidTimestampMillis(i64) = 0,
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String) = 1,
}

/// Timestamp implementation with serde/deser using milliseconds.
/// Sqlx encoding still preserves original Datetime encoding.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct Timestamp(DateTime<Utc>);

impl Default for Timestamp {
    fn default() -> Self {
        Timestamp(UniqueUtc::now_millis())
    }
}

impl Deref for Timestamp {
    type Target = DateTime<Utc>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<DateTime<Utc>> for Timestamp {
    fn from(dt: DateTime<Utc>) -> Self {
        Timestamp(dt)
    }
}

impl TryFrom<i64> for Timestamp {
    type Error = TimestampError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        Ok(Self(
            DateTime::<Utc>::from_timestamp_millis(value)
                .ok_or(TimestampError::InvalidTimestampMillis(value))?,
        ))
    }
}

impl Display for Timestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.timestamp_millis())
    }
}

impl FromStr for Timestamp {
    type Err = TimestampError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let date = if let Ok(dt) = s.parse::<i64>() {
            // First, try to parse as a timestamp in milliseconds
            match DateTime::from_timestamp_millis(dt) {
                Some(dt) => Ok(dt),
                None => Err(TimestampError::InvalidTimestampMillis(dt)),
            }
        } else {
            // Then, try to parse as a string representation of a DateTime
            s.parse::<DateTime<Utc>>()
                .map_err(|_| TimestampError::InvalidTimestamp(s.to_string()))
        }?;

        Ok(Timestamp(date))
    }
}

impl<'de> serde::Deserialize<'de> for Timestamp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = i64::deserialize(d)?;
        let s = DateTime::from_timestamp_millis(s)
            .ok_or_else(|| serde::de::Error::custom("Invalid timestamp"))?;
        Ok(Timestamp(s))
    }
}

impl serde::Serialize for Timestamp {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.timestamp_millis().serialize(s)
    }
}
