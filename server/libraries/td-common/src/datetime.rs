//
// Copyright 2025 Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use td_error::td_error;

#[td_error]
pub enum IntoDateTimeError {
    #[error("Invalid timestamp: {0}")]
    InvalidTimestampMillis(i64) = 0,
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(String) = 1,
}

pub trait IntoDateTimeUtc {
    fn datetime_utc(self) -> Result<DateTime<Utc>, IntoDateTimeError>;
}

impl IntoDateTimeUtc for i64 {
    fn datetime_utc(self) -> Result<DateTime<Utc>, IntoDateTimeError> {
        match DateTime::from_timestamp_millis(self) {
            Some(dt) => Ok(dt),
            None => Err(IntoDateTimeError::InvalidTimestampMillis(self)),
        }
    }
}

impl IntoDateTimeUtc for &str {
    fn datetime_utc(self) -> Result<DateTime<Utc>, IntoDateTimeError> {
        // First, try to parse as a timestamp in milliseconds
        let val = self
            .parse::<i64>()
            .map_err(|_| IntoDateTimeError::InvalidTimestamp(self.to_string()));
        if let Ok(dt) = val {
            return dt.datetime_utc();
        }

        // Then, try to parse as a string representation of a DateTime
        self.parse::<DateTime<Utc>>()
            .map_err(|_| IntoDateTimeError::InvalidTimestamp(self.to_string()))
    }
}
