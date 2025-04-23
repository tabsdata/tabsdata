//
// Copyright 2025 Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use td_error::td_error;

#[td_error]
pub enum FromTimestampMillisError {
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(i64) = 0,
}

pub trait IntoDateTimeUtc {
    fn datetime_utc(self) -> Result<DateTime<Utc>, FromTimestampMillisError>;
}

impl IntoDateTimeUtc for i64 {
    fn datetime_utc(self) -> Result<DateTime<Utc>, FromTimestampMillisError> {
        match DateTime::from_timestamp_millis(self) {
            Some(dt) => Ok(dt),
            None => Err(FromTimestampMillisError::InvalidTimestamp(self)),
        }
    }
}
