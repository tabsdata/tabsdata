//
// Copyright 2024 Tabs Data Inc.
//

use chrono::{DateTime, Utc};
use td_common::time::UniqueUtc;
use td_error::TdError;

pub async fn set_trigger_time() -> Result<DateTime<Utc>, TdError> {
    Ok(UniqueUtc::now_millis())
}
