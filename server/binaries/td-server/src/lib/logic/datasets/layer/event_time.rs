//
// Copyright 2024 Tabs Data Inc.
//

use td_common::error::TdError;
use td_common::time::UniqueUtc;
use td_objects::dlo::RequestTime;

pub async fn event_time() -> Result<RequestTime, TdError> {
    Ok(RequestTime::new(UniqueUtc::now_millis().await))
}
