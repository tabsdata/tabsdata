//
// Copyright 2025 Tabs Data Inc.
//

use crate::dlo::Creator;
use td_common::id;
use td_error::TdError;

pub async fn new_id<C: Creator<String>>() -> Result<C, TdError> {
    Ok(C::create(id::id().to_string()))
}
