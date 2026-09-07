//
// Copyright 2025 Tabsdata Inc.
//

use std::time::Duration;
use td_common::jwt::JwtConfig;
use td_common::time::UniqueUtc;
use td_error::TdError;
use td_objects::types::basic::AtTime;

pub fn session_expiration(jwt_settings: &JwtConfig) -> Result<AtTime, TdError> {
    let duration = Duration::from_secs(jwt_settings.access_token_expiration as u64);
    Ok((UniqueUtc::now_millis() + duration).into())
}
