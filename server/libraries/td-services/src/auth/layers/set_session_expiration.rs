//
// Copyright 2025 Tabsdata Inc.
//

use std::ops::Deref;
use std::time::Duration;
use td_common::jwt::JwtConfig;
use td_common::time::UniqueUtc;
use td_error::TdError;
use td_objects::dxo::auth::SessionDBBuilder;
use td_objects::types::basic::AtTime;
use td_tower::extractors::Ref;

pub async fn set_session_expiration(
    jwt_settings: Ref<JwtConfig>,
    session_builder: Ref<SessionDBBuilder>,
) -> Result<SessionDBBuilder, TdError> {
    let duration = Duration::from_secs(jwt_settings.access_token_expiration as u64);
    let expires_on: AtTime = (UniqueUtc::now_millis() + duration).try_into()?;
    let mut session_builder = session_builder.deref().clone();
    session_builder.expires_on(expires_on);
    Ok(session_builder)
}
