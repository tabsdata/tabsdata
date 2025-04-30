//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::services::JwtConfig;
use crate::auth::{encode_token, TokenClaims};
use td_common::id::Id;
use td_error::TdError;
use td_objects::types::auth::{SessionDB, TokenResponseX};
use td_tower::extractors::{Input, SrvCtx};

pub async fn create_access_token(
    SrvCtx(jwt_settings): SrvCtx<JwtConfig>,
    Input(session): Input<SessionDB>,
) -> Result<TokenResponseX, TdError> {
    const BEARER: &str = "Bearer";

    let id: Id = **session.access_token_id();
    let access_token = TokenClaims::new(id, session.expires_on().timestamp());
    let id: Id = **session.refresh_token_id();
    let refresh_token = TokenClaims::new(id, session.expires_on().timestamp() * 2);

    let token = TokenResponseX::builder()
        .try_access_token(encode_token(&jwt_settings, &access_token)?)?
        .try_token_type(BEARER)?
        .try_refresh_token(encode_token(&jwt_settings, &refresh_token)?)?
        .try_expires_in(*jwt_settings.access_token_expiration())?
        .build()?;
    Ok(token)
}
