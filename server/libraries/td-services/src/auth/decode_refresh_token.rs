//
// Copyright 2025 Tabsdata Inc.
//

use crate::auth::AuthError;
use td_common::jwt::{JwtConfig, decode_token};
use td_error::TdError;
use td_objects::types::basic::{RefreshToken, RefreshTokenId};

pub fn decode_refresh_token(
    jwt_config: &JwtConfig,
    refresh_token: &RefreshToken,
) -> Result<RefreshTokenId, TdError> {
    let token = refresh_token.as_str();
    let token = decode_token(jwt_config, token)
        .map_err(|_| TdError::from(AuthError::InvalidRefreshToken))?;
    let refresh_token_id: RefreshTokenId = token.jti.into();
    Ok(refresh_token_id)
}
