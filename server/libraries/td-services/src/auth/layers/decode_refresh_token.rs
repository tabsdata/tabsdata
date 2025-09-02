//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::AuthError;
use crate::auth::jwt::{JwtConfig, decode_token};
use td_error::TdError;
use td_objects::types::basic::{RefreshToken, RefreshTokenId};
use td_tower::extractors::{Input, SrvCtx};

pub async fn decode_refresh_token(
    SrvCtx(jwt_config): SrvCtx<JwtConfig>,
    Input(refresh_token): Input<RefreshToken>,
) -> Result<RefreshTokenId, TdError> {
    let token = refresh_token.as_str();
    let token = decode_token(&jwt_config, token)
        .map_err(|_| TdError::from(AuthError::InvalidRefreshToken))?;
    let refresh_token_id: RefreshTokenId = token.jti().into();
    Ok(refresh_token_id)
}
