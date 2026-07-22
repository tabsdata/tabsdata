//
// Copyright 2025 Tabsdata Inc.
//

use td_common::jwt::{JwtConfig, TokenClaims, encode_token};
use td_error::TdError;
use td_objects::Buildable;
use td_objects::dxo::auth::{SessionDB, TokenResponseX};
use td_tower::extractors::Ref;
use tracing::info;

pub async fn create_access_token(
    jwt_settings: Ref<JwtConfig>,
    session: Ref<SessionDB>,
) -> Result<TokenResponseX, TdError> {
    const BEARER: &str = "Bearer";

    let id = *session.access_token_id;
    let access_exp = session.expires_on.timestamp();
    let access_token = TokenClaims::new(id, access_exp);
    let id = *session.refresh_token_id;
    // `expires_on` is the access-token expiry (now + access_token_expiration); the
    // refresh token lives `refresh_token_expiration` from that same `now`.
    let refresh_token = TokenClaims::new(
        id,
        access_exp + jwt_settings.refresh_token_expiration - jwt_settings.access_token_expiration,
    );

    let token = TokenResponseX::builder()
        .try_access_token(encode_token(&jwt_settings, &access_token)?)?
        .try_token_type(BEARER)?
        .try_refresh_token(encode_token(&jwt_settings, &refresh_token)?)?
        .try_expires_in(jwt_settings.access_token_expiration)?
        .build()?;
    info!(
        user_id = %session.user_id,
        access_token_id = %session.access_token_id,
        "auth.login: success"
    );
    Ok(token)
}
