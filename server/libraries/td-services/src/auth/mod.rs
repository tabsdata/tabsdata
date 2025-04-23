//
// Copyright 2025. Tabs Data Inc.
//

pub mod layers;
pub mod services;
pub mod session;

use crate::auth::services::JwtConfig;
use getset::Getters;
use jsonwebtoken::{decode, encode, Algorithm, Validation};
use serde::{Deserialize, Serialize};
use td_common::id::Id;
use td_error::td_error;
use td_objects::types::basic::RoleId;

#[td_error]
pub enum AuthError {
    #[error("Could not encode JWT token: {0}")]
    JwtEncodingError(jsonwebtoken::errors::Error) = 1000,
    #[error("Could not decode JWT token: {0}")]
    JwtDecodingError(jsonwebtoken::errors::Error) = 1001,

    #[error("Authentication failed")]
    AuthenticationFailed = 4000,
    #[error("Missing Authorization Header")]
    MissingAuthorizationHeader = 4001,
    #[error("Invalid Authorization Header value: {0}")]
    InvalidAuthorizationHeaderValue(String) = 4002,
    #[error("User disabled")]
    UserDisabled = 4003,
    #[error("Invalid refresh token")]
    InvalidRefreshToken = 4004,
    #[error("User must change password")]
    UserMustChangePassword = 4005,

    #[error("Internal error: {0}")]
    InternalError(String) = 5000,

    #[error("RoleId not found: {0}")]
    RoleIdNotFound(RoleId) = 5001,
}

/// JWT Token Claims, serialized on login and deserialized on request
#[derive(Clone, Debug, Default, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct TokenClaims {
    /// ID
    jti: Id,
    /// Expiration time in seconds
    exp: i64,
}

impl TokenClaims {
    pub fn new(jti: impl Into<Id>, exp: i64) -> Self {
        Self {
            jti: jti.into(),
            exp,
        }
    }
}

fn encode_token(jwt_settings: &JwtConfig, token: &TokenClaims) -> Result<String, AuthError> {
    encode(
        &jsonwebtoken::Header::default(),
        token,
        jwt_settings.encoding_key(),
    )
    .map_err(AuthError::JwtEncodingError)
}

pub fn decode_token(jwt_settings: &JwtConfig, token: &str) -> Result<TokenClaims, AuthError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 5;
    validation.set_required_spec_claims(&["jti", "exp"]);
    decode::<TokenClaims>(
        token,
        jwt_settings.decoding_key(),
        jwt_settings.validation(),
    )
    .map_err(AuthError::JwtEncodingError)
    .map(|tt| tt.claims)
}