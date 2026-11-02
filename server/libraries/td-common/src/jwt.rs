//
// Copyright 2026 Tabsdata Inc.
//

use crate::id;
use crate::id::Id;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use td_error::td_error;
use tracing::debug;

/// Default access-token lifetime (seconds), and the multiple applied to it for
/// the refresh-token lifetime when `refresh_token_expiration` is absent from config.
const DEFAULT_ACCESS_TOKEN_EXPIRATION: i64 = 3600;
const DEFAULT_REFRESH_EXPIRATION_FACTOR: i64 = 10;

fn default_refresh_token_expiration() -> i64 {
    DEFAULT_ACCESS_TOKEN_EXPIRATION * DEFAULT_REFRESH_EXPIRATION_FACTOR
}

#[derive(Clone, Serialize, Deserialize)]
pub struct JwtConfig {
    pub secret: Option<String>,
    pub access_token_expiration: i64,
    /// Refresh-token lifetime in seconds. Absent from config it defaults to 10×
    /// the standard access-token lifetime; [`JwtConfig::new`] instead derives it
    /// as 10× the actual `access_token_expiration`.
    #[serde(default = "default_refresh_token_expiration")]
    pub refresh_token_expiration: i64,
    #[serde(skip)]
    encoding_key: Option<EncodingKey>,
    #[serde(skip)]
    decoding_key: Option<DecodingKey>,
    #[serde(skip)]
    pub validation: Validation,
}

impl JwtConfig {
    pub fn new(secret: String, access_token_expiration: i64) -> Self {
        let encoding_key = Some(EncodingKey::from_secret(secret.as_bytes()));
        let decoding_key = Some(DecodingKey::from_secret(secret.as_bytes()));
        let mut validation = Validation::new(Algorithm::HS256);
        validation.leeway = 5;
        validation.set_required_spec_claims(&["jti", "exp"]);

        Self {
            secret: Some(secret),
            access_token_expiration,
            refresh_token_expiration: access_token_expiration * DEFAULT_REFRESH_EXPIRATION_FACTOR,
            encoding_key,
            decoding_key,
            validation,
        }
    }

    pub fn encoding_key(&self) -> &EncodingKey {
        self.encoding_key.as_ref().unwrap()
    }

    pub fn decoding_key(&self) -> &DecodingKey {
        self.decoding_key.as_ref().unwrap()
    }
}

impl Default for JwtConfig {
    fn default() -> Self {
        JwtConfig::new(id::id().to_string(), DEFAULT_ACCESS_TOKEN_EXPIRATION)
    }
}

/// JWT Token Claims, serialized on login and deserialized on request
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TokenClaims {
    /// ID
    pub jti: Id,
    /// Expiration time in seconds
    pub exp: i64,
}

impl TokenClaims {
    pub fn new(jti: impl Into<Id>, exp: i64) -> Self {
        Self {
            jti: jti.into(),
            exp,
        }
    }
}

#[td_error]
pub enum JwtError {
    #[error("Could not encode JWT token: {0}")]
    JwtEncodingError(jsonwebtoken::errors::Error) = 1000,
    #[error("Could not decode JWT token: {0}")]
    JwtDecodingError(jsonwebtoken::errors::Error) = 1001,
    #[error("Internal token has unexpected kind: {0}")]
    InternalTokenKind(String) = 1002,
    #[error("Internal token expired")]
    InternalTokenExpired = 1003,
}

/// Discriminator carried in `InternalTokenClaims.kind`, kept distinct from user
/// `TokenClaims` so the two can't cross-validate (user tokens require `exp`,
/// which internal tokens never carry; internal tokens require `kind`, which
/// user tokens never carry).
pub const INTERNAL_TOKEN_KIND: &str = "internal";

/// Claims for a generic internal-endpoint token. Authorizes one internal
/// endpoint (`scope` = its route pattern) for one set of resource ids
/// (`bindings` = its path-param name → value), under one `method`. The
/// internal router validates these against the actual request. Most carry no
/// expiration, as a worker may call back long after it was scheduled.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalTokenClaims {
    /// Unique token id (audit/logging).
    pub jti: Id,
    /// Discriminator — must equal [`INTERNAL_TOKEN_KIND`].
    pub kind: String,
    /// HTTP method this token authorizes.
    pub method: String,
    /// Route pattern this token authorizes (the matched-path pattern, e.g.
    /// `/api/v1/internal/function-versions/{function_version}/execute`).
    pub scope: String,
    /// Path-param name → value bindings the request must match exactly.
    pub bindings: BTreeMap<String, String>,
    /// Unix seconds after which the token is refused; none never expires. Not
    /// the registered `exp`, which would let it pass as a user token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires: Option<i64>,
}

/// Mint an internal-endpoint token bound to `(method, scope, bindings)`,
/// refused after `expires` (Unix seconds) when one is given.
pub fn encode_internal_token(
    jwt_settings: &JwtConfig,
    method: &str,
    scope: &str,
    bindings: BTreeMap<String, String>,
    expires: Option<i64>,
) -> Result<String, JwtError> {
    let claims = InternalTokenClaims {
        jti: id::id(),
        kind: INTERNAL_TOKEN_KIND.to_string(),
        method: method.to_string(),
        scope: scope.to_string(),
        bindings,
        expires,
    };
    let encoded = encode(
        &jsonwebtoken::Header::default(),
        &claims,
        jwt_settings.encoding_key(),
    )
    .map_err(JwtError::JwtEncodingError)?;
    debug!(jti = %claims.jti, scope = %claims.scope, "auth.internal_token.issued");
    Ok(encoded)
}

/// Verify an internal token's signature, `kind` and `expires`. Binding the
/// claims to the actual request (`method`/`scope`/`bindings`) is the
/// caller's job.
pub fn decode_internal_token(
    jwt_settings: &JwtConfig,
    token: &str,
) -> Result<InternalTokenClaims, JwtError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 5;
    validation.validate_exp = false;
    validation.validate_aud = false;
    // Internal tokens carry no registered spec claims (no exp); require none.
    validation.set_required_spec_claims::<&str>(&[]);
    let claims = decode::<InternalTokenClaims>(token, jwt_settings.decoding_key(), &validation)
        .map_err(JwtError::JwtDecodingError)?
        .claims;
    if claims.kind != INTERNAL_TOKEN_KIND {
        return Err(JwtError::InternalTokenKind(claims.kind));
    }
    let now = chrono::Utc::now().timestamp();
    if claims
        .expires
        .is_some_and(|expires| expires + (validation.leeway as i64) < now)
    {
        return Err(JwtError::InternalTokenExpired);
    }
    Ok(claims)
}

pub fn encode_token(jwt_settings: &JwtConfig, token: &TokenClaims) -> Result<String, JwtError> {
    let encoded = encode(
        &jsonwebtoken::Header::default(),
        token,
        jwt_settings.encoding_key(),
    )
    .map_err(JwtError::JwtEncodingError)?;
    debug!(jti = %token.jti, exp = token.exp, "auth.token.issued");
    Ok(encoded)
}

pub fn decode_token(jwt_settings: &JwtConfig, token: &str) -> Result<TokenClaims, JwtError> {
    decode::<TokenClaims>(token, jwt_settings.decoding_key(), &jwt_settings.validation)
        .map_err(JwtError::JwtDecodingError)
        .map(|tt| tt.claims)
}

#[cfg(test)]
mod internal_token_tests {
    use super::*;

    fn config() -> JwtConfig {
        JwtConfig::new("test-secret".to_string(), 3600)
    }

    fn bindings(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn round_trip() {
        let jwt = config();
        let scope = "/api/v1/internal/function-versions/{function_version}/execute";
        let token = encode_internal_token(
            &jwt,
            "POST",
            scope,
            bindings(&[("function_version", "FV")]),
            None,
        )
        .unwrap();
        let claims = decode_internal_token(&jwt, &token).unwrap();
        assert_eq!(claims.kind, INTERNAL_TOKEN_KIND);
        assert_eq!(claims.method, "POST");
        assert_eq!(claims.scope, scope);
        assert_eq!(claims.bindings.get("function_version").unwrap(), "FV");
    }

    #[test]
    fn rejects_wrong_signature() {
        let token =
            encode_internal_token(&config(), "GET", "/s", bindings(&[("worker", "W")]), None)
                .unwrap();
        let other = JwtConfig::new("different-secret".to_string(), 3600);
        assert!(decode_internal_token(&other, &token).is_err());
    }

    #[test]
    fn rejects_user_token() {
        // A user token (jti + exp, no `kind`) must not verify as internal.
        let jwt = config();
        let user = encode_token(&jwt, &TokenClaims::new(id::id(), 9_999_999_999)).unwrap();
        assert!(decode_internal_token(&jwt, &user).is_err());
    }

    #[test]
    fn user_decoder_rejects_internal_token() {
        // And an internal token (no `exp`) must not verify as a user token.
        let jwt = config();
        let internal =
            encode_internal_token(&jwt, "GET", "/s", bindings(&[("worker", "W")]), None).unwrap();
        assert!(decode_token(&jwt, &internal).is_err());
        let expiring =
            encode_internal_token(&jwt, "GET", "/s", BTreeMap::new(), Some(i64::MAX)).unwrap();
        assert!(decode_token(&jwt, &expiring).is_err());
    }

    #[test]
    fn an_expiring_token_decodes_until_it_expires() {
        let jwt = config();
        let now = chrono::Utc::now().timestamp();
        let live =
            encode_internal_token(&jwt, "GET", "/s", BTreeMap::new(), Some(now + 60)).unwrap();
        assert_eq!(
            decode_internal_token(&jwt, &live).unwrap().expires,
            Some(now + 60)
        );
        let expired =
            encode_internal_token(&jwt, "GET", "/s", BTreeMap::new(), Some(now - 60)).unwrap();
        assert!(matches!(
            decode_internal_token(&jwt, &expired),
            Err(JwtError::InternalTokenExpired)
        ));
    }
}
