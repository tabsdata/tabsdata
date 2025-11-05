//
// Copyright 2025 Tabs Data Inc.
//

use getset::Getters;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Validation, decode, encode};
use serde::{Deserialize, Serialize};
use td_common::id;
use td_common::id::Id;
use td_error::td_error;

#[derive(Clone, Serialize, Deserialize)]
pub struct JwtConfig {
    pub secret: Option<String>,
    pub access_token_expiration: i64,
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
        const EXPIRATION: i64 = 3600;
        JwtConfig::new(id::id().to_string(), EXPIRATION)
    }
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

#[td_error]
pub enum JwtError {
    #[error("Could not encode JWT token: {0}")]
    JwtEncodingError(jsonwebtoken::errors::Error) = 1000,
    #[error("Could not decode JWT token: {0}")]
    JwtDecodingError(jsonwebtoken::errors::Error) = 1001,
}

pub fn encode_token(jwt_settings: &JwtConfig, token: &TokenClaims) -> Result<String, JwtError> {
    encode(
        &jsonwebtoken::Header::default(),
        token,
        jwt_settings.encoding_key(),
    )
    .map_err(JwtError::JwtEncodingError)
}

pub fn decode_token(jwt_settings: &JwtConfig, token: &str) -> Result<TokenClaims, JwtError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 5;
    validation.set_required_spec_claims(&["jti", "exp"]);
    decode::<TokenClaims>(token, jwt_settings.decoding_key(), &jwt_settings.validation)
        .map_err(JwtError::JwtEncodingError)
        .map(|tt| tt.claims)
}
