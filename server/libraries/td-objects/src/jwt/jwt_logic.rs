//
// Copyright 2025 Tabs Data Inc.
//

use crate::jwt::token::{AccessTokenClaims, EncodedToken, RefreshTokenClaims};
use chrono::Duration;
use getset::Getters;
use jsonwebtoken::{decode, encode, get_current_timestamp, DecodingKey, EncodingKey, Validation};
use serde::{Deserialize, Serialize};
use td_apiforge::apiserver_schema;
use td_error::td_error;

const BEARER_TOKEN_TYPE: &str = "Bearer";

/// Login request information needed to obtain a new JWT token.
#[apiserver_schema]
#[derive(Deserialize, Getters)]
#[getset(get = "pub")]
pub struct AccessRequest {
    #[serde(alias = "sub")]
    name: String,
    #[serde(alias = "pwd")]
    password: String,
}

/// Refresh request information needed to obtain a new JWT token.
#[apiserver_schema]
#[derive(Deserialize, Getters)]
#[getset(get = "pub")]
pub struct RefreshRequest {
    refresh_token: String,
}

/// Response when valid access happens.
/// This response follows the standard:
///```json
/// {
///     "access_token": "encoded_access_token",
///     "refresh_token": "encoded_refresh_token",
///     "token_type": "Bearer",
///     "expires_in": 3600
/// }
/// ```
#[apiserver_schema]
#[derive(Serialize)]
pub struct TokenResponse {
    access_token: String,
    refresh_token: String,
    token_type: String,
    expires_in: i64,
}

impl TokenResponse {
    fn new(
        access_token: EncodedToken<AccessTokenClaims>,
        refresh_token: EncodedToken<RefreshTokenClaims>,
        expires_in: Duration,
    ) -> Self {
        Self {
            access_token: access_token.as_str().to_string(),
            refresh_token: refresh_token.as_str().to_string(),
            token_type: String::from(BEARER_TOKEN_TYPE),
            expires_in: expires_in.num_seconds(),
        }
    }
}

#[td_error]
pub enum TokenError {
    #[error("Invalid token: {0}")]
    InvalidToken(String) = 0,
    #[error("Error creating token: {0}")]
    TokenCreation(String) = 5000,
}

/// `JwtLogic` is responsible for handling JWT token creation, encoding, decoding, and validation.
/// It provides methods to authorize access, authenticate access tokens, and authenticate refresh
/// tokens.
pub struct JwtLogic {
    encoding: EncodingKey,
    decoding: DecodingKey,
    access_exp: Duration,
    refresh_exp: Duration,
    token_validation: Validation,
}

impl JwtLogic {
    /// Creates a new instance of `JwtLogic`.
    ///
    /// # Arguments
    ///
    /// * `secret` - A string slice that holds the secret key used for encoding and decoding tokens.
    /// * `access_exp` - The expiration time for access tokens in seconds.
    /// * `refresh_exp` - The expiration time for refresh tokens in seconds.
    pub fn new(secret: &str, access_exp: Duration, refresh_exp: Duration) -> Self {
        Self {
            encoding: EncodingKey::from_secret(secret.as_bytes()),
            decoding: DecodingKey::from_secret(secret.as_bytes()),
            access_exp,
            refresh_exp,
            token_validation: Validation::default(),
        }
    }

    /// Authorizes access by creating a new access token and refresh token.
    ///
    /// # Arguments
    ///
    /// * `sub` - The subject (user identifier) for the token.
    /// * `role` - The role of the user.
    pub fn authorize_access(&self, sub: &str, role: &str) -> Result<TokenResponse, TokenError> {
        let access_token = self.new_access_token(sub, role)?;
        let refresh_token = self.new_refresh_token(&access_token)?;
        self.new_response(access_token, refresh_token)
    }

    /// Authenticates an access token.
    pub fn authenticate_access(
        &self,
        access_token: EncodedToken<AccessTokenClaims>,
    ) -> Result<AccessTokenClaims, TokenError> {
        self.decode::<AccessTokenClaims>(access_token)
    }

    /// Authenticates a refresh token.
    pub fn authenticate_refresh(
        &self,
        refresh_token: EncodedToken<RefreshTokenClaims>,
    ) -> Result<RefreshTokenClaims, TokenError> {
        self.decode::<RefreshTokenClaims>(refresh_token)
    }

    /// Creates a new response containing the access token and refresh token.
    fn new_response(
        &self,
        access_token: AccessTokenClaims,
        refresh_token: RefreshTokenClaims,
    ) -> Result<TokenResponse, TokenError> {
        let encoded_access_token = self.encode::<AccessTokenClaims>(access_token)?;
        let encoded_refresh_token = self.encode::<RefreshTokenClaims>(refresh_token)?;
        Ok(TokenResponse::new(
            encoded_access_token,
            encoded_refresh_token,
            self.access_exp,
        ))
    }

    /// Creates a new access token.
    fn new_access_token(&self, sub: &str, role: &str) -> Result<AccessTokenClaims, TokenError> {
        Ok(AccessTokenClaims::new(
            sub.to_string(),
            role.to_string(),
            Self::expire_in(self.access_exp),
        ))
    }

    /// Creates a new refresh token.
    fn new_refresh_token(&self, sub: &AccessTokenClaims) -> Result<RefreshTokenClaims, TokenError> {
        Ok(RefreshTokenClaims::new(
            sub.jti().to_string(),
            Self::expire_in(self.refresh_exp),
        ))
    }

    /// Calculates the expiration time for a token, in seconds.
    fn expire_in(exp: Duration) -> Duration {
        // use the same baseline as jsonwebtoken decoding
        let seconds = get_current_timestamp() as i64 + exp.num_seconds();
        Duration::seconds(seconds)
    }

    /// Encodes a token.
    fn encode<T: Serialize>(&self, token: T) -> Result<EncodedToken<T>, TokenError> {
        let header = jsonwebtoken::Header::default();
        match encode(&header, &token, &self.encoding) {
            Ok(token) => Ok(EncodedToken::new(token.as_str())),
            Err(e) => Err(TokenError::TokenCreation(e.to_string())),
        }
    }

    /// Decodes a token.
    fn decode<T: for<'de> Deserialize<'de>>(
        &self,
        token: EncodedToken<T>,
    ) -> Result<T, TokenError> {
        match decode::<T>(token.as_str(), &self.decoding, &self.token_validation) {
            Ok(token_data) => Ok(token_data.claims),
            Err(error) => Err(TokenError::InvalidToken(error.to_string())),
        }
    }
}

#[cfg(test)]
pub mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    pub fn test_jwt_logic() -> JwtLogic {
        // This is unsafe, it is only used for testing purposes.
        JwtLogic::new("SECRET", Duration::seconds(3600), Duration::seconds(7200))
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestClaims {
        sub: String,
        exp: u64,
    }

    #[tokio::test]
    async fn test_authorize_access() {
        let jwt_logic = test_jwt_logic();
        let result = jwt_logic.authorize_access("test_user", "test_role");
        assert!(result.is_ok());
        let token_response = result.unwrap();
        assert_eq!(token_response.token_type, "Bearer");
        assert_eq!(token_response.expires_in, 3600);
    }

    #[tokio::test]
    async fn test_authenticate_access() {
        let jwt_logic = test_jwt_logic();
        let access_token = jwt_logic
            .new_access_token("test_user", "test_role")
            .unwrap();
        let encoded_access_token = jwt_logic.encode(access_token.clone()).unwrap();
        let result = jwt_logic.authenticate_access(encoded_access_token);
        assert!(result.is_ok());
        let claims = result.unwrap();
        assert_eq!(claims.sub(), "test_user");
        assert_eq!(claims.role(), "test_role");
    }

    #[tokio::test]
    async fn test_authenticate_access_error() {
        let jwt_logic = test_jwt_logic();
        let result = jwt_logic.authenticate_access(EncodedToken::new("invalid_token"));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_authenticate_refresh() {
        let jwt_logic = test_jwt_logic();
        let access_token = jwt_logic
            .new_access_token("test_user", "test_role")
            .unwrap();
        let refresh_token = jwt_logic.new_refresh_token(&access_token).unwrap();
        let encoded_refresh_token = jwt_logic.encode(refresh_token.clone()).unwrap();
        let result = jwt_logic.authenticate_refresh(encoded_refresh_token);
        assert!(result.is_ok());
        let claims = result.unwrap();
        assert_eq!(claims.jti(), refresh_token.jti());
    }

    #[tokio::test]
    async fn test_authenticate_refresh_error() {
        let jwt_logic = test_jwt_logic();
        let result = jwt_logic.authenticate_refresh(EncodedToken::new("invalid_token"));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_expire_in() {
        let result = JwtLogic::expire_in(Duration::seconds(3600));
        assert!(result > Duration::seconds(get_current_timestamp() as i64 + 3599));
    }

    #[tokio::test]
    async fn test_encode_decode() {
        let jwt_logic = test_jwt_logic();
        let claims = TestClaims {
            sub: "test_user".to_string(),
            exp: get_current_timestamp(),
        };
        let encoded_token = jwt_logic.encode(claims.clone()).unwrap();
        let decoded_claims: TestClaims = jwt_logic.decode(encoded_token).unwrap();
        assert_eq!(claims.sub, decoded_claims.sub);
        assert_eq!(claims.exp, decoded_claims.exp);
    }

    #[tokio::test]
    async fn test_decode_error() {
        let jwt_logic = test_jwt_logic();
        let result: Result<TestClaims, _> = jwt_logic.decode(EncodedToken::new("invalid_token"));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_token_expiration() {
        let mut validation = Validation::default();
        // Leeway is set to 0 to make tokens expire as soon as the expiration is reached
        validation.leeway = 0;
        let jwt_logic = JwtLogic {
            encoding: EncodingKey::from_secret("secret".as_bytes()),
            decoding: DecodingKey::from_secret("secret".as_bytes()),
            access_exp: Duration::seconds(1),
            refresh_exp: Duration::seconds(2),
            token_validation: validation,
        };
        let token_response = jwt_logic.authorize_access("test_user", "test_role");
        assert!(token_response.is_ok());
        let token_response = token_response.unwrap();
        assert_eq!(token_response.expires_in, 1);

        // Wait for the access token to expire
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let encoded_access = EncodedToken::new(token_response.access_token.as_str());
        let access_token = jwt_logic.authenticate_access(encoded_access);
        assert!(access_token.is_err());
        let error = access_token.err().unwrap();
        assert!(matches!(error, TokenError::InvalidToken(_)));
        assert_eq!(error.to_string(), "Invalid token: ExpiredSignature");

        // Wait for the refresh token to expire
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let encoded_refresh = EncodedToken::new(token_response.refresh_token.as_str());
        let access_token = jwt_logic.authenticate_refresh(encoded_refresh);
        assert!(access_token.is_err());
        let error = access_token.err().unwrap();
        assert!(matches!(error, TokenError::InvalidToken(_)));
        assert_eq!(error.to_string(), "Invalid token: ExpiredSignature");
    }
}
