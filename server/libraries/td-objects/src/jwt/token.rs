//
//  Copyright 2024 Tabs Data Inc.
//

use chrono::Duration;
use getset::Getters;
use serde::{Deserialize, Serialize};
use td_common::id::{id, Id};
use utoipa::ToSchema;

const TOKEN_ISSUER: &str = "tabsdata";

/// JWT Token representation
#[derive(Debug, Eq, PartialEq, Serialize, ToSchema)]
pub struct EncodedToken<T> {
    token: String,

    #[serde(skip)]
    _type: std::marker::PhantomData<T>,
}

impl<T> EncodedToken<T> {
    pub fn new(token: &str) -> Self {
        Self {
            token: token.to_string(),
            _type: std::marker::PhantomData,
        }
    }

    pub fn as_str(&self) -> &str {
        &self.token
    }
}

/// JWT Token Claims, serialized on login and deserialized on request
#[derive(Clone, Debug, Default, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct AccessTokenClaims {
    /// ID
    jti: Id,
    /// Issuer
    iss: String,
    /// Subject (User)
    sub: String,
    /// Role
    role: String,
    /// Expiration time in seconds
    exp: i64,
}

impl AccessTokenClaims {
    pub fn new(sub: String, role: String, exp: Duration) -> Self {
        Self {
            jti: id(),
            iss: TOKEN_ISSUER.to_string(),
            sub,
            role,
            exp: exp.num_seconds(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, Getters)]
#[getset(get = "pub")]
pub struct RefreshTokenClaims {
    /// ID
    jti: Id,
    /// Issuer
    iss: String,
    /// Subject (Access Token ID)
    sub: String,
    /// Expiration time in seconds
    exp: i64,
}

impl RefreshTokenClaims {
    pub fn new(sub: String, exp: Duration) -> Self {
        Self {
            jti: id(),
            iss: TOKEN_ISSUER.to_string(),
            sub,
            exp: exp.num_seconds(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use td_common::time::UniqueUtc;

    #[tokio::test]
    async fn test_jwt_token() {
        let token_str = "test_token".to_string();
        let jwt_token = EncodedToken::<AccessTokenClaims>::new(token_str.as_str());

        assert_eq!(jwt_token.as_str(), token_str);
    }

    #[tokio::test]
    async fn test_claims_new() {
        let user = "test_user".to_string();
        let role = "user".to_string();
        let exp = UniqueUtc::now_millis().timestamp() + Duration::days(1).num_seconds();

        let claims = AccessTokenClaims::new(user.clone(), role.clone(), Duration::seconds(exp));

        assert_eq!(claims.sub(), &user);
        assert_eq!(claims.role(), &role);
        assert_eq!(claims.exp(), &exp);
    }
}
