//
// Copyright 2025 Tabsdata Inc.
//

pub mod services;

use td_error::td_error;

#[td_error]
pub enum AuthError {
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

    #[error("User does not belong the specified role")]
    UserDoesNotBelongToRole = 4006,

    #[error("Internal error: {0}")]
    InternalError(String) = 5000,
}

pub mod assert_current_password;
pub mod assert_user_enabled;
pub mod create_access_token;
pub mod set_session_expiration;
