//
// Copyright 2025 Tabsdata Inc.
//

use crate::auth::AuthError;
use td_error::TdError;
use td_objects::dxo::user::UserDB;

pub fn assert_login_user(user: Option<UserDB>) -> Result<UserDB, TdError> {
    user.ok_or_else(|| AuthError::AuthenticationFailed.into())
}
