//
// Copyright 2025 Tabsdata Inc.
//

use crate::auth::AuthError;
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::user::UserDB;
use td_tower::extractors::Ref;

pub async fn assert_login_user(user: Ref<Option<UserDB>>) -> Result<UserDB, TdError> {
    user.deref()
        .clone()
        .ok_or_else(|| AuthError::AuthenticationFailed.into())
}
