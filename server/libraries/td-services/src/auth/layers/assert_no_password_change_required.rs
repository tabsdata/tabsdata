//
// Copyright 2025 Tabsdata Inc.
//

use crate::auth::AuthError;
use td_error::TdError;
use td_objects::dxo::user::UserDB;
use td_tower::extractors::Ref;

pub async fn assert_no_password_change_required(user: Ref<UserDB>) -> Result<(), TdError> {
    if *user.password_must_change {
        Err(AuthError::UserMustChangePassword)?
    } else {
        Ok(())
    }
}
