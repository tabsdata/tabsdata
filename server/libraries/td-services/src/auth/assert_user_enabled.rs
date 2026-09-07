//
// Copyright 2025 Tabsdata Inc.
//

use crate::auth::AuthError;
use td_error::TdError;
use td_objects::dxo::user::UserDB;

pub fn assert_user_enabled(user: &UserDB) -> Result<(), TdError> {
    if *user.enabled {
        Ok(())
    } else {
        Err(AuthError::UserDisabled)?
    }
}
