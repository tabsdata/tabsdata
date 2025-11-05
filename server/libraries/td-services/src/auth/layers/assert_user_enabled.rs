//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::AuthError;
use td_error::TdError;
use td_objects::dxo::user::defs::UserDB;
use td_tower::extractors::Input;

pub async fn assert_user_enabled(Input(user): Input<UserDB>) -> Result<(), TdError> {
    if *user.enabled {
        Ok(())
    } else {
        Err(AuthError::UserDisabled)?
    }
}
