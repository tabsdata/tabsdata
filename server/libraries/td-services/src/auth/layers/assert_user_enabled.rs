//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::AuthError;
use td_error::TdError;
use td_objects::dxo::user::UserDB;
use td_tower::extractors::Ref;

pub async fn assert_user_enabled(user: Ref<UserDB>) -> Result<(), TdError> {
    if *user.enabled {
        Ok(())
    } else {
        Err(AuthError::UserDisabled)?
    }
}
