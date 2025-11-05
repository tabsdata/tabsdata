//
// Copyright 2025. Tabs Data Inc.
//

use crate::auth::AuthError;
use td_error::TdError;
use td_objects::dxo::user::defs::UserDB;
use td_tower::extractors::Input;

pub async fn assert_no_password_change_required(Input(user): Input<UserDB>) -> Result<(), TdError> {
    if *user.password_must_change {
        Err(AuthError::UserMustChangePassword)?
    } else {
        Ok(())
    }
}
