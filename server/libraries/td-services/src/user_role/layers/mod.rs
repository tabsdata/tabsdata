//
// Copyright 2025 Tabs Data Inc.
//

use crate::user_role::UserRoleError;
use td_error::TdError;
use td_objects::dxo::user_role::defs::UserRoleDBWithNames;
use td_tower::extractors::Input;

pub async fn assert_not_fixed(Input(user_role): Input<UserRoleDBWithNames>) -> Result<(), TdError> {
    if *user_role.fixed {
        Err(UserRoleError::FixedUserRole(
            user_role.user.clone(),
            user_role.role.clone(),
        ))?
    }

    Ok(())
}
