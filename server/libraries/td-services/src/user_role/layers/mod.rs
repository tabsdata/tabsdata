//
// Copyright 2025 Tabs Data Inc.
//

use crate::user_role::UserRoleError;
use td_error::TdError;
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::UserRoleDBWithNames;
use td_tower::extractors::Ref;

/// Rejects granting a role to a system user (the built-in `system` user and
/// other service users).
pub async fn assert_user_is_not_system(user: Ref<UserDB>) -> Result<(), TdError> {
    if *user.system {
        Err(UserRoleError::CannotGrantRoleToSystemUser(
            user.name.clone(),
        ))?
    }
    Ok(())
}

pub async fn assert_not_fixed(user_role: Ref<UserRoleDBWithNames>) -> Result<(), TdError> {
    if *user_role.fixed {
        Err(UserRoleError::FixedUserRole(
            user_role.user.clone(),
            user_role.role.clone(),
        ))?
    }

    Ok(())
}
