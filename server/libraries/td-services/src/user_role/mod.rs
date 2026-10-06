//
// Copyright 2025 Tabsdata Inc.
//

use td_error::td_error;
use td_objects::types::basic::{RoleName, UserLabel, UserName};

pub mod services;

#[td_error]
pub enum UserRoleError {
    #[error("The role [{0}] for user [{1}] is a fixed user role and cannot be deleted")]
    FixedUserRole(UserLabel, RoleName) = 0,

    #[error("Roles cannot be granted to system user [{0}]")]
    CannotGrantRoleToSystemUser(UserName) = 2000,
}

use td_error::TdError;
use td_objects::dxo::user::UserDB;
use td_objects::dxo::user_role::UserRoleDBWithNames;

/// Rejects granting a role to a system user (the built-in `system` user and
/// other service users).
pub fn assert_user_is_not_system(user: &UserDB) -> Result<(), TdError> {
    if *user.system {
        Err(UserRoleError::CannotGrantRoleToSystemUser(
            user.name.clone(),
        ))?
    }
    Ok(())
}

pub fn assert_not_fixed(user_role: &UserRoleDBWithNames) -> Result<(), TdError> {
    if *user_role.base.fixed {
        Err(UserRoleError::FixedUserRole(
            user_role.user.clone(),
            user_role.role.clone(),
        ))?
    }

    Ok(())
}
