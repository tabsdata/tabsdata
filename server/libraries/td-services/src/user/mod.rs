//
// Copyright 2024 Tabsdata Inc.
//

use td_error::td_error;

pub mod services;

#[td_error]
pub enum UserError {
    #[error("Password change is not allowed. Use the dedicated password change endpoint")]
    PasswordChangeNotAllowed = 0,
    #[error("You cannot enable or disable your own user account")]
    UserCannotEnableDisableThemselves = 1,
    #[error("The user update request has nothing to update")]
    UpdateRequestHasNothingToUpdate = 2,

    // 404 range: system users are hidden — reported as not found on read.
    #[error("User not found")]
    UserNotFound = 1000,

    #[error("You cannot delete your own user account")]
    NotAllowedToDeleteThemselves = 2000,
    #[error("System users cannot be modified or deleted")]
    SystemUserProtected = 2001,
}

pub mod delete;
pub mod update;

use td_error::TdError;
use td_objects::dxo::user::{UserDB, UserDBWithNames};

/// Rejects mutating a system user (the built-in `system` user and other service
/// users). Used by update/delete and role-grant paths.
pub fn assert_user_is_not_system(user: &UserDB) -> Result<(), TdError> {
    if *user.system {
        Err(UserError::SystemUserProtected)?
    }
    Ok(())
}

/// Hides system users from reads: a system user is reported as not found so its
/// existence is not revealed through the get-by-id/name endpoint.
pub fn assert_user_readable(user: &UserDBWithNames) -> Result<(), TdError> {
    if *user.base.system {
        Err(UserError::UserNotFound)?
    }
    Ok(())
}
