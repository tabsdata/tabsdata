//
// Copyright 2024 Tabs Data Inc.
//

pub mod create;
pub mod delete;
pub mod update;

use crate::user::UserError;
use td_error::TdError;
use td_objects::dxo::user::{UserDB, UserDBWithNames};
use td_tower::extractors::Ref;

/// Rejects mutating a system user (the built-in `system` user and other service
/// users). Used by update/delete and role-grant paths.
pub async fn assert_user_is_not_system(user: Ref<UserDB>) -> Result<(), TdError> {
    if *user.system {
        Err(UserError::SystemUserProtected)?
    }
    Ok(())
}

/// Hides system users from reads: a system user is reported as not found so its
/// existence is not revealed through the get-by-id/name endpoint.
pub async fn assert_user_readable(user: Ref<UserDBWithNames>) -> Result<(), TdError> {
    if *user.system {
        Err(UserError::UserNotFound)?
    }
    Ok(())
}
