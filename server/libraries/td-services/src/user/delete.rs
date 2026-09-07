//
// Copyright 2025 Tabsdata Inc.
//

use crate::user::UserError;
use td_error::TdError;
use td_objects::dxo::user::UserDB;
use td_objects::types::basic::UserId;

pub fn delete_user_validate(user_db: &UserDB, user_id: &UserId) -> Result<(), TdError> {
    if *user_db.system {
        Err(UserError::SystemUserProtected)?
    }
    if user_db.id == *user_id {
        Err(UserError::NotAllowedToDeleteThemselves)?
    }
    Ok(())
}
