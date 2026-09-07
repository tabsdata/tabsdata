//
// Copyright 2025 Tabsdata Inc.
//

use crate::user::UserError;
use td_error::TdError;
use td_objects::dxo::user::{UserDB, UserUpdate};
use td_objects::types::basic::UserId;

pub fn update_user_validate(update: &UserUpdate) -> Result<(), TdError> {
    if update.full_name.is_none()
        && update.email.is_none()
        && update.password.is_none()
        && update.enabled.is_none()
    {
        return Err(UserError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
}

pub fn update_user_validate_password_change(
    request_user_id: &UserId,
    update: &UserUpdate,
    user_db: &UserDB,
) -> Result<(), TdError> {
    #[allow(clippy::collapsible_if)]
    if update.password.is_some() {
        if *request_user_id == user_db.id {
            // a self password change must be done via de password_change endpoint
            return Err(UserError::PasswordChangeNotAllowed)?;
        }
        // only a manage_access holder can make it here without being the requester
    }
    Ok(())
}

pub fn update_user_validate_enabled(
    request_user_id: &UserId,
    user_db: &UserDB,
    update: &UserUpdate,
) -> Result<(), TdError> {
    if update.enabled.is_some() && *request_user_id == user_db.id {
        return Err(UserError::UserCannotEnableDisableThemselves)?;
    }
    Ok(())
}
