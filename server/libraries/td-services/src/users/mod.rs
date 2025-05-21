//
// Copyright 2024 Tabs Data Inc.
//

use td_error::td_error;

mod layers;
pub mod service;

#[td_error]
pub enum UserError {
    #[error("Password change is not allowed. Use the dedicated password change endpoint")]
    PasswordChangeNotAllowed = 0,
    #[error("You cannot enable or disable your own user account")]
    UserCannotEnableDisableThemselves = 1,
    #[error("The user update request has nothing to update")]
    UpdateRequestHasNothingToUpdate = 2,

    #[error("You cannot delete your own user account")]
    NotAllowedToDeleteThemselves = 2000,
}
