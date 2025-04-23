//
// Copyright 2024 Tabs Data Inc.
//

use td_error::td_error;

#[td_error]
pub enum UserError {
    #[error("Password must be at least {0} characters long")]
    InvalidPasswordLength(usize) = 0,
    #[error(
        "An admin cannot force a password change to themselves, but they can change their own password"
    )]
    CannotForcePasswordChangeToSelf = 1,
    #[error("A non admin cannot force a password change")]
    CannotForcePasswordChange = 2,
    #[error("An admin cannot permanently change other user password, but they can force a password change.")]
    CannotChangeOtherUserPassword = 3,
    #[error("Old password is not correct")]
    IncorrectOldPassword = 4,
    #[error("A user cannot enable or disable themselves")]
    UserCannotEnableDisableThemselves = 5,
    #[error("The user update request has nothing to update")]
    UpdateRequestHasNothingToUpdate = 6,
    #[error("User already exists")]
    AlreadyExists = 7,

    #[error("A non admin user cannot update other users")]
    NotAllowedToUpdateOtherUsers = 2000,
    #[error("A user cannot delete themselves")]
    NotAllowedToDeleteThemselves = 2001,

    #[error("User is not enabled")]
    UserNotEnabled = 3000,
    #[error("A non admin user cannot create users")]
    NotAllowedToCreateUsers = 3001,
    #[error("A non admin user cannot delete users")]
    NotAllowedToDeleteUsers = 3002,
    #[error("A non admin user cannot list users")]
    NotAllowedToListUsers = 3003,
    #[error("A non admin user cannot see information of other users")]
    NotAllowedToReadUsers = 3004,

    #[error("Authentication failed")]
    AuthenticationFailed = 4000,

    #[error("Could not fetch user for enabled check, error: {0}")]
    CouldNotFetchUserForEnabledCheck(#[source] sqlx::Error) = 5000,
    #[error("{0}")]
    ShouldNotHappen(String) = 5001,
    #[error("Incorrect password hash, error: {0}")]
    IncorrectPasswordHash(String) = 5002,
}
