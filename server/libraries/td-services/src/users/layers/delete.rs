//
// Copyright 2025 Tabs Data Inc.
//

use crate::users::UserError;
use td_error::TdError;
use td_objects::types::basic::UserId;
use td_objects::types::user::UserDB;
use td_tower::extractors::Input;

pub async fn delete_user_validate(
    Input(user_db): Input<UserDB>,
    Input(user_id): Input<UserId>,
) -> Result<(), TdError> {
    if user_db.id() == &*user_id {
        Err(UserError::NotAllowedToDeleteThemselves)?
    }
    Ok(())
}
