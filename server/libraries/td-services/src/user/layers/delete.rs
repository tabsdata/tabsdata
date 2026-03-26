//
// Copyright 2025 Tabs Data Inc.
//

use crate::user::UserError;
use td_error::TdError;
use td_objects::dxo::user::UserDB;
use td_objects::types::basic::UserId;
use td_tower::extractors::Ref;

pub async fn delete_user_validate(
    user_db: Ref<UserDB>,
    user_id: Ref<UserId>,
) -> Result<(), TdError> {
    if user_db.id == *user_id {
        Err(UserError::NotAllowedToDeleteThemselves)?
    }
    Ok(())
}
