//
// Copyright 2025 Tabs Data Inc.
//

use crate::user::UserError;
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::user::{UserDB, UserUpdate, UserUpdateDBBuilder};
use td_objects::tower_service::with::With;
use td_objects::types::basic::{AtTime, UserId};
use td_security::config::PasswordHashingConfig;
use td_security::password::create_password_hash;
use td_tower::extractors::Ref;

pub async fn update_user_validate(update: Ref<UserUpdate>) -> Result<(), TdError> {
    if update.full_name.is_none()
        && update.email.is_none()
        && update.password.is_none()
        && update.enabled.is_none()
    {
        return Err(UserError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
}

pub async fn update_user_validate_password_change(
    request_user_id: Ref<UserId>,
    update: Ref<UserUpdate>,
    user_db: Ref<UserDB>,
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

pub async fn update_user_validate_enabled(
    request_user_id: Ref<UserId>,
    user_db: Ref<UserDB>,
    update: Ref<UserUpdate>,
) -> Result<(), TdError> {
    if update.enabled.is_some() && *request_user_id == user_db.id {
        return Err(UserError::UserCannotEnableDisableThemselves)?;
    }
    Ok(())
}

pub trait UpdateUserDBBuilderUpdate {
    fn update_user_update_db_builder(
        password_hashing_config: Ref<PasswordHashingConfig>,
        request_time: Ref<AtTime>,
        update: Ref<UserUpdate>,
        builder: Ref<UserUpdateDBBuilder>,
    ) -> impl Future<Output = Result<UserUpdateDBBuilder, TdError>>;
}

impl UpdateUserDBBuilderUpdate for With<UserUpdate> {
    async fn update_user_update_db_builder(
        password_hashing_config: Ref<PasswordHashingConfig>,
        request_time: Ref<AtTime>,
        update: Ref<UserUpdate>,
        builder: Ref<UserUpdateDBBuilder>,
    ) -> Result<UserUpdateDBBuilder, TdError> {
        let mut builder = builder.deref().clone();
        if let Some(full_name) = &update.full_name {
            builder.full_name(full_name);
        }
        if let Some(email) = &update.email {
            builder.email(email.clone());
        }
        if let Some(password) = &update.password {
            builder.try_password_hash(create_password_hash(
                &password_hashing_config,
                password.trim(),
            ))?;
            builder.try_password_set_on(&*request_time)?;
            builder.password_must_change(true);
        };
        if let Some(enabled) = &update.enabled {
            builder.enabled(enabled);
        }

        Ok(builder)
    }
}
