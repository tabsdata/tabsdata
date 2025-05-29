//
// Copyright 2025 Tabs Data Inc.
//

use crate::user::UserError;
use async_trait::async_trait;
use std::ops::Deref;
use td_error::TdError;
use td_objects::tower_service::from::With;
use td_objects::types::basic::{AtTime, UserId};
use td_objects::types::user::{UserDB, UserUpdate, UserUpdateDBBuilder};
use td_security::config::PasswordHashingConfig;
use td_security::password::create_password_hash;
use td_tower::extractors::{Input, SrvCtx};

pub async fn update_user_validate(Input(update): Input<UserUpdate>) -> Result<(), TdError> {
    if update.full_name().is_none()
        && update.email().is_none()
        && update.password().is_none()
        && update.enabled().is_none()
    {
        return Err(UserError::UpdateRequestHasNothingToUpdate)?;
    }
    Ok(())
}

pub async fn update_user_validate_password_change(
    Input(request_user_id): Input<UserId>,
    Input(update): Input<UserUpdate>,
    Input(user_db): Input<UserDB>,
) -> Result<(), TdError> {
    #[allow(clippy::collapsible_if)]
    if update.password().is_some() {
        if &*request_user_id == user_db.id() {
            // a self password change must be done via de password_change endpoint
            return Err(UserError::PasswordChangeNotAllowed)?;
        }
        // only a sec_admin can make it here without being the requester
    }
    Ok(())
}

pub async fn update_user_validate_enabled(
    Input(request_user_id): Input<UserId>,
    Input(user_db): Input<UserDB>,
    Input(update): Input<UserUpdate>,
) -> Result<(), TdError> {
    if update.enabled().is_some() && &*request_user_id == user_db.id() {
        return Err(UserError::UserCannotEnableDisableThemselves)?;
    }
    Ok(())
}

#[async_trait]
pub trait UpdateUserDBBuilderUpdate {
    async fn update_user_update_db_builder(
        password_hashing_config: SrvCtx<PasswordHashingConfig>,
        request_time: Input<AtTime>,
        update: Input<UserUpdate>,
        builder: Input<UserUpdateDBBuilder>,
    ) -> Result<UserUpdateDBBuilder, TdError>;
}

#[async_trait]
impl UpdateUserDBBuilderUpdate for With<UserUpdate> {
    async fn update_user_update_db_builder(
        SrvCtx(password_hashing_config): SrvCtx<PasswordHashingConfig>,
        Input(request_time): Input<AtTime>,
        Input(update): Input<UserUpdate>,
        Input(builder): Input<UserUpdateDBBuilder>,
    ) -> Result<UserUpdateDBBuilder, TdError> {
        let mut builder = builder.deref().clone();
        if update.full_name().is_some() {
            builder.full_name(update.full_name().as_ref().unwrap());
        }
        if update.email().is_some() {
            builder.email(update.email().as_ref().cloned());
        }
        if let Some(password) = update.password() {
            builder.try_password_hash(create_password_hash(
                &password_hashing_config,
                password.trim(),
            ))?;
            builder.try_password_set_on(&*request_time)?;
            builder.password_must_change(true);
        };
        if update.enabled().is_some() {
            builder.enabled(update.enabled().as_ref().unwrap());
        }

        Ok(builder)
    }
}
