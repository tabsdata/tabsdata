//
// Copyright 2025 Tabs Data Inc.
//

use async_trait::async_trait;
use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::user::{UserCreate, UserDBBuilder};
use td_objects::tower_service::from::With;
use td_objects::types::basic::AtTime;
use td_security::config::PasswordHashingConfig;
use td_security::password::create_password_hash;
use td_tower::extractors::{Input, SrvCtx};

#[async_trait]
pub trait UpdateCreateUserDBBuilder {
    async fn update_create_user_db_builder(
        password_hashing_config: SrvCtx<PasswordHashingConfig>,
        request_time: Input<AtTime>,
        update: Input<UserCreate>,
        builder: Input<UserDBBuilder>,
    ) -> Result<UserDBBuilder, TdError>;
}

#[async_trait]
impl UpdateCreateUserDBBuilder for With<UserCreate> {
    async fn update_create_user_db_builder(
        SrvCtx(password_hashing_config): SrvCtx<PasswordHashingConfig>,
        Input(request_time): Input<AtTime>,
        Input(create): Input<UserCreate>,
        Input(builder): Input<UserDBBuilder>,
    ) -> Result<UserDBBuilder, TdError> {
        let mut builder = builder.deref().clone();
        builder
            .name(create.name.clone())
            .full_name(create.full_name.clone())
            .email(create.email.clone())
            .try_password_hash(create_password_hash(
                &password_hashing_config,
                create.password.trim(),
            ))?
            .try_password_set_on(&*request_time)?
            .password_must_change(true)
            .enabled(create.enabled.clone());
        Ok(builder)
    }
}
