//
// Copyright 2025 Tabs Data Inc.
//

use std::ops::Deref;
use td_error::TdError;
use td_objects::dxo::user::{UserCreate, UserDBBuilder};
use td_objects::tower_service::with::With;
use td_objects::types::basic::AtTime;
use td_security::config::PasswordHashingConfig;
use td_security::password::create_password_hash;
use td_tower::extractors::Ref;

pub trait UpdateCreateUserDBBuilder {
    fn update_create_user_db_builder(
        password_hashing_config: Ref<PasswordHashingConfig>,
        request_time: Ref<AtTime>,
        update: Ref<UserCreate>,
        builder: Ref<UserDBBuilder>,
    ) -> impl Future<Output = Result<UserDBBuilder, TdError>>;
}

impl UpdateCreateUserDBBuilder for With<UserCreate> {
    async fn update_create_user_db_builder(
        password_hashing_config: Ref<PasswordHashingConfig>,
        request_time: Ref<AtTime>,
        create: Ref<UserCreate>,
        builder: Ref<UserDBBuilder>,
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
            .enabled(create.enabled);
        Ok(builder)
    }
}
