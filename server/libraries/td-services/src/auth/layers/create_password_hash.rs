//
// Copyright 2025. Tabs Data Inc.
//

use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::SaltString;
use argon2::PasswordHasher;
use std::ops::Deref;
use td_error::TdError;
use td_objects::types::basic::PasswordHash;
use td_security::config::PasswordHashingConfig;
use td_tower::extractors::{Input, SrvCtx};

pub async fn create_password_hash<P: Deref<Target = String>>(
    SrvCtx(password_hashing_config): SrvCtx<PasswordHashingConfig>,
    Input(password): Input<P>,
) -> Result<PasswordHash, TdError> {
    let hash = password_hashing_config
        .password_hasher()
        .hash_password(
            password.deref().as_bytes(),
            &SaltString::generate(&mut OsRng),
        )
        .unwrap()
        .to_string();

    PasswordHash::try_from(hash)
}
