//
// Copyright 2025 Tabsdata Inc.
//

use argon2::PasswordHasher;
use argon2::password_hash::SaltString;
use argon2::password_hash::rand_core::OsRng;
use std::ops::Deref;
use td_error::TdError;
use td_objects::types::basic::PasswordHash;
use td_security::config::PasswordHashingConfig;

pub fn create_password_hash<P: Deref<Target = String>>(
    password_hashing_config: &PasswordHashingConfig,
    password: &P,
) -> Result<PasswordHash, TdError> {
    let hash = password_hashing_config
        .password_hasher()
        .hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))
        .unwrap()
        .to_string();

    PasswordHash::try_from(hash)
}
