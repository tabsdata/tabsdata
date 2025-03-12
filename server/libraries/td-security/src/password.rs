//
// Copyright 2024 Tabs Data Inc.
//

use crate::config::PasswordHashingConfig;
use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::SaltString;
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier};
use td_error::{td_error, TdError};

#[td_error]
pub enum Error {
    #[error("Password must be at least {0} characters")]
    PasswordLengthViolation(usize),
}

// Verifies the password hash.
pub fn verify_password(phc_str: &str, password: &str) -> bool {
    match PasswordHash::new(phc_str) {
        // the values encode in the PHC string are used to configure the verifier
        Ok(parsed_hash) => Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok(),
        Err(_) => false,
    }
}

pub fn assert_password_policy(password: &str) -> Result<(), TdError> {
    const MIN_PASSWORD_LENGTH: usize = 8;
    if password.len() < MIN_PASSWORD_LENGTH {
        Err(Error::PasswordLengthViolation(MIN_PASSWORD_LENGTH))?
    }
    Ok(())
}
pub fn create_password_hash(
    password_hashing_config: &PasswordHashingConfig,
    password: &str,
) -> String {
    // creates a PHC string
    password_hashing_config
        .password_hasher()
        .hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))
        .unwrap()
        .to_string()
}

#[cfg(test)]
mod tests {
    use crate::config::PasswordHashingConfig;
    use crate::password::{create_password_hash, verify_password};

    #[test]
    fn test_password_hash_with_custom_config() {
        let config = PasswordHashingConfig::builder()
            .algorithm("argon2i")
            .version(16usize)
            .memory_cost_mib(18usize)
            .time_cost(1usize)
            .parallelism_cost(2usize)
            .kdf_len(32usize)
            .build()
            .unwrap();
        let password = "password";
        let hash = create_password_hash(&config, password);
        assert!(hash.starts_with("$argon2i$v=16$m=18432,t=1,p=2"));
    }

    #[test]
    fn test_create_password_hash() {
        let config = PasswordHashingConfig::default();
        let password = "password";
        let hash = create_password_hash(&config, password);
        assert!(verify_password(&hash, password));
    }
}
