//
// Copyright 2025 Tabs Data Inc.
//

use argon2::{Algorithm, Argon2, Params, PasswordHasher, Version};
use derive_builder::Builder;
use getset::Getters;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Users password hashing configuration.
#[derive(Debug, Clone, Deserialize, Serialize, Getters, Builder)]
#[builder(setter(into), default)]
#[getset(get = "pub")]
pub struct PasswordHashingConfig {
    algorithm: String,
    version: usize,
    memory_cost_mib: usize,
    time_cost: usize,
    parallelism_cost: usize,
    kdf_len: usize,
}

impl PasswordHashingConfig {
    /// Returns a [`PasswordHashingConfig`] builder with default values.
    pub fn builder() -> PasswordHashingConfigBuilder {
        PasswordHashingConfigBuilder::default()
    }
}

impl Default for PasswordHashingConfig {
    fn default() -> Self {
        PasswordHashingConfig {
            algorithm: String::from("argon2id"),
            version: 19, // V0x13
            memory_cost_mib: 19,
            time_cost: 2,
            parallelism_cost: 1,
            kdf_len: 32,
        }
    }
}

impl PasswordHashingConfig {
    pub fn password_hasher(&self) -> impl PasswordHasher {
        Argon2::new(
            Algorithm::from_str(&self.algorithm)
                .expect("Invalid configuration: unknown password hashing algorithm. Valid values: argon2d, argon2i, argon2id (default)"),
            Version::try_from(self.version as u32)
                .expect("Invalid configuration: unknown password hashing version. Valid values: 16, 19 (default)"),
            Params::new(
                (self.memory_cost_mib * 1024) as u32,
                self.time_cost as u32,
                self.parallelism_cost as u32,
                None,
            )
            .unwrap(),
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::config::PasswordHashingConfig;

    #[test]
    fn test_password_hashing_config_default() {
        let config = PasswordHashingConfig::default();
        assert_eq!(config.algorithm(), "argon2id");
        assert_eq!(*config.version(), 19);
        assert_eq!(*config.memory_cost_mib(), 19);
        assert_eq!(*config.time_cost(), 2);
        assert_eq!(*config.parallelism_cost(), 1);
        assert_eq!(*config.kdf_len(), 32);
    }
}
