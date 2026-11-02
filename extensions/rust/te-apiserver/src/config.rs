//
// Copyright 2025 Tabsdata Inc.
//

use clap::Args;
use getset::Getters;
use serde::{Deserialize, Serialize};
use ta_services::factory::FieldAccessors;
use td_error::TdError;
use td_objects::types::basic::ServiceToken;

#[derive(Clone, Default, Serialize, Deserialize, FieldAccessors)]
pub struct ExtendedConfig {}

impl ExtendedConfig {
    /// The tokens the server seeds as system tokens at boot, if any.
    pub fn seeded_tokens(&self) -> Option<&ServiceToken> {
        None
    }
}

#[derive(Debug, Default, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct ExtendedParams {}

impl ExtendedParams {
    pub fn resolve(&self, _config: ExtendedConfig) -> Result<ExtendedConfig, TdError> {
        let config = ExtendedConfig {};
        Ok(config)
    }
}
