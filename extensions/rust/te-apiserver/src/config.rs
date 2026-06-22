//
// Copyright 2025 Tabs Data Inc.
//

use clap::Args;
use getset::Getters;
use serde::{Deserialize, Serialize};
use ta_services::factory::FieldAccessors;
use td_error::TdError;

#[derive(Clone, Default, Serialize, Deserialize, FieldAccessors)]
pub struct ExtendedConfig {}

#[derive(Debug, Default, Clone, Getters, Args)]
#[getset(get = "pub")]
pub struct ExtendedParams {}

impl ExtendedParams {
    pub fn resolve(&self, _config: ExtendedConfig) -> Result<ExtendedConfig, TdError> {
        let config = ExtendedConfig {};
        Ok(config)
    }
}
