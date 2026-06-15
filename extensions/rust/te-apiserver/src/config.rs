//
// Copyright 2025 Tabs Data Inc.
//

use clap::Args;
use getset::Getters;
use serde::{Deserialize, Serialize};
use ta_services::factory::FieldAccessors;
use td_database::config::{APISERVER_SCHEMA_KEY, DatabaseDescriptor, SYSTEM_SCHEMA_KEY};
use td_error::TdError;

pub const SCHEMA_KEYS: &[&str] = &[SYSTEM_SCHEMA_KEY, APISERVER_SCHEMA_KEY];

pub fn validate_database(database_descriptor: &DatabaseDescriptor) -> Result<(), TdError> {
    database_descriptor.validate_schema_keys(SCHEMA_KEYS)?;
    Ok(())
}

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
