//
// Copyright 2025 Tabs Data Inc.
//

use crate::dxo::request::v2::{FunctionInputV2, FunctionOutputV2};
use serde::{Deserialize, Serialize};
use serde_yaml::Value;
use std::collections::HashSet;
use url::Url;

pub mod v2;

#[td_type::typed(string)]
pub struct EnvPrefix;

#[td_type::Dto]
#[derive(Eq, PartialEq)]
pub struct Location {
    pub uri: Url,
    #[builder(default)]
    pub env_prefix: Option<EnvPrefix>,
}

pub trait Locations {
    fn locations(&self) -> Vec<&Location>;
}

impl<T: Locations> Locations for Vec<T> {
    fn locations(&self) -> Vec<&Location> {
        self.iter().flat_map(|t| t.locations()).collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum FunctionInput {
    V0(String), // used in testing
    V2(FunctionInputV2),
}

impl TryFrom<Value> for FunctionInput {
    type Error = serde_yaml::Error;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        serde_yaml::from_value(value)
    }
}

impl Locations for FunctionInput {
    fn locations(&self) -> Vec<&Location> {
        match self {
            FunctionInput::V0(_) => vec![],
            FunctionInput::V2(input) => input.locations(),
        }
    }
}

impl FunctionInput {
    pub fn env_prefixes(&self) -> HashSet<&EnvPrefix> {
        self.locations()
            .into_iter()
            .map(|location| &location.env_prefix)
            .filter_map(|env_prefix| env_prefix.as_ref())
            .collect()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub enum FunctionOutput {
    V2(FunctionOutputV2),
}
