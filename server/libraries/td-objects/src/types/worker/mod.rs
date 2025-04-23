//
// Copyright 2025 Tabs Data Inc.
//

use crate::types::worker::v1::{FunctionInputV1, FunctionOutputV1};
use crate::types::worker::v2::{FunctionInputV2, FunctionOutputV2};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use td_apiforge::apiserver_schema;
use url::Url;

pub mod v1;
pub mod v2;

#[td_type::typed(string)]
pub struct EnvPrefix;

#[td_type::Dlo]
pub struct Location {
    uri: Url,
    #[builder(default)]
    env_prefix: Option<EnvPrefix>,
}

pub trait Locations {
    fn locations(&self) -> Vec<&Location>;
}

impl<T: Locations> Locations for Vec<T> {
    fn locations(&self) -> Vec<&Location> {
        self.iter().flat_map(|t| t.locations()).collect()
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum FunctionInput {
    V0(String), // used in testing
    V1(FunctionInputV1),
    V2(FunctionInputV2),
}

impl Locations for FunctionInput {
    fn locations(&self) -> Vec<&Location> {
        match self {
            FunctionInput::V0(_) => vec![],
            FunctionInput::V1(input) => input.locations(),
            FunctionInput::V2(input) => input.locations(),
        }
    }
}

impl FunctionInput {
    pub fn env_prefixes(&self) -> HashSet<&EnvPrefix> {
        self.locations()
            .into_iter()
            .map(|location| location.env_prefix())
            .filter_map(|env_prefix| env_prefix.as_ref())
            .collect()
    }
}

#[apiserver_schema]
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum FunctionOutput {
    V1(FunctionOutputV1),
    V2(FunctionOutputV2),
}
