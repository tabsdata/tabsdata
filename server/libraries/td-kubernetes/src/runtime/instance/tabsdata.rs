//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const TABSDATA_FILE: &str = "tabsdata.yaml";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TabsdataConfig {
    pub pods: PodsConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PodsConfig {
    pub global: GlobalPodConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct GlobalPodConfig {
    pub envs: BTreeMap<String, EnvValue>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EnvValue {
    Text(String),
    Number(serde_yaml::Number),
    Flag(bool),
}

impl EnvValue {
    pub fn render(&self) -> String {
        match self {
            Self::Text(text) => text.clone(),
            Self::Number(number) => number.to_string(),
            Self::Flag(flag) => flag.to_string(),
        }
    }
}

impl TabsdataConfig {
    pub fn parse(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }
}

#[cfg(test)]
mod tests;
