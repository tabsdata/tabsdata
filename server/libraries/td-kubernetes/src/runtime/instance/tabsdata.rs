//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumString};

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
    pub components: BTreeMap<Component, ComponentConfig>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct GlobalPodConfig {
    pub envs: BTreeMap<String, EnvValue>,
}

#[derive(
    Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, EnumString, AsRefStr,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase")]
pub enum Component {
    Database,
    Apiserver,
    Aiagent,
    Aiembeddings,
    Aimetadata,
    Ephemeral,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ComponentConfig {
    pub resources: ResourcesConfig,
    pub placement: PlacementConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ResourcesConfig {
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub disk: Option<String>,
}

impl ResourcesConfig {
    pub fn is_empty(&self) -> bool {
        self.cpu.is_none() && self.memory.is_none() && self.disk.is_none()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PlacementConfig {
    pub selectors: BTreeMap<String, String>,
    pub tolerations: Vec<TolerationConfig>,
}

impl PlacementConfig {
    pub fn is_empty(&self) -> bool {
        self.selectors.is_empty() && self.tolerations.is_empty()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TolerationConfig {
    pub key: Option<String>,
    pub operator: Option<String>,
    pub value: Option<String>,
    pub effect: Option<String>,
    pub toleration_seconds: Option<i64>,
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

impl Component {
    pub fn of(component_label: &str) -> Self {
        Self::from_str(component_label).unwrap_or(Self::Ephemeral)
    }
}

impl TabsdataConfig {
    pub fn parse(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    pub fn component(&self, component_label: &str) -> Option<&ComponentConfig> {
        self.pods.components.get(&Component::of(component_label))
    }
}

#[cfg(test)]
mod tests;
