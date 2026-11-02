//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, EnumString};

use crate::runtime::gateway::messaging::messenger::request::PROBE_WORKER;

pub const TABSDATA_FILE: &str = "tabsdata.yaml";

const TIER_EPHEMERAL: &str = "ephemeral";

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TabsdataConfig {
    pub pods: PodsConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PodsConfig {
    pub global: GlobalPodConfig,
    pub service: ServiceConfig,
    pub function: FunctionConfig,
    pub probe: ComponentConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct GlobalPodConfig {
    pub envs: BTreeMap<String, EnvValue>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ServiceConfig {
    #[serde(flatten)]
    pub defaults: ComponentConfig,
    pub database: ComponentConfig,
    pub apiserver: ComponentConfig,
    pub aiagent: ComponentConfig,
    pub aitdk: ComponentConfig,
    pub embedder: ComponentConfig,
    #[serde(rename = "catalog-classify")]
    pub catalog_classify: ComponentConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumString, AsRefStr)]
#[strum(serialize_all = "lowercase")]
pub enum Component {
    Database,
    Apiserver,
    Aiagent,
    Aitdk,
    Embedder,
    #[strum(serialize = "catalog-classify")]
    CatalogClassify,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ComponentConfig {
    pub resources: ResourcesConfig,
    pub placement: PlacementConfig,
    pub priority: Option<String>,
    pub service_account: ServiceAccountConfig,
    pub worker_logs: WorkerLogsConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ServiceAccountConfig {
    pub annotations: BTreeMap<String, String>,
}

/// Where the apiserver reads a worker's logs once its pod is gone, when they
/// are kept somewhere other than the provider's default.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct WorkerLogsConfig {
    pub bucket: Option<String>,
    pub location: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct FunctionConfig {
    #[serde(flatten)]
    pub component: ComponentConfig,
    pub dataflow: DataflowConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct DataflowConfig {
    pub temporary_folder: Option<String>,
    pub spill: SpillConfig,
    pub abort: AbortConfig,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SpillConfig {
    pub enabled: Option<bool>,
    pub folder: Option<String>,
    pub memory_threshold_percent: Option<Percent>,
    pub disk_limit_percent: Option<Percent>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AbortConfig {
    pub memory_threshold_percent: Option<Percent>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "u8", into = "u8")]
pub struct Percent(u8);

impl TryFrom<u8> for Percent {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        if (1..=100).contains(&value) {
            Ok(Self(value))
        } else {
            Err(format!("A percentage must be from 1 to 100, not {value}"))
        }
    }
}

impl From<Percent> for u8 {
    fn from(percent: Percent) -> Self {
        percent.0
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ResourcesConfig {
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub disk: Option<String>,
    pub limits: LimitsConfig,
}

impl ResourcesConfig {
    pub fn is_empty(&self) -> bool {
        self.cpu.is_none() && self.memory.is_none() && self.disk.is_none() && self.limits.is_empty()
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LimitsConfig {
    pub cpu: Option<String>,
    pub memory: Option<String>,
    pub disk: Option<String>,
}

impl LimitsConfig {
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
    Unset(()),
    Text(String),
    Number(serde_yaml::Number),
    Flag(bool),
}

impl EnvValue {
    pub fn render(&self) -> Option<String> {
        match self {
            Self::Unset(()) => None,
            Self::Text(text) => Some(text.clone()),
            Self::Number(number) => Some(number.to_string()),
            Self::Flag(flag) => Some(flag.to_string()),
        }
    }
}

impl ServiceConfig {
    fn of(&self, component: Component) -> &ComponentConfig {
        match component {
            Component::Database => &self.database,
            Component::Apiserver => &self.apiserver,
            Component::Aiagent => &self.aiagent,
            Component::Aitdk => &self.aitdk,
            Component::Embedder => &self.embedder,
            Component::CatalogClassify => &self.catalog_classify,
        }
    }
}

impl TabsdataConfig {
    pub fn parse(yaml: &str) -> Result<Self, serde_yaml::Error> {
        serde_yaml::from_str(yaml)
    }

    pub fn component(&self, component_label: &str, tier: Option<&str>) -> &ComponentConfig {
        match Component::from_str(component_label) {
            Ok(component) => self.pods.service.of(component),
            Err(_) if component_label == PROBE_WORKER => &self.pods.probe,
            Err(_) if tier == Some(TIER_EPHEMERAL) => &self.pods.function.component,
            Err(_) => &self.pods.service.defaults,
        }
    }

    pub fn dataflow(&self, component_label: &str, tier: Option<&str>) -> Option<&DataflowConfig> {
        match Component::from_str(component_label) {
            Err(_) if tier == Some(TIER_EPHEMERAL) => Some(&self.pods.function.dataflow),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests;
