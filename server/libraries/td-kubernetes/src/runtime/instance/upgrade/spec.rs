//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::base::error::upgrade::UpgradeError;

pub const SPEC_SCHEMA: u32 = 1;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Spec {
    pub schema: u32,
    #[serde(default)]
    pub assets: BTreeMap<String, Group>,
    #[serde(default)]
    pub configs: BTreeMap<String, Group>,
    #[serde(default)]
    pub pods: BTreeMap<String, Group>,
}

pub type Group = BTreeMap<String, Action>;

#[derive(Clone, Debug, Deserialize)]
struct Probe {
    #[serde(default)]
    changes: Option<serde_yaml::Value>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum Action {
    Retain(Bare),
    Create(Bare),
    Replace(Bare),
    Delete(Bare),
    Modify { changes: Vec<Operation> },
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bare {}

impl Action {
    pub fn writes_content(&self) -> bool {
        matches!(self, Self::Create(_) | Self::Replace(_))
    }

    pub fn changes(&self) -> &[Operation] {
        match self {
            Self::Modify { changes } => changes,
            _ => &[],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
pub enum Operation {
    ModifyLeafValueIf(ModifyLeafValueIf),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModifyLeafValueIf {
    pub leaf: String,
    #[serde(default)]
    pub from: serde_yaml::Value,
    #[serde(default)]
    pub to: serde_yaml::Value,
}

impl Spec {
    pub fn families(&self) -> [(&'static str, &BTreeMap<String, Group>); 3] {
        [
            ("assets", &self.assets),
            ("configs", &self.configs),
            ("pods", &self.pods),
        ]
    }
}

pub fn deserialize_spec(version: &str, yaml: &str) -> Result<Spec, UpgradeError> {
    let spec: Spec = serde_yaml::from_str(yaml).map_err(|error| UpgradeError::SpecParseFailed {
        version: version.to_owned(),
        cause: error.to_string(),
    })?;
    validate(version, &spec, yaml)?;
    Ok(spec)
}

fn validate(version: &str, spec: &Spec, yaml: &str) -> Result<(), UpgradeError> {
    if spec.schema != SPEC_SCHEMA {
        return Err(UpgradeError::SpecSchemaUnsupported {
            version: version.to_owned(),
            found: spec.schema,
            supported: SPEC_SCHEMA,
        });
    }
    let raw: serde_yaml::Value =
        serde_yaml::from_str(yaml).map_err(|error| UpgradeError::SpecParseFailed {
            version: version.to_owned(),
            cause: error.to_string(),
        })?;
    for (family, groups) in spec.families() {
        for (file, group) in groups {
            for (variant, action) in group {
                let carries = raw
                    .get(family)
                    .and_then(|value| value.get(file))
                    .and_then(|value| value.get(variant))
                    .and_then(|value| serde_yaml::from_value::<Probe>(value.clone()).ok())
                    .is_some_and(|probe| probe.changes.is_some());
                if carries && !matches!(action, Action::Modify { .. }) {
                    return Err(UpgradeError::SpecChangesUnexpected {
                        version: version.to_owned(),
                        entry: format!("{family}/{file}/{variant}"),
                    });
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
