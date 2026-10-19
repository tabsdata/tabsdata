//
// Copyright 2026 Tabsdata Inc.
//

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::base::error::upgrade::UpgradeError;
use crate::core::system::exit::CommandResult;
use crate::runtime::instance::upgrade::inventory::{Hop, Inventory};

pub const RULES_SCHEMA: u32 = 1;
pub const RULES_FOLDER: &str = "rules";

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rules {
    pub schema: u32,
    #[serde(default)]
    pub rules: Vec<String>,
}

pub fn rules_path(root: &Path, version: &str) -> PathBuf {
    root.join(RULES_FOLDER).join(format!("{version}.yaml"))
}

pub fn deserialize_rules(version: &str, yaml: &str) -> Result<Rules, UpgradeError> {
    let rules: Rules =
        serde_yaml::from_str(yaml).map_err(|error| UpgradeError::RulesParseFailed {
            version: version.to_owned(),
            cause: error.to_string(),
        })?;
    if rules.schema != RULES_SCHEMA {
        return Err(UpgradeError::RulesSchemaUnsupported {
            version: version.to_owned(),
            found: rules.schema,
            supported: RULES_SCHEMA,
        });
    }
    Ok(rules)
}

pub fn load_rules(root: &Path, version: &str) -> CommandResult<Vec<String>> {
    let path = rules_path(root, version);
    let yaml = std::fs::read_to_string(&path).map_err(|error| UpgradeError::RulesReadFailed {
        version: version.to_owned(),
        path: path.display().to_string(),
        cause: error.to_string(),
    })?;
    Ok(deserialize_rules(version, &yaml)?.rules)
}

pub fn rules_to_apply(
    root: &Path,
    inventory: &Inventory,
    hops: &[Hop],
    product: &str,
    interrupted: bool,
) -> CommandResult<Vec<String>> {
    let versions: Vec<&str> = if interrupted {
        inventory
            .entries_up_to(product)?
            .into_iter()
            .filter(|entry| entry.upgrader)
            .map(|entry| entry.version.as_str())
            .collect()
    } else {
        hops.iter()
            .filter(|hop| hop.upgrader)
            .map(|hop| hop.to.as_str())
            .collect()
    };
    let mut collected: Vec<String> = Vec::new();
    for version in versions {
        for rule in load_rules(root, version)? {
            if !collected.contains(&rule) {
                collected.push(rule);
            }
        }
    }
    Ok(collected)
}

pub fn ensure_rules(root: &Path, inventory: &Inventory) -> CommandResult {
    for entry in &inventory.versions {
        let path = rules_path(root, &entry.version);
        let present = path.is_file();
        if entry.upgrader && !present {
            return Err(UpgradeError::RulesMissing {
                version: entry.version.clone(),
                path: path.display().to_string(),
            }
            .into());
        }
        if !entry.upgrader && present {
            return Err(UpgradeError::RulesUnexpected {
                version: entry.version.clone(),
                path: path.display().to_string(),
            }
            .into());
        }
    }
    ensure_no_orphan_rules(root, inventory)
}

fn ensure_no_orphan_rules(root: &Path, inventory: &Inventory) -> CommandResult {
    let folder = root.join(RULES_FOLDER);
    if !folder.is_dir() {
        return Ok(());
    }
    let entries =
        std::fs::read_dir(&folder).map_err(|error| UpgradeError::RulesFolderReadFailed {
            path: folder.display().to_string(),
            cause: error.to_string(),
        })?;
    for entry in entries {
        let entry = entry.map_err(|error| UpgradeError::RulesFolderReadFailed {
            path: folder.display().to_string(),
            cause: error.to_string(),
        })?;
        let path = entry.path();
        if !path.is_file() || path.extension().is_none_or(|suffix| suffix != "yaml") {
            continue;
        }
        let Some(version) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        if !inventory
            .versions
            .iter()
            .any(|entry| entry.version == version)
        {
            return Err(UpgradeError::RulesOrphan {
                version: version.to_owned(),
                path: path.display().to_string(),
            }
            .into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
