//
// Copyright 2026 Tabsdata Inc.
//

use std::str::FromStr;

use pep440_rs::Version;
use serde::{Deserialize, Serialize};

use crate::base::error::upgrade::UpgradeError;

pub const INVENTORY_SCHEMA: u32 = 1;
pub const INVENTORY_FILE: &str = "upgrades.yaml";
pub const UPGRADES_FOLDER: &str = "upgrades";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Inventory {
    pub schema: u32,
    pub versions: Vec<InventoryEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InventoryEntry {
    pub version: String,
    pub upgrader: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Hop {
    pub from: String,
    pub to: String,
    pub upgrader: bool,
}

impl Inventory {
    pub fn floor(&self) -> &str {
        self.versions
            .last()
            .map(|entry| entry.version.as_str())
            .unwrap_or_default()
    }

    pub fn newest(&self) -> &str {
        self.versions
            .first()
            .map(|entry| entry.version.as_str())
            .unwrap_or_default()
    }

    fn position(&self, version: &str) -> Option<usize> {
        self.versions
            .iter()
            .position(|entry| entry.version == version)
    }

    fn locate(&self, version: &str) -> Result<usize, UpgradeError> {
        if let Some(position) = self.position(version) {
            return Ok(position);
        }
        let requested = parse(version)?;
        if requested < parse(self.floor())? {
            return Err(UpgradeError::VersionBelowFloor {
                version: version.to_owned(),
                floor: self.floor().to_owned(),
            });
        }
        Err(UpgradeError::VersionNotInInventory {
            version: version.to_owned(),
            newest: self.newest().to_owned(),
        })
    }

    /// The hops to walk from `instance` up to `product`, oldest hop first.
    pub fn hops(&self, instance: &str, product: &str) -> Result<Vec<Hop>, UpgradeError> {
        let start = self.locate(instance)?;
        let end = self.locate(product)?;
        if end > start {
            return Err(UpgradeError::ProductOlderThanInstance {
                product: product.to_owned(),
                instance: instance.to_owned(),
            });
        }
        Ok(self.versions[end..=start]
            .windows(2)
            .rev()
            .map(|pair| Hop {
                from: pair[1].version.clone(),
                to: pair[0].version.clone(),
                upgrader: pair[0].upgrader,
            })
            .collect())
    }
}

fn parse(version: &str) -> Result<Version, UpgradeError> {
    Version::from_str(version).map_err(|error| UpgradeError::VersionParseFailed {
        version: version.to_owned(),
        cause: error.to_string(),
    })
}

pub fn deserialize_inventory(yaml: &str) -> Result<Inventory, UpgradeError> {
    let inventory: Inventory = serde_yaml::from_str(yaml)
        .map_err(|error| UpgradeError::InventoryParseFailed(error.to_string()))?;
    validate(&inventory)?;
    Ok(inventory)
}

fn validate(inventory: &Inventory) -> Result<(), UpgradeError> {
    if inventory.schema != INVENTORY_SCHEMA {
        return Err(UpgradeError::InventorySchemaUnsupported {
            found: inventory.schema,
            supported: INVENTORY_SCHEMA,
        });
    }
    if inventory.versions.is_empty() {
        return Err(UpgradeError::InventoryEmpty);
    }
    let mut previous: Option<Version> = None;
    for entry in &inventory.versions {
        let current = parse(&entry.version)?;
        if let Some(previous) = previous {
            if current == previous {
                return Err(UpgradeError::InventoryDuplicateVersion {
                    version: entry.version.clone(),
                });
            }
            if current > previous {
                return Err(UpgradeError::InventoryNotDescending {
                    version: entry.version.clone(),
                    after: previous.to_string(),
                });
            }
        }
        previous = Some(current);
    }
    let floor = inventory
        .versions
        .last()
        .expect("the inventory is not empty");
    if floor.upgrader {
        return Err(UpgradeError::InventoryFloorHasUpgrader {
            version: floor.version.clone(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests;
