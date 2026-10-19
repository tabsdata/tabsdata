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

    pub fn entries_up_to(&self, product: &str) -> Result<Vec<&InventoryEntry>, UpgradeError> {
        let end = self.locate(product)?;
        Ok(self.versions[end..].iter().rev().collect())
    }

    fn start(&self, instance: &str) -> Result<usize, UpgradeError> {
        if let Some(position) = self.position(instance) {
            return Ok(position);
        }
        let requested = parse(instance)?;
        if requested < parse(self.floor())? {
            return self.locate(instance);
        }
        for (position, entry) in self.versions.iter().enumerate() {
            let listed = parse(&entry.version)?;
            if same_minor(&listed, &requested) && listed <= requested {
                return Ok(position);
            }
        }
        self.locate(instance)
    }

    /// The hops to walk from `instance` up to `product`, oldest hop first.
    pub fn hops(&self, instance: &str, product: &str) -> Result<Vec<Hop>, UpgradeError> {
        let end = self.locate(product)?;
        let start = self.start(instance)?;
        if end > start || parse(instance)? > parse(product)? {
            return Err(UpgradeError::ProductOlderThanInstance {
                product: product.to_owned(),
                instance: instance.to_owned(),
            });
        }
        let mut hops: Vec<Hop> = self.versions[end..=start]
            .windows(2)
            .rev()
            .map(|pair| Hop {
                from: pair[1].version.clone(),
                to: pair[0].version.clone(),
                upgrader: pair[0].upgrader,
            })
            .collect();
        if let Some(first) = hops.first_mut() {
            first.from = instance.to_owned();
        }
        Ok(hops)
    }
}

const MAJOR_POSITION: usize = 0;
const MINOR_POSITION: usize = 1;
const MICRO_POSITION: usize = 2;
const INITIAL_MICRO: u64 = 0;

pub fn is_micro(version: &str) -> Result<bool, UpgradeError> {
    Ok(parse(version)?
        .release()
        .get(MICRO_POSITION)
        .is_some_and(|micro| *micro > INITIAL_MICRO))
}

fn same_minor(left: &Version, right: &Version) -> bool {
    let left = left.release();
    let right = right.release();
    left.get(MAJOR_POSITION) == right.get(MAJOR_POSITION)
        && left.get(MINOR_POSITION) == right.get(MINOR_POSITION)
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
