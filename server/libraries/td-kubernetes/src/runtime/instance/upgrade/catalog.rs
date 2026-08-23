//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::base::constants::RESOURCE_KUPGRADE;
use crate::base::error::upgrade::UpgradeError;
use crate::core::system::exit::CommandResult;
use crate::runtime::instance::profile::bundle::parse_filename;
use crate::runtime::instance::profile::resolve_resource_path;
use crate::runtime::instance::upgrade::inventory::{
    INVENTORY_FILE, Inventory, UPGRADES_FOLDER, deserialize_inventory,
};
use crate::runtime::instance::upgrade::spec::{Spec, deserialize_spec};

pub fn resolve_kupgrade_path(verbose: bool) -> CommandResult<PathBuf> {
    let path = resolve_resource_path(RESOURCE_KUPGRADE, verbose)?;
    if !path.is_dir() {
        return Err(UpgradeError::KupgradePathInvalid(path.display().to_string()).into());
    }
    Ok(path)
}

pub fn load_inventory(root: &Path) -> CommandResult<Inventory> {
    let path = root.join(INVENTORY_FILE);
    let yaml =
        std::fs::read_to_string(&path).map_err(|error| UpgradeError::InventoryReadFailed {
            path: path.display().to_string(),
            cause: error.to_string(),
        })?;
    let inventory = deserialize_inventory(&yaml)?;
    ensure_specs(root, &inventory)?;
    Ok(inventory)
}

pub fn spec_path(root: &Path, version: &str) -> PathBuf {
    root.join(UPGRADES_FOLDER).join(format!("{version}.yaml"))
}

pub fn content_folder(root: &Path, version: &str) -> PathBuf {
    root.join(UPGRADES_FOLDER).join(version)
}

pub fn content_path(root: &Path, version: &str, folder: &str, variant: &str) -> PathBuf {
    content_folder(root, version).join(folder).join(variant)
}

pub fn load_spec(root: &Path, version: &str) -> CommandResult<Spec> {
    let path = spec_path(root, version);
    let yaml = std::fs::read_to_string(&path).map_err(|error| UpgradeError::SpecReadFailed {
        version: version.to_owned(),
        path: path.display().to_string(),
        cause: error.to_string(),
    })?;
    let spec = deserialize_spec(version, &yaml)?;
    ensure_grouping(version, &spec)?;
    ensure_content(root, version, &spec)?;
    Ok(spec)
}

pub fn read_content(
    root: &Path,
    version: &str,
    folder: &str,
    variant: &str,
) -> CommandResult<String> {
    let path = content_path(root, version, folder, variant);
    std::fs::read_to_string(&path).map_err(|error| {
        UpgradeError::ContentReadFailed {
            version: version.to_owned(),
            path: path.display().to_string(),
            cause: error.to_string(),
        }
        .into()
    })
}

fn ensure_grouping(version: &str, spec: &Spec) -> CommandResult {
    for (family, groups) in spec.families() {
        for (file, group) in groups {
            for variant in group.keys() {
                let stripped = parse_filename(variant).render_stripped();
                if &stripped != file {
                    return Err(UpgradeError::VariantOutsideGroup {
                        version: version.to_owned(),
                        entry: format!("{family}/{file}"),
                        variant: variant.clone(),
                        stripped,
                    }
                    .into());
                }
            }
        }
    }
    Ok(())
}

fn ensure_content(root: &Path, version: &str, spec: &Spec) -> CommandResult {
    let mut expected: BTreeSet<PathBuf> = BTreeSet::new();
    for (family, groups) in spec.families() {
        for (file, group) in groups {
            for (variant, action) in group {
                if !action.writes_content() {
                    continue;
                }
                let path = content_path(root, version, family, variant);
                if !path.is_file() {
                    return Err(UpgradeError::ContentMissing {
                        version: version.to_owned(),
                        entry: format!("{family}/{file}/{variant}"),
                        path: path.display().to_string(),
                    }
                    .into());
                }
                expected.insert(path);
            }
        }
    }
    let folder = content_folder(root, version);
    if !folder.is_dir() {
        return Ok(());
    }
    for (family, _) in spec.families() {
        let family_folder = folder.join(family);
        if !family_folder.is_dir() {
            continue;
        }
        let listed =
            std::fs::read_dir(&family_folder).map_err(|error| UpgradeError::ContentReadFailed {
                version: version.to_owned(),
                path: family_folder.display().to_string(),
                cause: error.to_string(),
            })?;
        for entry in listed {
            let entry = entry.map_err(|error| UpgradeError::ContentReadFailed {
                version: version.to_owned(),
                path: family_folder.display().to_string(),
                cause: error.to_string(),
            })?;
            let path = entry.path();
            if path.is_file() && !expected.contains(&path) {
                return Err(UpgradeError::ContentOrphan {
                    version: version.to_owned(),
                    path: path.display().to_string(),
                }
                .into());
            }
        }
    }
    Ok(())
}

fn ensure_specs(root: &Path, inventory: &Inventory) -> CommandResult {
    for entry in &inventory.versions {
        let path = spec_path(root, &entry.version);
        let present = path.is_file();
        if entry.upgrader && !present {
            return Err(UpgradeError::SpecMissing {
                version: entry.version.clone(),
                path: path.display().to_string(),
            }
            .into());
        }
        if !entry.upgrader && present {
            return Err(UpgradeError::SpecUnexpected {
                version: entry.version.clone(),
                path: path.display().to_string(),
            }
            .into());
        }
    }
    ensure_no_orphan_specs(root, inventory)
}

fn ensure_no_orphan_specs(root: &Path, inventory: &Inventory) -> CommandResult {
    let folder = root.join(UPGRADES_FOLDER);
    if !folder.is_dir() {
        return Ok(());
    }
    let entries = std::fs::read_dir(&folder).map_err(|error| UpgradeError::SpecsReadFailed {
        path: folder.display().to_string(),
        cause: error.to_string(),
    })?;
    for entry in entries {
        let entry = entry.map_err(|error| UpgradeError::SpecsReadFailed {
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
        let claimed = inventory
            .versions
            .iter()
            .any(|entry| entry.upgrader && entry.version == version);
        if !claimed {
            return Err(UpgradeError::SpecOrphan {
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
