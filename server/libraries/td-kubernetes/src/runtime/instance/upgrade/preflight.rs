//
// Copyright 2026 Tabsdata Inc.
//

use crate::base::constants::{LEGACY_VERSION, TD_VERSION_LABEL};
use crate::base::error::upgrade::UpgradeError;
use crate::core::system::exit::CommandResult;
use crate::kube::context::KubeContext;
use crate::runtime::instance::manifest::{EntryState, Manifest};
use crate::runtime::instance::upgrade::catalog::{load_inventory, resolve_kupgrade_path};
use crate::runtime::instance::upgrade::inventory::Hop;
use crate::runtime::instance::upgrade::lock::{LOCK_ENTRY, UpgradeLock, deserialize_lock};
use crate::runtime::instance::version::{product_version, require_manifest};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Standing {
    Settled,
    Upgrading {
        target: String,
        held: bool,
        entries_behind: usize,
    },
    Mismatched {
        instance: String,
        product: String,
    },
}

pub struct Preflight {
    pub instance: String,
    pub instance_version: String,
    pub product_version: String,
    pub standing: Standing,
    pub manifest: Manifest,
    pub lock: Option<UpgradeLock>,
    pub hops: Vec<Hop>,
}

impl Preflight {
    pub fn up_to_date(&self) -> bool {
        self.hops.is_empty() && !self.interrupted()
    }

    pub fn interrupted(&self) -> bool {
        self.lock.is_some()
    }

    pub fn upgraders(&self) -> usize {
        self.hops.iter().filter(|hop| hop.upgrader).count()
    }

    pub fn startable(&self) -> bool {
        self.standing == Standing::Settled
    }
}

pub async fn inspect(
    context: &KubeContext,
    instance: &str,
    verbose: bool,
) -> CommandResult<Preflight> {
    let read = require_manifest(context, instance).await?;
    let manifest = read.manifest;
    let lock = match read.data.get(LOCK_ENTRY) {
        Some(yaml) => deserialize_lock(yaml)?,
        None => None,
    };
    let product_version = product_version()?;
    let instance_version = manifest.version.clone();
    let label = read.labels.get(TD_VERSION_LABEL).cloned();

    let standing = stand(&manifest, lock.as_ref(), label.as_deref(), &product_version);
    if let Standing::Upgrading { target, .. } = &standing
        && target != &product_version
    {
        return Err(UpgradeError::TargetMismatch {
            instance: instance.to_owned(),
            target: target.clone(),
            product: product_version.clone(),
        }
        .into());
    }

    let root = resolve_kupgrade_path(verbose)?;
    let inventory = load_inventory(&root)?;
    let hops = inventory.hops(&instance_version, &product_version)?;

    Ok(Preflight {
        instance: instance.to_owned(),
        instance_version,
        product_version,
        standing,
        manifest,
        lock,
        hops,
    })
}

fn stand(
    manifest: &Manifest,
    lock: Option<&UpgradeLock>,
    label: Option<&str>,
    product: &str,
) -> Standing {
    let behind = entries_behind(manifest);
    if let Some(lock) = lock {
        return Standing::Upgrading {
            target: lock.target.clone(),
            held: lock.held(),
            entries_behind: behind,
        };
    }
    if behind > 0 {
        return Standing::Upgrading {
            target: manifest.version.clone(),
            held: false,
            entries_behind: behind,
        };
    }
    if manifest.version == product && stamped(label) == product {
        return Standing::Settled;
    }
    Standing::Mismatched {
        instance: manifest.version.clone(),
        product: product.to_owned(),
    }
}

pub fn stamped(label: Option<&str>) -> &str {
    label.unwrap_or(LEGACY_VERSION)
}

fn entries_behind(manifest: &Manifest) -> usize {
    manifest
        .entries
        .values()
        .filter(|entry| entry.state == EntryState::Active && entry.version != manifest.version)
        .count()
}

#[cfg(test)]
mod tests;
