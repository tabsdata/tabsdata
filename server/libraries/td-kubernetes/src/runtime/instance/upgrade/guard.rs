//
// Copyright 2026 Tabsdata Inc.
//

use crate::base::error::upgrade::UpgradeError;
use crate::core::system::exit::CommandResult;
use crate::kube::context::KubeContext;
use crate::runtime::instance::manifest::{EntryState, Manifest};
use crate::runtime::instance::upgrade::lock::{LOCK_ENTRY, deserialize_lock};
use crate::runtime::instance::upgrade::preflight::stamped;
use crate::runtime::instance::version::{product_version, require_manifest};

pub async fn ensure_upgraded(context: &KubeContext, instance: &str) -> CommandResult {
    let read = require_manifest(context, instance).await?;

    if let Some(yaml) = read.data.get(LOCK_ENTRY)
        && let Some(lock) = deserialize_lock(yaml)?
    {
        return Err(UpgradeError::StartWhileUpgrading {
            instance: instance.to_owned(),
            version: read.manifest.version,
            target: lock.target,
        }
        .into());
    }

    let stamp = stamped(read.stamp());
    if stamp != read.manifest.version {
        return Err(UpgradeError::StartWhilePartiallyUpgraded {
            instance: instance.to_owned(),
            version: read.manifest.version.clone(),
            stamped: stamp.to_owned(),
        }
        .into());
    }

    let behind = entries_behind(&read.manifest);
    if !behind.is_empty() {
        return Err(UpgradeError::StartWithStrayEntries {
            instance: instance.to_owned(),
            version: read.manifest.version.clone(),
            entries: behind.join(", "),
        }
        .into());
    }

    let product = product_version()?;
    if read.manifest.version != product {
        return Err(UpgradeError::StartOnAnotherVersion {
            instance: instance.to_owned(),
            version: read.manifest.version,
            product,
        }
        .into());
    }
    Ok(())
}

fn entries_behind(manifest: &Manifest) -> Vec<String> {
    manifest
        .entries
        .iter()
        .filter(|(_, entry)| entry.state == EntryState::Active && entry.version != manifest.version)
        .map(|(path, entry)| format!("{path} (at {})", entry.version))
        .collect()
}

#[cfg(test)]
mod tests;
