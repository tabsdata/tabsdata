//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;

use chrono::Utc;

use crate::base::constants::CONFIGMAP_TABSDATA_MANIFEST;
use crate::base::error::upgrade::UpgradeError;
use crate::core::system::exit::CommandResult;
use crate::kube::api::entity::configmap::replace_configmap_at_version;
use crate::kube::context::KubeContext;
use crate::runtime::instance::manifest::{MANIFEST_FILE, serialize_manifest};
use crate::runtime::instance::upgrade::lock::{
    LOCK_ENTRY, UpgradeLock, deserialize_lock, serialize_lock,
};
use crate::runtime::instance::version::require_manifest;

struct Latch {
    data: BTreeMap<String, String>,
    labels: BTreeMap<String, String>,
    resource_version: Option<String>,
    held: Option<UpgradeLock>,
}

async fn read(context: &KubeContext, instance: &str) -> CommandResult<Latch> {
    let manifest = require_manifest(context, instance).await?;
    let held = match manifest.data.get(LOCK_ENTRY) {
        Some(yaml) => deserialize_lock(yaml)?,
        None => None,
    };
    Ok(Latch {
        data: manifest.data,
        labels: manifest.labels,
        resource_version: manifest.resource_version,
        held,
    })
}

async fn write(context: &KubeContext, instance: &str, latch: Latch) -> CommandResult {
    let written = replace_configmap_at_version(
        context,
        CONFIGMAP_TABSDATA_MANIFEST,
        latch.data,
        latch.labels,
        latch.resource_version,
        None,
    )
    .await?;
    if written {
        Ok(())
    } else {
        Err(UpgradeError::LockLost {
            instance: instance.to_owned(),
        }
        .into())
    }
}

pub async fn acquire(
    context: &KubeContext,
    instance: &str,
    target: &str,
) -> CommandResult<UpgradeLock> {
    let mut latch = read(context, instance).await?;
    if let Some(standing) = latch.held.take() {
        if standing.held() {
            return Err(UpgradeError::AlreadyLocked {
                instance: instance.to_owned(),
                id: standing.id.unwrap_or_default(),
                timestamp: standing
                    .timestamp
                    .map(|timestamp| timestamp.to_rfc3339())
                    .unwrap_or_default(),
            }
            .into());
        }
        if standing.target != target {
            return Err(UpgradeError::TargetMismatch {
                instance: instance.to_owned(),
                target: standing.target,
                product: target.to_owned(),
            }
            .into());
        }
    }
    let lock = UpgradeLock::issue(target);
    latch
        .data
        .insert(LOCK_ENTRY.to_owned(), serialize_lock(&lock)?);
    write(context, instance, latch).await?;
    Ok(lock)
}

pub async fn verify(context: &KubeContext, instance: &str, lock: &UpgradeLock) -> CommandResult {
    let latch = read(context, instance).await?;
    match latch.held {
        Some(held) if lock.holds(&held) => Ok(()),
        _ => Err(UpgradeError::LockLost {
            instance: instance.to_owned(),
        }
        .into()),
    }
}

pub async fn unlock(context: &KubeContext, instance: &str) -> CommandResult<Option<UpgradeLock>> {
    let mut latch = read(context, instance).await?;
    let Some(standing) = latch.held.take().filter(UpgradeLock::held) else {
        return Ok(None);
    };
    let unlocked = standing.unlocked();
    latch
        .data
        .insert(LOCK_ENTRY.to_owned(), serialize_lock(&unlocked)?);
    write(context, instance, latch).await?;
    Ok(Some(standing))
}

pub async fn finalise(
    context: &KubeContext,
    instance: &str,
    lock: &UpgradeLock,
    version: &str,
) -> CommandResult {
    let mut latch = read(context, instance).await?;
    let Some(standing) = latch.held.take() else {
        return Err(UpgradeError::LockAbsent {
            instance: instance.to_owned(),
        }
        .into());
    };
    if !lock.holds(&standing) {
        return Err(UpgradeError::LockLost {
            instance: instance.to_owned(),
        }
        .into());
    }
    let mut manifest = require_manifest(context, instance).await?.manifest;
    manifest.settle(version, Utc::now());
    latch
        .data
        .insert(MANIFEST_FILE.to_owned(), serialize_manifest(&manifest)?);
    latch.data.remove(LOCK_ENTRY);
    let written = replace_configmap_at_version(
        context,
        CONFIGMAP_TABSDATA_MANIFEST,
        latch.data,
        latch.labels,
        latch.resource_version,
        Some(version),
    )
    .await?;
    if written {
        Ok(())
    } else {
        Err(UpgradeError::LockLost {
            instance: instance.to_owned(),
        }
        .into())
    }
}
