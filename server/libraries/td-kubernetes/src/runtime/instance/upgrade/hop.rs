//
// Copyright 2026 Tabsdata Inc.
//

use std::path::Path;

use chrono::Utc;
use tracing::info;

use crate::base::constants::{
    CONFIGMAP_FAMILY_LABEL, CONFIGMAP_TABSDATA_MANIFEST, KPROFILE_CONFIGS_FAMILY,
    SCHEMA_SOURCE_EXTENSION,
};
use crate::base::error::upgrade::UpgradeError;
use crate::base::naming::name_from_kube_resource_name;
use crate::base::reporter::Reporter;
use crate::client::registry::{instance_edit_folder, instance_init_folder};
use crate::core::system::exit::CommandResult;
use crate::kube::api::configmap::replace_configmap_at_version;
use crate::kube::api::{LabelFilter, configmap as configmap_api, secret as secret_api};
use crate::kube::context::KubeContext;
use crate::runtime::instance::manifest::{MANIFEST_FILE, Manifest, serialize_manifest};
use crate::runtime::instance::profile::bundle::Axis;
use crate::runtime::instance::upgrade::apply::{Applied, apply_foreign_assets, apply_step};
use crate::runtime::instance::upgrade::catalog::{load_spec, resolve_kupgrade_path};
use crate::runtime::instance::upgrade::inventory::Hop;
use crate::runtime::instance::upgrade::latch::verify;
use crate::runtime::instance::upgrade::lock::UpgradeLock;
use crate::runtime::instance::upgrade::plan::plan;
use crate::runtime::instance::upgrade::store::ConfigmapStore;
use crate::runtime::instance::version::{InstanceManifest, require_manifest};
use crate::runtime::provider::Provider;

pub struct Walked {
    pub applied: usize,
    pub retained: usize,
}

pub fn discard_editable_folders(instance: &str) -> CommandResult<Vec<String>> {
    let mut discarded = Vec::new();
    for folder in [
        instance_init_folder(instance)?,
        instance_edit_folder(instance)?,
    ] {
        if !folder.is_dir() {
            continue;
        }
        std::fs::remove_dir_all(&folder).map_err(|error| UpgradeError::EditableRemoveFailed {
            path: folder.display().to_string(),
            cause: error.to_string(),
        })?;
        discarded.push(folder.display().to_string());
    }
    Ok(discarded)
}

pub async fn upgrade_resources(
    provider: &dyn Provider,
    reporter: &dyn Reporter,
    context: &KubeContext,
    instance: &str,
    hops: &[Hop],
    lock: &UpgradeLock,
    verbose: bool,
) -> CommandResult<Walked> {
    let root = resolve_kupgrade_path(verbose)?;
    let axes = provider.cluster_provider().axes();
    let preferred = provider.cluster_provider().preferred_values();
    let mut total = Walked {
        applied: 0,
        retained: 0,
    };
    for hop in hops {
        verify(context, instance, lock).await?;
        let bar = reporter.step(&format!("Upgrading to {}", hop.to));
        let walked = match walk(context, instance, &root, hop, axes, preferred, verbose).await {
            Ok(walked) => walked,
            Err(error) => {
                bar.fail();
                return Err(error);
            }
        };
        bar.done(&format!(
            "Upgraded to {} ({} changed, {} retained)",
            hop.to, walked.applied, walked.retained
        ));
        total.applied += walked.applied;
        total.retained += walked.retained;
    }
    Ok(total)
}

pub async fn walk(
    context: &KubeContext,
    instance: &str,
    root: &Path,
    hop: &Hop,
    axes: &[Axis],
    preferred: &[&'static str],
    verbose: bool,
) -> CommandResult<Walked> {
    let mut read = require_manifest(context, instance).await?;
    let mut walked = Walked {
        applied: 0,
        retained: 0,
    };

    if hop.upgrader {
        let spec = load_spec(root, &hop.to)?;
        let store = ConfigmapStore::new(context, &hop.to);
        for step in plan(&spec, axes, preferred) {
            let applied = apply_step(&store, root, &hop.to, &step).await?;
            let now = Utc::now();
            match applied {
                Applied::Dropped => {
                    read.manifest.drop_entry(&step.entry, now);
                }
                Applied::Stamped(touched) => {
                    read.manifest
                        .stamp_entry(&step.entry, &hop.to, now, touched);
                }
            }
            if applied.changed() {
                walked.applied += 1;
            } else {
                walked.retained += 1;
            }
            info!("  {} -> {:?}", step.entry, step.action);
        }
    }

    walked.applied += purge_staged_schemas(context, &mut read.manifest).await?;

    walked.applied += apply_foreign_assets(context, &hop.to, &mut read.manifest, verbose).await?;

    for path in untouched(&read.manifest, &hop.to) {
        read.manifest.stamp_entry(&path, &hop.to, Utc::now(), false);
        walked.retained += 1;
    }

    read.manifest.settle(&hop.to, Utc::now());
    commit(context, instance, &read, &hop.to).await?;
    Ok(walked)
}

async fn purge_staged_schemas(
    context: &KubeContext,
    manifest: &mut Manifest,
) -> CommandResult<usize> {
    let family = &KPROFILE_CONFIGS_FAMILY;
    let filter = LabelFilter::new(CONFIGMAP_FAMILY_LABEL, family.kind);
    let existing = configmap_api::list_configmaps(context, Some(filter)).await?;
    let mut purged = 0;
    for (name, _) in existing {
        let file = name_from_kube_resource_name(&name, family.scope, family.kind)?;
        if !file.ends_with(SCHEMA_SOURCE_EXTENSION) {
            continue;
        }
        let entry = format!("{}/{file}", family.folder);
        configmap_api::delete_configmap(context, &name).await?;
        manifest.drop_entry(&entry, Utc::now());
        purged += 1;
        info!("  {entry} -> deleted (schema files are not configs)");
    }
    Ok(purged)
}

async fn stamp_instance(context: &KubeContext, version: &str) -> CommandResult {
    let configmaps =
        configmap_api::stamp_configmaps(context, version, &[CONFIGMAP_TABSDATA_MANIFEST]).await?;
    let secrets = secret_api::stamp_secrets(context, version).await?;
    if !configmaps.is_empty() || !secrets.is_empty() {
        info!(
            "  Labelled {} configmap(s) and {} secret(s) with version {version}",
            configmaps.len(),
            secrets.len()
        );
    }
    Ok(())
}

fn untouched(manifest: &Manifest, version: &str) -> Vec<String> {
    manifest
        .entries
        .iter()
        .filter(|(_, entry)| entry.version != version)
        .map(|(path, _)| path.clone())
        .collect()
}

async fn commit(
    context: &KubeContext,
    instance: &str,
    read: &InstanceManifest,
    version: &str,
) -> CommandResult {
    stamp_instance(context, version).await?;
    let mut data = read.data.clone();
    data.insert(
        MANIFEST_FILE.to_owned(),
        serialize_manifest(&read.manifest)?,
    );
    let labels = read.labels.clone();
    let written = replace_configmap_at_version(
        context,
        CONFIGMAP_TABSDATA_MANIFEST,
        data,
        labels,
        read.resource_version.clone(),
        Some(version),
    )
    .await?;
    if !written {
        return Err(UpgradeError::LockLost {
            instance: instance.to_owned(),
        }
        .into());
    }
    Ok(())
}
