//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::path::Path;

use chrono::Utc;
use tracing::info;

use crate::base::constants::{
    Family, KPROFILE_ASSETS_FAMILY, KPROFILE_CONFIGS_FAMILY, KPROFILE_PODS_FAMILY,
};
use crate::base::error::upgrade::UpgradeError;
use crate::base::naming::{id_from_file, to_kube_resource_name};
use crate::core::system::exit::CommandResult;
use crate::kube::api::configmap::{
    delete_configmap, exists_configmap, get_configmap, upsert_configmap,
};
use crate::kube::context::KubeContext;
use crate::runtime::instance::manifest::Manifest;
use crate::runtime::instance::upgrade::catalog::read_content;
use crate::runtime::instance::upgrade::plan::Step;
use crate::runtime::instance::upgrade::spec::{Action, ModifyLeafValueIf, Operation};

pub struct Outcome {
    pub applied: usize,
    pub stamped: usize,
}

pub async fn apply_step(
    context: &KubeContext,
    root: &Path,
    version: &str,
    step: &Step,
    manifest: &mut Manifest,
) -> CommandResult<bool> {
    let family = family_of(step.folder)?;
    let id = id_from_file(&step.file);
    let name = to_kube_resource_name(family.scope, id, family.kind, &step.file)?;
    let now = Utc::now();

    match &step.action {
        Action::Retain(_) => {
            manifest.stamp_entry(&step.entry, version, now, false);
            Ok(false)
        }
        Action::Create(_) | Action::Replace(_) => {
            let content = read_content(root, version, step.folder, &step.variant)?;
            write_configmap(context, &name, &step.file, content, family, version).await?;
            manifest.stamp_entry(&step.entry, version, now, true);
            Ok(true)
        }
        Action::Delete(_) => {
            if exists_configmap(context, &name).await? {
                delete_configmap(context, &name).await?;
            }
            manifest.drop_entry(&step.entry, now);
            Ok(true)
        }
        Action::Modify { changes } => {
            let Some(data) = get_configmap(context, &name).await? else {
                return Err(UpgradeError::ModifyTargetMissing {
                    version: version.to_owned(),
                    entry: step.entry.clone(),
                    configmap: name,
                }
                .into());
            };
            let Some(current) = data.get(&step.file) else {
                return Err(UpgradeError::ModifyTargetMissing {
                    version: version.to_owned(),
                    entry: step.entry.clone(),
                    configmap: format!("{name}/{}", step.file),
                }
                .into());
            };
            let (content, touched) = modify(version, &step.entry, current, changes)?;
            write_configmap(context, &name, &step.file, content, family, version).await?;
            manifest.stamp_entry(&step.entry, version, now, touched);
            Ok(touched)
        }
    }
}

async fn write_configmap(
    context: &KubeContext,
    name: &str,
    file: &str,
    content: String,
    family: &'static Family,
    version: &str,
) -> CommandResult {
    let mut data = BTreeMap::new();
    data.insert(file.to_owned(), content);
    upsert_configmap(
        context,
        name,
        data,
        BTreeMap::new(),
        None,
        Some(family.kind),
        Some(version),
    )
    .await?;
    Ok(())
}

fn family_of(folder: &str) -> CommandResult<&'static Family> {
    for family in [
        &KPROFILE_ASSETS_FAMILY,
        &KPROFILE_CONFIGS_FAMILY,
        &KPROFILE_PODS_FAMILY,
    ] {
        if family.folder == folder {
            return Ok(family);
        }
    }
    Err(UpgradeError::UnknownFamily {
        folder: folder.to_owned(),
    }
    .into())
}

fn modify(
    version: &str,
    entry: &str,
    current: &str,
    changes: &[Operation],
) -> CommandResult<(String, bool)> {
    let mut document: serde_yaml::Value =
        serde_yaml::from_str(current).map_err(|error| UpgradeError::ModifyParseFailed {
            version: version.to_owned(),
            entry: entry.to_owned(),
            cause: error.to_string(),
        })?;
    let mut touched = false;
    for change in changes {
        match change {
            Operation::ModifyLeafValueIf(operation) => {
                if replace_leaf(&mut document, operation) {
                    touched = true;
                } else {
                    info!(
                        "Upgrade to '{version}' left '{entry}' leaf '{}' alone; it does not \
                         hold the expected value.",
                        operation.leaf
                    );
                }
            }
        }
    }
    let rendered =
        serde_yaml::to_string(&document).map_err(|error| UpgradeError::ModifyRenderFailed {
            version: version.to_owned(),
            entry: entry.to_owned(),
            cause: error.to_string(),
        })?;
    Ok((rendered, touched))
}

fn replace_leaf(document: &mut serde_yaml::Value, operation: &ModifyLeafValueIf) -> bool {
    let mut cursor = document;
    let segments = split_leaf(&operation.leaf);
    let mut walking = segments.iter().peekable();
    while let Some(segment) = walking.next() {
        let key = serde_yaml::Value::from(segment.as_str());
        if walking.peek().is_none() {
            let Some(slot) = cursor.get_mut(&key) else {
                return false;
            };
            if *slot != operation.from {
                return false;
            }
            *slot = operation.to.clone();
            return true;
        }
        let Some(next) = cursor.get_mut(&key) else {
            return false;
        };
        cursor = next;
    }
    false
}

fn split_leaf(leaf: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for character in leaf.chars() {
        match character {
            '"' => quoted = !quoted,
            '.' if !quoted => segments.push(std::mem::take(&mut current)),
            _ => current.push(character),
        }
    }
    segments.push(current);
    segments
}

#[cfg(test)]
mod tests;
