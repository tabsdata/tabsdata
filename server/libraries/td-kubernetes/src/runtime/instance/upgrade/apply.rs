//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::path::Path;

use chrono::Utc;
use regex::Regex;
use tracing::info;

use crate::base::constants::{FOREIGN_ASSETS, Family, KPROFILE_ASSETS_FAMILY};
use crate::base::error::upgrade::UpgradeError;
use crate::base::naming::{id_from_file, to_kube_resource_name};
use crate::core::system::exit::CommandResult;
use crate::kube::api::configmap::upsert_configmap;
use crate::kube::context::KubeContext;
use crate::runtime::instance::manifest::Manifest;
use crate::runtime::instance::profile::resolve_foreign_path;
use crate::runtime::instance::upgrade::catalog::read_content;
use crate::runtime::instance::upgrade::plan::Step;
use crate::runtime::instance::upgrade::spec::{
    Action, InsertChildIfMissing, InsertSiblingIfExisting, ModifyLeafValueIf, ModifyTextIf,
    NestChildrenIfFlat, Operation, Placement, Scope,
};
use crate::runtime::instance::upgrade::store::Store;

pub struct Outcome {
    pub applied: usize,
    pub stamped: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Applied {
    Stamped(bool),
    Dropped,
}

impl Applied {
    pub fn changed(&self) -> bool {
        match self {
            Self::Stamped(touched) => *touched,
            Self::Dropped => true,
        }
    }
}

pub async fn apply_step(
    store: &dyn Store,
    root: &Path,
    version: &str,
    step: &Step,
) -> CommandResult<Applied> {
    match &step.action {
        Action::Retain(_) => Ok(Applied::Stamped(false)),
        Action::Create(_) | Action::Replace(_) => {
            let content = read_content(root, version, step.folder, &step.variant)?;
            store.write(step, content).await?;
            Ok(Applied::Stamped(true))
        }
        Action::Merge(_) => Err(UpgradeError::MergeNotImplemented {
            version: version.to_owned(),
            entry: step.entry.clone(),
        }
        .into()),
        Action::Delete(_) => {
            store.remove(step).await?;
            Ok(Applied::Dropped)
        }
        Action::Modify { changes } => {
            let Some(current) = store.read(step).await? else {
                return Err(UpgradeError::ModifyTargetMissing {
                    version: version.to_owned(),
                    entry: step.entry.clone(),
                    configmap: store.locate(step)?,
                }
                .into());
            };
            let (content, touched) = modify(version, &step.entry, &current, changes)?;
            store.write(step, content).await?;
            Ok(Applied::Stamped(touched))
        }
    }
}

pub async fn apply_foreign_assets(
    context: &KubeContext,
    version: &str,
    manifest: &mut Manifest,
    verbose: bool,
) -> CommandResult<usize> {
    let family = &KPROFILE_ASSETS_FAMILY;
    let mut applied = 0;
    for foreign in FOREIGN_ASSETS {
        let file = foreign.asset();
        let source = resolve_foreign_path(*foreign, verbose)?;
        let content = std::fs::read_to_string(&source).map_err(|error| {
            UpgradeError::ForeignAssetReadFailed {
                path: source.display().to_string(),
                cause: error.to_string(),
            }
        })?;
        let name = to_kube_resource_name(family.scope, id_from_file(file), family.kind, file)?;
        write_configmap(context, &name, file, content, family, version).await?;
        manifest.stamp_entry(
            &format!("{}/{file}", family.folder),
            version,
            Utc::now(),
            true,
        );
        applied += 1;
        info!(
            "  {}/{file} -> applied from {}",
            family.folder,
            source.display()
        );
    }
    Ok(applied)
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

pub(super) fn modify(
    version: &str,
    entry: &str,
    current: &str,
    changes: &[Operation],
) -> CommandResult<(String, bool)> {
    let mut text = current.to_owned();
    let mut touched = false;
    for change in changes {
        let (rewritten, hit) = change.replace(version, entry, &text)?;
        text = rewritten;
        touched |= hit;
    }
    Ok((text, touched))
}

impl Operation {
    fn replace(&self, version: &str, entry: &str, current: &str) -> CommandResult<(String, bool)> {
        match self {
            Self::ModifyTextIf(operation) => operation.replace(version, entry, current),
            Self::ModifyLeafValueIf(operation) => operation.replace(version, entry, current),
            Self::InsertSiblingIfExisting(operation) => operation.replace(version, entry, current),
            Self::InsertChildIfMissing(operation) => operation.replace(version, entry, current),
            Self::NestChildrenIfFlat(operation) => operation.replace(version, entry, current),
        }
    }
}

impl NestChildrenIfFlat {
    fn replace(&self, version: &str, entry: &str, current: &str) -> CommandResult<(String, bool)> {
        let mut document: serde_yaml::Value =
            serde_yaml::from_str(current).map_err(|error| UpgradeError::ModifyParseFailed {
                version: version.to_owned(),
                entry: entry.to_owned(),
                cause: error.to_string(),
            })?;
        if !self.nest(&mut document) {
            info!(
                "Upgrade to '{version}' left '{entry}' alone; leaf '{}' is absent or no longer \
                 holds a flat block.",
                self.leaf
            );
            return Ok((current.to_owned(), false));
        }
        let rendered =
            serde_yaml::to_string(&document).map_err(|error| UpgradeError::ModifyRenderFailed {
                version: version.to_owned(),
                entry: entry.to_owned(),
                cause: error.to_string(),
            })?;
        Ok((rendered, true))
    }

    fn nest(&self, document: &mut serde_yaml::Value) -> bool {
        let mut cursor = document;
        for segment in split_leaf(&self.leaf) {
            let Some(next) = cursor.get_mut(serde_yaml::Value::from(segment.as_str())) else {
                return false;
            };
            cursor = next;
        }
        let serde_yaml::Value::Mapping(holder) = cursor else {
            return false;
        };
        let flat = holder
            .values()
            .all(|value| !value.is_mapping() && !value.is_sequence());
        if holder.is_empty() || !flat {
            return false;
        }
        let children = std::mem::take(holder);
        holder.insert(
            serde_yaml::Value::from(self.under.as_str()),
            serde_yaml::Value::Mapping(children),
        );
        true
    }
}

impl InsertChildIfMissing {
    fn replace(&self, version: &str, entry: &str, current: &str) -> CommandResult<(String, bool)> {
        let mut document: serde_yaml::Value =
            serde_yaml::from_str(current).map_err(|error| UpgradeError::ModifyParseFailed {
                version: version.to_owned(),
                entry: entry.to_owned(),
                cause: error.to_string(),
            })?;
        if !self.insert(&mut document) {
            info!(
                "Upgrade to '{version}' left '{entry}' alone; leaf '{}' already has child '{}' \
                 or does not hold a mapping.",
                self.leaf, self.child
            );
            return Ok((current.to_owned(), false));
        }
        let rendered =
            serde_yaml::to_string(&document).map_err(|error| UpgradeError::ModifyRenderFailed {
                version: version.to_owned(),
                entry: entry.to_owned(),
                cause: error.to_string(),
            })?;
        Ok((rendered, true))
    }

    fn insert(&self, document: &mut serde_yaml::Value) -> bool {
        let mut cursor = document;
        for segment in split_leaf(&self.leaf) {
            let key = serde_yaml::Value::from(segment.as_str());
            if cursor.is_null() {
                *cursor = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
            }
            let serde_yaml::Value::Mapping(holder) = cursor else {
                return false;
            };
            cursor = holder.entry(key).or_insert(serde_yaml::Value::Null);
        }
        if cursor.is_null() {
            *cursor = serde_yaml::Value::Mapping(serde_yaml::Mapping::new());
        }
        let serde_yaml::Value::Mapping(holder) = cursor else {
            return false;
        };
        let child = serde_yaml::Value::from(self.child.as_str());
        if holder.contains_key(&child) {
            return false;
        }
        holder.insert(child, self.value.clone());
        true
    }
}

impl InsertSiblingIfExisting {
    fn replace(&self, version: &str, entry: &str, current: &str) -> CommandResult<(String, bool)> {
        let mut document: serde_yaml::Value =
            serde_yaml::from_str(current).map_err(|error| UpgradeError::ModifyParseFailed {
                version: version.to_owned(),
                entry: entry.to_owned(),
                cause: error.to_string(),
            })?;
        if !self.insert(&mut document) {
            info!(
                "Upgrade to '{version}' left '{entry}' alone; leaf '{}' is absent or already has \
                 sibling '{}'.",
                self.leaf, self.sibling
            );
            return Ok((current.to_owned(), false));
        }
        let rendered =
            serde_yaml::to_string(&document).map_err(|error| UpgradeError::ModifyRenderFailed {
                version: version.to_owned(),
                entry: entry.to_owned(),
                cause: error.to_string(),
            })?;
        Ok((rendered, true))
    }

    fn insert(&self, document: &mut serde_yaml::Value) -> bool {
        let segments = split_leaf(&self.leaf);
        let Some((anchor, parents)) = segments.split_last() else {
            return false;
        };
        let anchor = serde_yaml::Value::from(anchor.as_str());
        match self.scope {
            Scope::Addressed => {
                let mut cursor = document;
                for segment in parents {
                    let Some(next) = cursor.get_mut(serde_yaml::Value::from(segment.as_str()))
                    else {
                        return false;
                    };
                    cursor = next;
                }
                self.beside(cursor, &anchor)
            }
            Scope::Everywhere => self.beside_everywhere(document, &anchor),
        }
    }

    fn beside_everywhere(&self, node: &mut serde_yaml::Value, anchor: &serde_yaml::Value) -> bool {
        let mut inserted = self.beside(node, anchor);
        match node {
            serde_yaml::Value::Mapping(holder) => {
                for (_, value) in holder.iter_mut() {
                    inserted |= self.beside_everywhere(value, anchor);
                }
            }
            serde_yaml::Value::Sequence(items) => {
                for item in items.iter_mut() {
                    inserted |= self.beside_everywhere(item, anchor);
                }
            }
            _ => {}
        }
        inserted
    }

    fn beside(&self, node: &mut serde_yaml::Value, anchor: &serde_yaml::Value) -> bool {
        let sibling = serde_yaml::Value::from(self.sibling.as_str());
        let serde_yaml::Value::Mapping(holder) = node else {
            return false;
        };
        if !holder.contains_key(anchor) || holder.contains_key(&sibling) {
            return false;
        }
        let mut rebuilt = serde_yaml::Mapping::with_capacity(holder.len() + 1);
        for (key, value) in holder.iter() {
            if key == anchor && self.placement == Placement::Before {
                rebuilt.insert(sibling.clone(), self.value.clone());
            }
            rebuilt.insert(key.clone(), value.clone());
            if key == anchor && self.placement == Placement::After {
                rebuilt.insert(sibling.clone(), self.value.clone());
            }
        }
        *holder = rebuilt;
        true
    }
}

impl ModifyTextIf {
    fn replace(&self, version: &str, entry: &str, current: &str) -> CommandResult<(String, bool)> {
        let expression = format!(
            "{}{}{}",
            boundary("before", self.before.as_deref()),
            self.pattern,
            boundary("after", self.after.as_deref())
        );
        let pattern =
            Regex::new(&expression).map_err(|error| UpgradeError::ModifyPatternInvalid {
                version: version.to_owned(),
                entry: entry.to_owned(),
                pattern: expression.clone(),
                cause: error.to_string(),
            })?;
        if !pattern.is_match(current) {
            info!(
                "Upgrade to '{version}' left '{entry}' alone; the pattern '{expression}' matched \
                 nothing."
            );
            return Ok((current.to_owned(), false));
        }
        let mut replacement = String::new();
        if self.before.is_some() {
            replacement.push_str("${before}");
        }
        replacement.push_str(&self.replacement.replace('$', "$$"));
        if self.after.is_some() {
            replacement.push_str("${after}");
        }
        Ok((
            pattern
                .replace_all(current, replacement.as_str())
                .into_owned(),
            true,
        ))
    }
}

impl ModifyLeafValueIf {
    fn replace(&self, version: &str, entry: &str, current: &str) -> CommandResult<(String, bool)> {
        let mut document: serde_yaml::Value =
            serde_yaml::from_str(current).map_err(|error| UpgradeError::ModifyParseFailed {
                version: version.to_owned(),
                entry: entry.to_owned(),
                cause: error.to_string(),
            })?;
        if !self.assign(&mut document) {
            info!(
                "Upgrade to '{version}' left '{entry}' leaf '{}' alone; it does not hold the \
                 expected value.",
                self.leaf
            );
            return Ok((current.to_owned(), false));
        }
        let rendered =
            serde_yaml::to_string(&document).map_err(|error| UpgradeError::ModifyRenderFailed {
                version: version.to_owned(),
                entry: entry.to_owned(),
                cause: error.to_string(),
            })?;
        Ok((rendered, true))
    }

    fn assign(&self, document: &mut serde_yaml::Value) -> bool {
        let mut cursor = document;
        let segments = split_leaf(&self.leaf);
        let mut walking = segments.iter().peekable();
        while let Some(segment) = walking.next() {
            let key = serde_yaml::Value::from(segment.as_str());
            if walking.peek().is_none() {
                let Some(slot) = cursor.get_mut(&key) else {
                    return false;
                };
                if *slot != self.from {
                    return false;
                }
                *slot = self.to.clone();
                return true;
            }
            let Some(next) = cursor.get_mut(&key) else {
                return false;
            };
            cursor = next;
        }
        false
    }
}

fn boundary(name: &str, expression: Option<&str>) -> String {
    match expression {
        Some(expression) => format!("(?<{name}>{expression})"),
        None => String::new(),
    }
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
