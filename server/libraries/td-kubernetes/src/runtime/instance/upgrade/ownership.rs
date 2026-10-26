//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::path::Path;

use serde_yaml::Value;
use td_error::TdError;

use crate::base::constants::{KPROFILE_CONFIGS_FAMILY, SCHEMA_SOURCE_EXTENSION};
use crate::base::error::upgrade::UpgradeError;
use crate::core::system::exit::CommandResult;
use crate::runtime::gateway::resolver::schema::Schema;
use crate::runtime::instance::profile::bundle::parse_filename;
use crate::runtime::instance::upgrade::apply::modify;
use crate::runtime::instance::upgrade::catalog::load_spec;
use crate::runtime::instance::upgrade::offline::steps_of;
use crate::runtime::instance::upgrade::plan::Step;
use crate::runtime::instance::upgrade::spec::Action;

const CONFIG_EXTENSION: &str = ".yaml";
const PATH_SEPARATOR: &str = ".";

type OpenMappings = BTreeMap<String, Value>;

pub fn ownership_problems(
    kupgrade_folder: &Path,
    version: &str,
    previous_kprofile_folder: &Path,
    current_kprofile_folder: &Path,
) -> CommandResult<Vec<String>> {
    let spec = load_spec(kupgrade_folder, version)?;
    let configs_folder = previous_kprofile_folder.join(KPROFILE_CONFIGS_FAMILY.folder);
    let mut problems = Vec::new();
    for step in steps_of(&spec, previous_kprofile_folder)? {
        if step.folder != KPROFILE_CONFIGS_FAMILY.folder
            || step.file.ends_with(SCHEMA_SOURCE_EXTENSION)
        {
            continue;
        }
        let schemas = schemas_of(current_kprofile_folder, &step.file)?;
        if schemas.is_empty() {
            continue;
        }
        let Ok(previous) = std::fs::read_to_string(configs_folder.join(&step.variant)) else {
            continue;
        };
        problems.extend(step_problems(version, &step, &previous, &schemas)?);
    }
    Ok(problems)
}

fn step_problems(
    version: &str,
    step: &Step,
    previous: &str,
    schemas: &[Schema],
) -> CommandResult<Vec<String>> {
    let entry = format!("{}/{}", step.folder, step.variant);
    let before = open_mappings_of(version, &entry, previous, schemas)?;
    let problems = match &step.action {
        Action::Replace(_) | Action::Merge(_) => before
            .keys()
            .map(|path| problem(version, &entry, path, "rewrites the whole file around"))
            .collect(),
        Action::Modify { changes } => {
            let (upgraded, _) = modify(version, &entry, previous, changes)?;
            let after = open_mappings_of(version, &entry, &upgraded, schemas)?;
            changed_mappings(&before, &after)
                .into_iter()
                .map(|(path, change)| problem(version, &entry, &path, change))
                .collect()
        }
        Action::Retain(_) | Action::Create(_) | Action::Delete(_) => Vec::new(),
    };
    Ok(problems)
}

fn changed_mappings(before: &OpenMappings, after: &OpenMappings) -> Vec<(String, &'static str)> {
    let mut changes = Vec::new();
    for (path, mapping) in before {
        match after.get(path) {
            None => changes.push((path.clone(), "removes")),
            Some(upgraded) if upgraded != mapping => changes.push((path.clone(), "changes")),
            Some(_) => {}
        }
    }
    for (path, mapping) in after {
        let created = !before.contains_key(path);
        let filled = mapping
            .as_mapping()
            .is_some_and(|entries| !entries.is_empty());
        if created && filled {
            changes.push((path.clone(), "fills"));
        }
    }
    changes
}

fn problem(version: &str, entry: &str, path: &str, change: &str) -> String {
    format!(
        "The upgrader of version '{version}' {change} the open mapping '{path}' of '{entry}'; \
         its keys belong to the user, so an upgrader must leave them alone."
    )
}

fn schemas_of(kprofile_folder: &Path, config: &str) -> CommandResult<Vec<Schema>> {
    let stem = config.strip_suffix(CONFIG_EXTENSION).unwrap_or(config);
    let schema_file = format!("{stem}{SCHEMA_SOURCE_EXTENSION}");
    let folder = kprofile_folder.join(KPROFILE_CONFIGS_FAMILY.folder);
    let Ok(entries) = std::fs::read_dir(&folder) else {
        return Ok(Vec::new());
    };
    let mut names: Vec<String> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| parse_filename(name).render_stripped() == schema_file)
        .collect();
    names.sort();
    let mut schemas = Vec::with_capacity(names.len());
    for name in names {
        let path = folder.join(&name);
        let text =
            std::fs::read_to_string(&path).map_err(|error| UpgradeError::SchemaReadFailed {
                path: path.display().to_string(),
                cause: error.to_string(),
            })?;
        schemas.push(Schema::parse(&name, &text).map_err(TdError::from)?);
    }
    Ok(schemas)
}

fn open_mappings_of(
    version: &str,
    entry: &str,
    content: &str,
    schemas: &[Schema],
) -> CommandResult<OpenMappings> {
    let document: Value =
        serde_yaml::from_str(content).map_err(|error| UpgradeError::ModifyParseFailed {
            version: version.to_owned(),
            entry: entry.to_owned(),
            cause: error.to_string(),
        })?;
    let mut found = OpenMappings::new();
    collect_open_mappings(schemas, &document, &mut Vec::new(), &mut found);
    Ok(found)
}

fn collect_open_mappings(
    schemas: &[Schema],
    node: &Value,
    path: &mut Vec<String>,
    found: &mut OpenMappings,
) {
    match node {
        Value::Mapping(entries) => {
            if schemas.iter().any(|schema| schema.open_at(path)) {
                found.insert(path.join(PATH_SEPARATOR), node.clone());
            }
            for (key, child) in entries {
                path.push(scalar_text(key).unwrap_or_default());
                collect_open_mappings(schemas, child, path, found);
                path.pop();
            }
        }
        Value::Sequence(items) => {
            for (index, item) in items.iter().enumerate() {
                path.push(item_identity(item, index));
                collect_open_mappings(schemas, item, path, found);
                path.pop();
            }
        }
        _ => {}
    }
}

fn item_identity(item: &Value, index: usize) -> String {
    item.as_mapping()
        .and_then(|entries| entries.iter().next())
        .and_then(|(_, value)| scalar_text(value))
        .unwrap_or_else(|| index.to_string())
}

fn scalar_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
