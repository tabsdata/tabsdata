//
// Copyright 2026 Tabsdata Inc.
//

use std::path::Path;

use serde_yaml::{Mapping, Value};

use crate::base::error::injector::InjectorError;
use crate::client::tdkserver::injector::engine::address::{
    Address, Segment, display_path, normalize_file, parse_path, split_assignment,
};
use crate::client::tdkserver::injector::engine::value::Provided;
use crate::runtime::gateway::resolver::kind::PlaceholderKind;

pub const CONFIG_ARGUMENT: &str = "config";
pub const FILE_PREFIX: char = '@';
pub const ENV_PREFIX: &str = "env:";
pub const VAL_PREFIX: &str = "val:";

const REFERENCE_OPEN: &str = "${";
const REFERENCE_CLOSE: &str = "}";

const FILES_SECTION: &str = "files";
const VALUES_SECTION: &str = "values";

#[derive(Debug, Clone)]
pub struct Setting {
    pub address: Address,
    pub value: Provided,
    pub origin: String,
}

impl Setting {
    pub fn new(address: Address, value: Provided, origin: impl Into<String>) -> Self {
        Self {
            address,
            value,
            origin: origin.into(),
        }
    }

    pub fn names_category(&self, kind: PlaceholderKind, name: &str) -> bool {
        matches!(
            &self.address,
            Address::Category { kind: named, category }
                if category == name && named.is_none_or(|named| named == kind)
        )
    }

    pub fn names_key(&self, config: &str, keys: &[&str]) -> bool {
        matches!(
            &self.address,
            Address::Key { file, path }
                if file == config
                    && path.len() == keys.len()
                    && path
                        .iter()
                        .zip(keys)
                        .all(|(segment, key)| segment.selectors.is_empty() && segment.key == *key)
        )
    }
}

pub fn key_address(config: &str, keys: &[&str]) -> Address {
    Address::Key {
        file: config.to_owned(),
        path: keys
            .iter()
            .map(|key| Segment {
                key: (*key).to_owned(),
                selectors: Vec::new(),
            })
            .collect(),
    }
}

pub fn parse_settings(arguments: &[String]) -> Result<Vec<Setting>, InjectorError> {
    let mut settings = Vec::new();
    for argument in arguments {
        match argument.strip_prefix(FILE_PREFIX) {
            Some(file) => settings.extend(read_settings_file(Path::new(file.trim()))?),
            None => settings.push(parse_assignment(argument, |address| {
                format!("'--{CONFIG_ARGUMENT} {address}'")
            })?),
        }
    }
    Ok(settings)
}

fn parse_assignment(
    text: &str,
    origin_of: impl Fn(&str) -> String,
) -> Result<Setting, InjectorError> {
    let Some((address_text, value)) = split_assignment(text) else {
        return Err(InjectorError::InvalidSetting {
            origin: origin_of(text.trim()),
            reason: "it has no '=' between the address and the value".to_owned(),
        });
    };
    let origin = origin_of(address_text.trim());
    let address = Address::parse(address_text).map_err(|reason| InjectorError::InvalidSetting {
        origin: origin.clone(),
        reason,
    })?;
    let value = resolve_value(Provided::Text(value.to_owned()), &origin)?;
    Ok(Setting::new(address, value, origin))
}

fn read_settings_file(file: &Path) -> Result<Vec<Setting>, InjectorError> {
    let shown = file.display().to_string();
    let content =
        std::fs::read_to_string(file).map_err(|cause| InjectorError::SettingsFileReadFailed {
            file: shown.clone(),
            cause: cause.to_string(),
        })?;
    let document: Value =
        serde_yaml::from_str(&content).map_err(|cause| InjectorError::InvalidSettingsFile {
            file: shown.clone(),
            reason: cause.to_string(),
        })?;
    let invalid = |reason: &str| InjectorError::InvalidSettingsFile {
        file: shown.clone(),
        reason: reason.to_owned(),
    };
    match document {
        Value::Sequence(items) => items
            .iter()
            .map(|item| match item {
                Value::String(text) => {
                    parse_assignment(text, |address| entry_origin(address, &shown))
                }
                _ => Err(invalid(
                    "a list of settings holds only 'address=value' strings",
                )),
            })
            .collect(),
        Value::Mapping(root) => {
            let sections = root
                .keys()
                .filter(|key| matches!(key.as_str(), Some(FILES_SECTION | VALUES_SECTION)))
                .count();
            if sections == 0 {
                return compact_settings(&root, &shown);
            }
            if sections != root.len() {
                return Err(invalid(
                    "it mixes the 'files' and 'values' sections with compact settings",
                ));
            }
            let mut settings = Vec::new();
            if let Some(values) = root.get(FILES_SECTION) {
                let Value::Mapping(files) = values else {
                    return Err(invalid(
                        "section 'files' maps config file names to their keys",
                    ));
                };
                settings.extend(file_settings(files, &shown)?);
            }
            if let Some(values) = root.get(VALUES_SECTION) {
                let Value::Mapping(values) = values else {
                    return Err(invalid("section 'values' maps addresses to values"));
                };
                settings.extend(compact_settings(values, &shown)?);
            }
            Ok(settings)
        }
        Value::Null => Ok(Vec::new()),
        _ => Err(invalid(
            "it holds a mapping of settings or a list of 'address=value' strings",
        )),
    }
}

fn compact_settings(entries: &Mapping, file: &str) -> Result<Vec<Setting>, InjectorError> {
    entries
        .iter()
        .map(|(key, value)| {
            let address_text = mapping_key(key, file)?;
            let origin = entry_origin(&address_text, file);
            let address =
                Address::parse(&address_text).map_err(|reason| InjectorError::InvalidSetting {
                    origin: origin.clone(),
                    reason,
                })?;
            let value = scalar(value, &origin)?;
            Ok(Setting::new(
                address,
                resolve_value(value, &origin)?,
                origin,
            ))
        })
        .collect()
}

fn file_settings(files: &Mapping, file: &str) -> Result<Vec<Setting>, InjectorError> {
    let mut settings = Vec::new();
    for (key, keys) in files {
        let config = mapping_key(key, file)?;
        let config = normalize_file(&config).map_err(|reason| InjectorError::InvalidSetting {
            origin: entry_origin(&config, file),
            reason,
        })?;
        let Value::Mapping(keys) = keys else {
            return Err(InjectorError::InvalidSetting {
                origin: entry_origin(&config, file),
                reason: "a config file entry maps its keys to values".to_owned(),
            });
        };
        nested_settings(&config, keys, &[], file, &mut settings)?;
    }
    Ok(settings)
}

fn nested_settings(
    config: &str,
    keys: &Mapping,
    prefix: &[Segment],
    file: &str,
    settings: &mut Vec<Setting>,
) -> Result<(), InjectorError> {
    for (key, value) in keys {
        let key_text = mapping_key(key, file)?;
        let mut path = prefix.to_vec();
        let parsed = parse_path(&key_text).map_err(|reason| InjectorError::InvalidSetting {
            origin: entry_origin(&format!("{config}:{key_text}"), file),
            reason,
        })?;
        path.extend(parsed);
        let origin = entry_origin(&format!("{config}:{}", display_path(&path)), file);
        match value {
            Value::Mapping(children) if children.is_empty() => {
                return Err(InjectorError::InvalidSetting {
                    origin,
                    reason: "it has no value; only final nodes can be set".to_owned(),
                });
            }
            Value::Mapping(children) => {
                nested_settings(config, children, &path, file, settings)?;
            }
            _ => {
                let provided = scalar(value, &origin)?;
                let address = Address::Key {
                    file: config.to_owned(),
                    path,
                };
                settings.push(Setting::new(
                    address,
                    resolve_value(provided, &origin)?,
                    origin,
                ));
            }
        }
    }
    Ok(())
}

fn mapping_key(key: &Value, file: &str) -> Result<String, InjectorError> {
    match key {
        Value::String(text) => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(boolean) => Ok(boolean.to_string()),
        _ => Err(InjectorError::InvalidSettingsFile {
            file: file.to_owned(),
            reason: "its keys are addresses, file names or key names".to_owned(),
        }),
    }
}

fn scalar(value: &Value, origin: &str) -> Result<Provided, InjectorError> {
    match value {
        Value::String(_) | Value::Number(_) | Value::Bool(_) | Value::Null => {
            Ok(Provided::Typed(value.clone()))
        }
        _ => Err(InjectorError::ValueNotScalar {
            origin: origin.to_owned(),
        }),
    }
}

fn entry_origin(address: &str, file: &str) -> String {
    format!("'{address}' in '{file}'")
}

fn resolve_value(value: Provided, origin: &str) -> Result<Provided, InjectorError> {
    let text = match &value {
        Provided::Text(text) => text.as_str(),
        Provided::Typed(Value::String(text)) => text.as_str(),
        Provided::Typed(_) => return Ok(value),
    };
    if let Some(reference) = text.strip_prefix(ENV_PREFIX) {
        let reference = reference.trim();
        return if reference.contains(REFERENCE_OPEN) {
            expand_references(reference, origin).map(Provided::Text)
        } else {
            read_variable(reference, origin).map(Provided::Text)
        };
    }
    if let Some(literal) = text.strip_prefix(VAL_PREFIX) {
        return Ok(match value {
            Provided::Text(_) => Provided::Text(literal.to_owned()),
            _ => Provided::Typed(Value::String(literal.to_owned())),
        });
    }
    Ok(value)
}

fn expand_references(template: &str, origin: &str) -> Result<String, InjectorError> {
    let invalid = |reason: String| InjectorError::InvalidSetting {
        origin: origin.to_owned(),
        reason,
    };
    let mut expanded = String::new();
    let mut rest = template;
    while let Some(open) = rest.find(REFERENCE_OPEN) {
        expanded.push_str(&rest[..open]);
        let after = &rest[open + REFERENCE_OPEN.len()..];
        let close = after.find(REFERENCE_CLOSE).ok_or_else(|| {
            invalid(format!(
                "a '{REFERENCE_OPEN}' in '{ENV_PREFIX}{template}' has no closing \
                 '{REFERENCE_CLOSE}'"
            ))
        })?;
        let variable = after[..close].trim();
        if variable.is_empty() {
            return Err(invalid(format!(
                "'{ENV_PREFIX}{template}' has a '{REFERENCE_OPEN}{REFERENCE_CLOSE}' without a \
                 variable name"
            )));
        }
        expanded.push_str(&read_variable(variable, origin)?);
        rest = &after[close + REFERENCE_CLOSE.len()..];
    }
    expanded.push_str(rest);
    Ok(expanded)
}

fn read_variable(variable: &str, origin: &str) -> Result<String, InjectorError> {
    std::env::var(variable).map_err(|_| InjectorError::EnvironmentVariableMissing {
        origin: origin.to_owned(),
        variable: variable.to_owned(),
    })
}
