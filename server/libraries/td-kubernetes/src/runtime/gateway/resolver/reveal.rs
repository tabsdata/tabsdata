//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::HashMap;

use serde_yaml::Value;

use crate::base::error::gateway::ResolverError;
use crate::runtime::secrets::walker::encode_secret_key;

use super::kind::PlaceholderKind;
use super::schema::Schema;
use super::walker::{Placeholder, Rule, parse_bare_placeholder, placeholder_occurrences};

const PATH_SEPARATOR: &str = ".";

pub fn reveal(
    template: &str,
    configmap: &str,
    schema: &Schema,
    values: bool,
    secret_values: &HashMap<String, String>,
) -> Result<String, ResolverError> {
    let template_value: Value = parse(template)?;
    let mut configmap_value: Value = parse(configmap)?;

    let placeholders = collect_template_placeholders(&template_value);

    let mut path: Vec<String> = Vec::new();
    transform_node(
        &mut configmap_value,
        &mut path,
        &placeholders,
        schema,
        values,
        secret_values,
    )?;

    if configmap_value.is_null() {
        return Ok(String::new());
    }
    serde_yaml::to_string(&configmap_value).map_err(|cause| ResolverError::ConfigSerializeFailed {
        cause: cause.to_string(),
    })
}

fn parse(text: &str) -> Result<Value, ResolverError> {
    serde_yaml::from_str(text).map_err(|cause| ResolverError::ConfigParseFailed {
        cause: cause.to_string(),
    })
}

pub fn prefill_secrets(
    input: &str,
    server_values: &HashMap<String, String>,
) -> Result<String, ResolverError> {
    let mut value: Value = parse(input)?;
    let mut path: Vec<String> = Vec::new();
    prefill_node(&mut value, &mut path, server_values)?;
    serde_yaml::to_string(&value).map_err(|cause| ResolverError::ConfigSerializeFailed {
        cause: cause.to_string(),
    })
}

fn prefill_node(
    node: &mut Value,
    path: &mut Vec<String>,
    server_values: &HashMap<String, String>,
) -> Result<(), ResolverError> {
    match node {
        Value::Mapping(map) => {
            let keys: Vec<Value> = map.keys().cloned().collect();
            for key in keys {
                path.push(mapping_key(&key));
                if let Some(child) = map.get_mut(&key) {
                    prefill_node(child, path, server_values)?;
                }
                path.pop();
            }
            Ok(())
        }
        Value::Sequence(items) => {
            for index in 0..items.len() {
                let segment = sequence_segment(&items[index], index);
                path.push(segment);
                prefill_node(&mut items[index], path, server_values)?;
                path.pop();
            }
            Ok(())
        }
        Value::String(text) => {
            let Some(placeholder) = parse_bare_placeholder(text) else {
                return Ok(());
            };
            let Ok(kind) = placeholder.kind.parse::<PlaceholderKind>() else {
                return Ok(());
            };
            if !matches!(kind, PlaceholderKind::Secret | PlaceholderKind::SecretAuto) {
                return Ok(());
            }
            let key = encode_secret_key(SECRET_CONSUMER, path).map_err(|cause| {
                ResolverError::SecretKeyEncodingFailed {
                    path: path.join(PATH_SEPARATOR),
                    cause: cause.to_string(),
                }
            })?;
            if let Some(value) = server_values.get(&key) {
                *node = Value::String(value.clone());
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

struct TemplateEntry {
    placeholder: Placeholder,
    text: String,
}

fn collect_template_placeholders(template: &Value) -> HashMap<String, TemplateEntry> {
    let mut placeholders = HashMap::new();
    let mut path: Vec<String> = Vec::new();
    collect_node(template, &mut path, &mut placeholders);
    placeholders
}

fn collect_node(
    node: &Value,
    path: &mut Vec<String>,
    placeholders: &mut HashMap<String, TemplateEntry>,
) {
    match node {
        Value::String(text) => {
            let placeholder = parse_bare_placeholder(text)
                .or_else(|| placeholder_occurrences(text).into_iter().next());
            if let Some(placeholder) = placeholder {
                placeholders.insert(
                    path.join(PATH_SEPARATOR),
                    TemplateEntry {
                        placeholder,
                        text: text.clone(),
                    },
                );
            }
        }
        Value::Mapping(map) => {
            for (key, value) in map {
                path.push(mapping_key(key));
                collect_node(value, path, placeholders);
                path.pop();
            }
        }
        Value::Sequence(items) => {
            for (index, item) in items.iter().enumerate() {
                path.push(sequence_segment(item, index));
                collect_node(item, path, placeholders);
                path.pop();
            }
        }
        _ => {}
    }
}

fn transform_node(
    node: &mut Value,
    path: &mut Vec<String>,
    placeholders: &HashMap<String, TemplateEntry>,
    schema: &Schema,
    values: bool,
    secret_values: &HashMap<String, String>,
) -> Result<(), ResolverError> {
    match node {
        Value::Mapping(map) => {
            let keys: Vec<Value> = map.keys().cloned().collect();
            for key in keys {
                path.push(mapping_key(&key));
                if let Some(child) = map.get_mut(&key) {
                    transform_node(child, path, placeholders, schema, values, secret_values)?;
                }
                path.pop();
            }
            Ok(())
        }
        Value::Sequence(items) => {
            for index in 0..items.len() {
                let segment = sequence_segment(&items[index], index);
                path.push(segment);
                transform_node(&mut items[index], path, placeholders, schema, values, secret_values)?;
                path.pop();
            }
            Ok(())
        }
        scalar => transform_scalar(scalar, path, placeholders, schema, values, secret_values),
    }
}

fn transform_scalar(
    node: &mut Value,
    path: &[String],
    placeholders: &HashMap<String, TemplateEntry>,
    schema: &Schema,
    values: bool,
    secret_values: &HashMap<String, String>,
) -> Result<(), ResolverError> {
    let kind = classify(path, placeholders, schema);
    let Some(kind) = kind else {
        return Ok(());
    };

    if kind.is_editable() {
        if matches!(kind, PlaceholderKind::Secret | PlaceholderKind::SecretAuto) {
            if values {
                let key = encode_secret_key(SECRET_CONSUMER, path).map_err(|cause| {
                    ResolverError::SecretKeyEncodingFailed {
                        path: path.join(PATH_SEPARATOR),
                        cause: cause.to_string(),
                    }
                })?;
                match secret_values.get(&key) {
                    Some(value) => *node = Value::String(value.clone()),
                    None => *node = Value::String(secret_token(path, placeholders)),
                }
            } else {
                *node = Value::String(secret_token(path, placeholders));
            }
        }
        return Ok(());
    }

    *node = Value::String(fixed_token(path, placeholders));
    Ok(())
}

fn classify(
    path: &[String],
    placeholders: &HashMap<String, TemplateEntry>,
    schema: &Schema,
) -> Option<PlaceholderKind> {
    if let Some(entry) = placeholders.get(&path.join(PATH_SEPARATOR)) {
        return entry.placeholder.kind.parse().ok();
    }
    if schema.secret_at(path) {
        return Some(PlaceholderKind::Secret);
    }
    None
}

fn secret_token(path: &[String], placeholders: &HashMap<String, TemplateEntry>) -> String {
    placeholders
        .get(&path.join(PATH_SEPARATOR))
        .map(|entry| entry.text.clone())
        .unwrap_or_else(|| {
            Placeholder {
                kind: PlaceholderKind::Secret.as_str().to_owned(),
                category: None,
                rule: Rule::Mandatory,
            }
            .token()
        })
}

fn fixed_token(path: &[String], placeholders: &HashMap<String, TemplateEntry>) -> String {
    placeholders
        .get(&path.join(PATH_SEPARATOR))
        .map(|entry| entry.text.clone())
        .unwrap_or_default()
}

const SECRET_CONSUMER: &str = "<consumer config>";

fn mapping_key(key: &Value) -> String {
    match key {
        Value::String(text) => text.clone(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        other => format!("{other:?}"),
    }
}

fn sequence_segment(item: &Value, index: usize) -> String {
    if let Value::Mapping(map) = item {
        if let Some((_, value)) = map.iter().next() {
            if let Some(identity) = scalar_as_string(value) {
                return identity;
            }
        }
    }
    index.to_string()
}

fn scalar_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(boolean) => Some(boolean.to_string()),
        _ => None,
    }
}
