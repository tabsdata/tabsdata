//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::HashMap;

use serde_yaml::Value;

use crate::base::error::secret::SecretError;

pub const SECRET_PLACEHOLDER: &str = "${secret}";
pub const SECRET_KEY_SEPARATOR: &str = "__";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretLeaf {
    pub path: Vec<String>,
    pub key: String,
}

pub fn extract_secret_leaves(
    config: &str,
    yaml: &Value,
) -> Result<Vec<SecretLeaf>, SecretError> {
    let mut paths: Vec<Vec<String>> = Vec::new();
    walk(config, yaml, &mut Vec::new(), &mut paths)?;

    let mut leaves: Vec<SecretLeaf> = Vec::with_capacity(paths.len());
    let mut seen: HashMap<String, String> = HashMap::with_capacity(paths.len());
    for path in paths {
        let key = encode_secret_key(config, &path)?;
        let displayed = path.join(".");
        if let Some(first) = seen.get(&key) {
            return Err(SecretError::KeyCollision {
                config: config.to_owned(),
                key,
                first: first.clone(),
                second: displayed,
            });
        }
        seen.insert(key.clone(), displayed);
        leaves.push(SecretLeaf { path, key });
    }
    Ok(leaves)
}

fn walk(
    config: &str,
    node: &Value,
    path: &mut Vec<String>,
    leaves: &mut Vec<Vec<String>>,
) -> Result<(), SecretError> {
    match node {
        Value::String(text) if text == SECRET_PLACEHOLDER => {
            leaves.push(path.clone());
            Ok(())
        }
        Value::Mapping(map) => {
            for (key, value) in map {
                let segment = mapping_key_as_string(key);
                path.push(segment);
                walk(config, value, path, leaves)?;
                path.pop();
            }
            Ok(())
        }
        Value::Sequence(items) => walk_promoted_sequence(config, items, path, leaves),
        _ => Ok(()),
    }
}

fn walk_promoted_sequence(
    config: &str,
    items: &[Value],
    path: &mut Vec<String>,
    leaves: &mut Vec<Vec<String>>,
) -> Result<(), SecretError> {
    if !sequence_contains_secret_placeholder(items) {
        return Ok(());
    }

    let displayed_path = path.join(".");
    let mut first_key: Option<String> = None;
    let mut seen_values: HashMap<String, usize> = HashMap::with_capacity(items.len());

    let item_first_keys: Vec<(String, String)> = items
        .iter()
        .enumerate()
        .map(|(item_index, item)| {
            let mapping = match item {
                Value::Mapping(map) => map,
                _ => {
                    return Err(SecretError::SequenceMixedItems {
                        config: config.to_owned(),
                        path: displayed_path.clone(),
                        item_index,
                    });
                }
            };
            let (key_node, value_node) = mapping.iter().next().ok_or_else(|| {
                SecretError::SequenceEmptyItem {
                    config: config.to_owned(),
                    path: displayed_path.clone(),
                    item_index,
                }
            })?;
            let key_text = mapping_key_as_string(key_node);
            match first_key.as_ref() {
                None => first_key = Some(key_text.clone()),
                Some(expected) if expected == &key_text => {}
                Some(expected) => {
                    return Err(SecretError::SequenceFirstKeyMismatch {
                        config: config.to_owned(),
                        path: displayed_path.clone(),
                        first_key: expected.clone(),
                        item_index,
                        other_key: key_text,
                    });
                }
            }
            let value_text = match value_node {
                Value::String(text) if text == SECRET_PLACEHOLDER => {
                    return Err(SecretError::PromotedKeyIsSecret {
                        config: config.to_owned(),
                        path: displayed_path.clone(),
                    });
                }
                Value::String(text) => text.clone(),
                Value::Number(number) => number.to_string(),
                Value::Bool(boolean) => boolean.to_string(),
                _ => {
                    return Err(SecretError::SequenceFirstValueNonScalar {
                        config: config.to_owned(),
                        path: displayed_path.clone(),
                        item_index,
                    });
                }
            };
            if let Some(prior_index) = seen_values.get(&value_text) {
                return Err(SecretError::SequenceDuplicateFirstValues {
                    config: config.to_owned(),
                    path: displayed_path.clone(),
                    value: value_text,
                    first_index: *prior_index,
                    second_index: item_index,
                });
            }
            seen_values.insert(value_text.clone(), item_index);
            Ok((key_text, value_text))
        })
        .collect::<Result<Vec<_>, _>>()?;

    for (item, (_first_key, item_id)) in items.iter().zip(item_first_keys.into_iter()) {
        path.push(item_id);
        walk(config, item, path, leaves)?;
        path.pop();
    }
    Ok(())
}

fn sequence_contains_secret_placeholder(items: &[Value]) -> bool {
    items.iter().any(node_contains_secret_placeholder)
}

fn node_contains_secret_placeholder(node: &Value) -> bool {
    match node {
        Value::String(text) => text == SECRET_PLACEHOLDER,
        Value::Mapping(map) => map.values().any(node_contains_secret_placeholder),
        Value::Sequence(items) => items.iter().any(node_contains_secret_placeholder),
        _ => false,
    }
}

fn mapping_key_as_string(key: &Value) -> String {
    match key {
        Value::String(text) => text.clone(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        other => format!("{other:?}"),
    }
}

pub fn encode_secret_key(config: &str, path: &[String]) -> Result<String, SecretError> {
    let mut segments: Vec<String> = Vec::with_capacity(path.len());
    for segment in path {
        if segment.is_empty() {
            return Err(SecretError::EmptySegment {
                config: config.to_owned(),
                path: path.join("."),
            });
        }
        segments.push(encode_segment(segment));
    }
    Ok(segments.join(SECRET_KEY_SEPARATOR))
}

fn encode_segment(segment: &str) -> String {
    let mut encoded: String = segment
        .chars()
        .map(|character| {
            let upper = character.to_ascii_uppercase();
            if upper.is_ascii_alphanumeric() || upper == '_' {
                upper
            } else {
                '_'
            }
        })
        .collect();
    if let Some(first) = encoded.chars().next()
        && first.is_ascii_digit()
    {
        encoded.insert(0, '_');
    }
    encoded
}

#[cfg(test)]
mod tests;
