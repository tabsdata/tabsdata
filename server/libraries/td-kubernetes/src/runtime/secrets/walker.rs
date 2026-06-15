//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::HashMap;

use once_cell::sync::Lazy;
use regex::Regex;
use serde_yaml::Value;

use crate::base::constants::{
    SECRET_GENERATOR_JWT_SIGNING_KEY, SECRET_GENERATOR_POSTGRES_PASSWORD,
    SECRET_GENERATOR_POSTGRES_SYSTEM_USER,
};
use crate::base::error::secret::SecretError;

pub const SECRET_KEY_SEPARATOR: &str = "__";

static PLACEHOLDER_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^\$\{secret(?::([A-Za-z][A-Za-z0-9_]*))?\}$")
        .expect("invalid secret placeholder pattern")
});

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Generator {
    JwtSigningKey,
    PostgresSystemUser,
    PostgresPassword,
}

const EMBEDDED_POSTGRES_DEFAULT_USER: &str = "postgres";

impl Generator {
    pub fn from_name(name: &str) -> Option<Generator> {
        match name {
            SECRET_GENERATOR_JWT_SIGNING_KEY => Some(Generator::JwtSigningKey),
            SECRET_GENERATOR_POSTGRES_SYSTEM_USER => Some(Generator::PostgresSystemUser),
            SECRET_GENERATOR_POSTGRES_PASSWORD => Some(Generator::PostgresPassword),
            _ => None,
        }
    }

    pub fn supported() -> &'static [&'static str] {
        &[
            SECRET_GENERATOR_JWT_SIGNING_KEY,
            SECRET_GENERATOR_POSTGRES_SYSTEM_USER,
            SECRET_GENERATOR_POSTGRES_PASSWORD,
        ]
    }

    pub fn generate(self) -> String {
        match self {
            Generator::JwtSigningKey => td_common::id::id().to_string(),
            Generator::PostgresSystemUser => EMBEDDED_POSTGRES_DEFAULT_USER.to_owned(),
            Generator::PostgresPassword => td_common::random::password(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placeholder {
    Bare,
    Generated(Generator),
}

pub fn parse_placeholder(config: &str, text: &str) -> Result<Option<Placeholder>, SecretError> {
    let Some(captures) = PLACEHOLDER_PATTERN.captures(text) else {
        return Ok(None);
    };
    let Some(name) = captures.get(1) else {
        return Ok(Some(Placeholder::Bare));
    };
    match Generator::from_name(name.as_str()) {
        Some(generator) => Ok(Some(Placeholder::Generated(generator))),
        None => Err(SecretError::UnknownGeneratorName {
            config: config.to_owned(),
            name: name.as_str().to_owned(),
            supported: Generator::supported().join(", "),
        }),
    }
}

pub trait SecretVisitor {
    type Error: From<SecretError>;

    fn visit_placeholder(
        &mut self,
        config: &str,
        path: &[String],
        placeholder: Placeholder,
        scalar: &mut String,
    ) -> Result<(), Self::Error>;
}

pub fn walk<V: SecretVisitor>(
    config: &str,
    node: &mut Value,
    visitor: &mut V,
) -> Result<(), V::Error> {
    let mut path: Vec<String> = Vec::new();
    walk_node(config, node, &mut path, visitor)
}

fn walk_node<V: SecretVisitor>(
    config: &str,
    node: &mut Value,
    path: &mut Vec<String>,
    visitor: &mut V,
) -> Result<(), V::Error> {
    match node {
        Value::String(text) => {
            if let Some(placeholder) = parse_placeholder(config, text)? {
                visitor.visit_placeholder(config, path, placeholder, text)?;
            }
            Ok(())
        }
        Value::Mapping(map) => {
            for (key, value) in map.iter_mut() {
                let segment = mapping_key_as_string(key);
                path.push(segment);
                walk_node(config, value, path, visitor)?;
                path.pop();
            }
            Ok(())
        }
        Value::Sequence(items) => walk_promoted_sequence(config, items, path, visitor),
        _ => Ok(()),
    }
}

fn walk_promoted_sequence<V: SecretVisitor>(
    config: &str,
    items: &mut [Value],
    path: &mut Vec<String>,
    visitor: &mut V,
) -> Result<(), V::Error> {
    if !sequence_contains_secret_placeholder(config, items)? {
        return Ok(());
    }

    let displayed_path = path.join(".");
    let mut first_key: Option<String> = None;
    let mut seen_values: HashMap<String, usize> = HashMap::with_capacity(items.len());

    let item_first_keys: Vec<String> = items
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
            let (key_node, value_node) =
                mapping
                    .iter()
                    .next()
                    .ok_or_else(|| SecretError::SequenceEmptyItem {
                        config: config.to_owned(),
                        path: displayed_path.clone(),
                        item_index,
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
                Value::String(text) if parse_placeholder(config, text)?.is_some() => {
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
            Ok(value_text)
        })
        .collect::<Result<Vec<_>, _>>()?;

    for (item, item_id) in items.iter_mut().zip(item_first_keys.into_iter()) {
        path.push(item_id);
        walk_node(config, item, path, visitor)?;
        path.pop();
    }
    Ok(())
}

fn sequence_contains_secret_placeholder(
    config: &str,
    items: &[Value],
) -> Result<bool, SecretError> {
    for item in items {
        if node_contains_secret_placeholder(config, item)? {
            return Ok(true);
        }
    }
    Ok(false)
}

fn node_contains_secret_placeholder(config: &str, node: &Value) -> Result<bool, SecretError> {
    match node {
        Value::String(text) => Ok(parse_placeholder(config, text)?.is_some()),
        Value::Mapping(map) => {
            for value in map.values() {
                if node_contains_secret_placeholder(config, value)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Value::Sequence(items) => sequence_contains_secret_placeholder(config, items),
        _ => Ok(false),
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
