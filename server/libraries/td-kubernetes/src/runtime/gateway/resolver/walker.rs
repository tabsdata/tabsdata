//
// Copyright 2026 Tabs Data Inc.
//

#![allow(dead_code)]

//! General YAML placeholder walker.
//!
//! Parses a YAML document, walks its structure and, for every VALUE scalar that
//! contains one or more `${kind}` / `${kind:category}` placeholders, dispatches
//! each occurrence to a [`PlaceholderVisitor`]. Placeholders may be embedded in
//! literal text (`0.0.0.0:${secret}`) and a value may carry several (`${a}:${b}`).
//! The visitor (the resolver) decides what each placeholder becomes; this module
//! owns the traversal, the substring scan/splice, and the deterministic-addressing
//! rules so a placeholder is always reachable by a stable path between an authored
//! template and an edited file.
//!
//! Positions:
//! - the scalar value of a plain nested key (addressed by key path);
//! - an item of a SCALAR list (addressed positionally);
//! - the value of a key inside a map that is a list item, when each list entry's
//!   first key/value is a unique identity within the list (addressed
//!   `list[firstValue].key`).
//!
//! A list of maps with differing first keys or duplicated first values, or a list
//! mixing scalar and map items, cannot carry placeholders and is rejected.

use std::collections::HashMap;

use once_cell::sync::Lazy;
use regex::Regex;
use serde_yaml::Value;

use crate::base::error::gateway::ResolverError;

pub(super) const PATH_SEPARATOR: &str = ".";

const KIND_PATTERN: &str = r"[A-Za-z][A-Za-z0-9]*";
const CATEGORY_PATTERN: &str = r"[A-Za-z0-9._-]*";
const RULE_PATTERN: &str = r"\?";

const PLACEHOLDER_OPENER: &str = "${";
const PLACEHOLDER_CLOSER: &str = "}";
const CATEGORY_SEPARATOR: &str = ":";
const OPTIONAL_RULE_MARKER: &str = "?";

static PLACEHOLDER_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(&format!(
        r"\$\{{({KIND_PATTERN})(?::({CATEGORY_PATTERN}))?({RULE_PATTERN})?\}}"
    ))
    .expect("The placeholder pattern is invalid.")
});

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Rule {
    #[default]
    Mandatory,
    Optional,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placeholder {
    pub kind: String,
    pub category: Option<String>,
    pub rule: Rule,
}

impl Placeholder {
    pub fn token(&self) -> String {
        let mut token = String::from(PLACEHOLDER_OPENER);
        token.push_str(&self.kind);
        if let Some(category) = &self.category {
            token.push_str(CATEGORY_SEPARATOR);
            token.push_str(category);
        }
        if self.rule == Rule::Optional {
            token.push_str(OPTIONAL_RULE_MARKER);
        }
        token.push_str(PLACEHOLDER_CLOSER);
        token
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretEntry {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    pub rendered: String,
    pub secret: Option<SecretEntry>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Creation,
    Execution,
}

pub struct ResolveContext<'a> {
    pub path: &'a [String],
    pub occurrence: usize,
    pub placeholder: &'a Placeholder,
    pub init_value: Option<&'a str>,
    pub system: &'a super::SystemValues,
}

pub trait ConcreteResolver {
    fn kind(&self) -> super::kind::PlaceholderKind;
    fn resolve(&self, context: &ResolveContext) -> Result<Resolution, ResolverError>;
    fn materialize(&self, context: &ResolveContext) -> Result<Resolution, ResolverError>;
}

pub fn contains_placeholder(text: &str) -> bool {
    PLACEHOLDER_PATTERN.is_match(text)
}

fn placeholder_from_captures(captures: &regex::Captures) -> Option<Placeholder> {
    let kind = captures.get(1)?;
    let rule = match captures.get(3) {
        Some(_) => Rule::Optional,
        None => Rule::Mandatory,
    };
    Some(Placeholder {
        kind: kind.as_str().to_owned(),
        category: captures.get(2).map(|category| category.as_str().to_owned()),
        rule,
    })
}

pub fn parse_bare_placeholder(text: &str) -> Option<Placeholder> {
    let captures = PLACEHOLDER_PATTERN.captures(text)?;
    let matched = captures.get(0)?;
    if matched.start() != 0 || matched.end() != text.len() {
        return None;
    }
    placeholder_from_captures(&captures)
}

pub fn placeholder_occurrences(text: &str) -> Vec<Placeholder> {
    PLACEHOLDER_PATTERN
        .captures_iter(text)
        .filter_map(|captures| placeholder_from_captures(&captures))
        .collect()
}

pub fn splice_placeholders<F>(text: &str, mut render: F) -> Result<String, ResolverError>
where
    F: FnMut(usize, &Placeholder) -> Result<String, ResolverError>,
{
    let mut result = String::with_capacity(text.len());
    let mut last = 0usize;
    let mut occurrence = 0usize;
    for captures in PLACEHOLDER_PATTERN.captures_iter(text) {
        let Some(matched) = captures.get(0) else {
            continue;
        };
        let Some(placeholder) = placeholder_from_captures(&captures) else {
            continue;
        };
        result.push_str(&text[last..matched.start()]);
        result.push_str(&render(occurrence, &placeholder)?);
        last = matched.end();
        occurrence += 1;
    }
    result.push_str(&text[last..]);
    Ok(result)
}

pub trait PlaceholderVisitor {
    fn visit_placeholder(
        &mut self,
        path: &[String],
        occurrence: usize,
        placeholder: &Placeholder,
    ) -> Result<String, ResolverError>;
}

pub fn walk<V: PlaceholderVisitor>(node: &mut Value, visitor: &mut V) -> Result<(), ResolverError> {
    let mut path: Vec<String> = Vec::new();
    walk_node(node, &mut path, visitor)
}

fn walk_node<V: PlaceholderVisitor>(
    node: &mut Value,
    path: &mut Vec<String>,
    visitor: &mut V,
) -> Result<(), ResolverError> {
    match node {
        Value::String(text) => {
            *text = substitute_scalar(text, path, visitor)?;
            Ok(())
        }
        Value::Mapping(map) => {
            for (key, value) in map.iter_mut() {
                let segment = mapping_key_as_string(key);
                path.push(segment);
                walk_node(value, path, visitor)?;
                path.pop();
            }
            Ok(())
        }
        Value::Sequence(items) => walk_sequence(items, path, visitor),
        _ => Ok(()),
    }
}

fn substitute_scalar<V: PlaceholderVisitor>(
    text: &str,
    path: &[String],
    visitor: &mut V,
) -> Result<String, ResolverError> {
    if !contains_placeholder(text) {
        return Ok(text.to_owned());
    }
    let mut result = String::with_capacity(text.len());
    let mut last = 0usize;
    let mut occurrence = 0usize;
    for captures in PLACEHOLDER_PATTERN.captures_iter(text) {
        let (Some(matched), Some(kind)) = (captures.get(0), captures.get(1)) else {
            continue;
        };
        let rule = match captures.get(3) {
            Some(_) => Rule::Optional,
            None => Rule::Mandatory,
        };
        let placeholder = Placeholder {
            kind: kind.as_str().to_owned(),
            category: captures.get(2).map(|category| category.as_str().to_owned()),
            rule,
        };
        result.push_str(&text[last..matched.start()]);
        result.push_str(&visitor.visit_placeholder(path, occurrence, &placeholder)?);
        last = matched.end();
        occurrence += 1;
    }
    result.push_str(&text[last..]);
    Ok(result)
}

fn walk_sequence<V: PlaceholderVisitor>(
    items: &mut [Value],
    path: &mut Vec<String>,
    visitor: &mut V,
) -> Result<(), ResolverError> {
    if !sequence_contains_placeholder(items) {
        return Ok(());
    }

    let displayed_path = path.join(PATH_SEPARATOR);

    if items.iter().all(|item| matches!(item, Value::String(_))) {
        for (index, item) in items.iter_mut().enumerate() {
            if let Value::String(text) = item {
                path.push(index.to_string());
                *text = substitute_scalar(text, path, visitor)?;
                path.pop();
            }
        }
        return Ok(());
    }

    let identities = sequence_identities(items, &displayed_path)?;
    for (item, identity) in items.iter_mut().zip(identities.into_iter()) {
        path.push(identity);
        walk_node(item, path, visitor)?;
        path.pop();
    }
    Ok(())
}

fn sequence_identities(
    items: &[Value],
    displayed_path: &str,
) -> Result<Vec<String>, ResolverError> {
    let mut first_key: Option<String> = None;
    let mut seen_values: HashMap<String, usize> = HashMap::with_capacity(items.len());
    let mut identities: Vec<String> = Vec::with_capacity(items.len());

    for (item_index, item) in items.iter().enumerate() {
        let Value::Mapping(mapping) = item else {
            return Err(ResolverError::SequenceMixedItems {
                path: displayed_path.to_owned(),
                item_index,
            });
        };
        let (key_node, value_node) =
            mapping
                .iter()
                .next()
                .ok_or_else(|| ResolverError::SequenceEmptyItem {
                    path: displayed_path.to_owned(),
                    item_index,
                })?;
        let key_text = mapping_key_as_string(key_node);
        match first_key.as_ref() {
            None => first_key = Some(key_text.clone()),
            Some(expected) if expected == &key_text => {}
            Some(expected) => {
                return Err(ResolverError::SequenceFirstKeyMismatch {
                    path: displayed_path.to_owned(),
                    first_key: expected.clone(),
                    item_index,
                    other_key: key_text,
                });
            }
        }
        let value_text = match value_node {
            Value::String(text) if contains_placeholder(text) => {
                return Err(ResolverError::PlaceholderAsIdentity {
                    path: displayed_path.to_owned(),
                });
            }
            Value::String(text) => text.clone(),
            Value::Number(number) => number.to_string(),
            Value::Bool(boolean) => boolean.to_string(),
            _ => {
                return Err(ResolverError::SequenceFirstValueNonScalar {
                    path: displayed_path.to_owned(),
                    item_index,
                });
            }
        };
        if let Some(prior_index) = seen_values.get(&value_text) {
            return Err(ResolverError::SequenceDuplicateFirstValues {
                path: displayed_path.to_owned(),
                value: value_text,
                first_index: *prior_index,
                second_index: item_index,
            });
        }
        seen_values.insert(value_text.clone(), item_index);
        identities.push(value_text);
    }
    Ok(identities)
}

fn sequence_contains_placeholder(items: &[Value]) -> bool {
    items.iter().any(node_contains_placeholder)
}

fn node_contains_placeholder(node: &Value) -> bool {
    match node {
        Value::String(text) => contains_placeholder(text),
        Value::Mapping(map) => map.values().any(node_contains_placeholder),
        Value::Sequence(items) => sequence_contains_placeholder(items),
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
