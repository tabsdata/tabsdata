//
// Copyright 2026 Tabsdata Inc.
//

use serde_yaml::Value;

use crate::base::error::schema::SchemaError;
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::gateway::resolver::walker::{Placeholder, Rule as PlaceholderRule};

const DIRECTIVES_KEY: &str = "directives";
const AT_KEY: &str = "at";
const TREAT_KEY: &str = "treat";
const TREAT_SECRET: &str = "secret";
const TREAT_OPTIONAL: &str = "optional";
const PATTERN_SEPARATOR: char = '.';
const WILDCARD_TOKEN: &str = "*";
const LIST_WILDCARD_SUFFIX: &str = "[*]";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Treatment {
    Secret,
    Optional,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Segment {
    Wildcard,
    Literal(String),
}

#[derive(Debug, Clone)]
struct Rule {
    segments: Vec<Segment>,
    treatment: Treatment,
}

#[derive(Debug, Clone, Default)]
pub struct Schema {
    rules: Vec<Rule>,
}

impl Schema {
    pub fn parse(schema: &str, text: &str) -> Result<Self, SchemaError> {
        let document: Value =
            serde_yaml::from_str(text).map_err(|cause| SchemaError::ParseFailed {
                schema: schema.to_owned(),
                cause: cause.to_string(),
            })?;
        let mut rules = Vec::new();
        let entries = document
            .get(DIRECTIVES_KEY)
            .and_then(Value::as_sequence)
            .map(Vec::as_slice)
            .unwrap_or_default();
        for entry in entries {
            let at = entry
                .get(AT_KEY)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim();
            if at.is_empty() {
                return Err(SchemaError::EmptyPattern {
                    schema: schema.to_owned(),
                });
            }
            let treatment = match entry.get(TREAT_KEY).and_then(Value::as_str) {
                Some(TREAT_SECRET) => Treatment::Secret,
                Some(TREAT_OPTIONAL) => Treatment::Optional,
                other => {
                    return Err(SchemaError::UnsupportedTreatment {
                        schema: schema.to_owned(),
                        treatment: other.unwrap_or("").to_owned(),
                    });
                }
            };
            rules.push(Rule {
                segments: parse_pattern(at),
                treatment,
            });
        }
        Ok(Self { rules })
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    pub fn secret_at(&self, path: &[String]) -> bool {
        self.rules
            .iter()
            .any(|rule| rule.treatment == Treatment::Secret && matches_full(&rule.segments, path))
    }

    pub fn optional_at(&self, path: &[String]) -> bool {
        self.rules
            .iter()
            .any(|rule| rule.treatment == Treatment::Optional && matches_full(&rule.segments, path))
    }

    pub fn could_match_prefix(&self, path: &[String]) -> bool {
        self.rules
            .iter()
            .any(|rule| matches_prefix(&rule.segments, path))
    }

    pub fn rewrite_secrets(&self, value: &mut Value) {
        if self.is_empty() {
            return;
        }
        let mut path: Vec<String> = Vec::new();
        self.rewrite_node(value, &mut path);
    }

    fn rewrite_node(&self, node: &mut Value, path: &mut Vec<String>) {
        if !self.could_match_prefix(path) {
            return;
        }
        match node {
            Value::Mapping(map) => {
                for (key, child) in map.iter_mut() {
                    let segment = mapping_key_as_string(key);
                    path.push(segment);
                    if self.secret_at(path) {
                        *child = secret_token();
                    } else {
                        self.rewrite_node(child, path);
                    }
                    path.pop();
                }
            }
            Value::Sequence(items) => {
                for item in items.iter_mut() {
                    let identity = sequence_item_identity(item);
                    path.push(identity);
                    self.rewrite_node(item, path);
                    path.pop();
                }
            }
            _ => {}
        }
    }
}

fn secret_token() -> Value {
    let placeholder = Placeholder {
        kind: PlaceholderKind::Secret.as_str().to_owned(),
        category: None,
        rule: PlaceholderRule::Mandatory,
    };
    Value::String(placeholder.token())
}

fn mapping_key_as_string(key: &Value) -> String {
    match key {
        Value::String(text) => text.clone(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        other => format!("{other:?}"),
    }
}

fn sequence_item_identity(item: &Value) -> String {
    match item {
        Value::Mapping(map) => map
            .iter()
            .next()
            .map(|(_, value)| scalar_as_string(value))
            .unwrap_or_default(),
        _ => String::new(),
    }
}

fn scalar_as_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => number.to_string(),
        Value::Bool(boolean) => boolean.to_string(),
        _ => String::new(),
    }
}

fn parse_pattern(at: &str) -> Vec<Segment> {
    let mut segments = Vec::new();
    for raw in at.split(PATTERN_SEPARATOR) {
        match raw.strip_suffix(LIST_WILDCARD_SUFFIX) {
            Some(name) => {
                segments.push(Segment::Literal(name.to_owned()));
                segments.push(Segment::Wildcard);
            }
            None if raw == WILDCARD_TOKEN => segments.push(Segment::Wildcard),
            None => segments.push(Segment::Literal(raw.to_owned())),
        }
    }
    segments
}

fn matches_full(segments: &[Segment], path: &[String]) -> bool {
    segments.len() == path.len() && zip_matches(segments, path)
}

fn matches_prefix(segments: &[Segment], path: &[String]) -> bool {
    segments.len() >= path.len() && zip_matches(&segments[..path.len()], path)
}

fn zip_matches(segments: &[Segment], path: &[String]) -> bool {
    segments
        .iter()
        .zip(path)
        .all(|(segment, actual)| match segment {
            Segment::Wildcard => true,
            Segment::Literal(literal) => literal == actual,
        })
}

#[cfg(test)]
mod tests;
