//
// Copyright 2026 Tabsdata Inc.
//

use saphyr_parser::ScalarStyle;
use serde_yaml::Value;

use crate::client::tdkserver::injector::engine::document::Scalar;

const NULL_TEXT: &str = "null";

#[derive(Debug, Clone, PartialEq)]
pub enum Provided {
    Text(String),
    Typed(Value),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeafType {
    Text,
    Number,
    Bool,
    Null,
}

impl LeafType {
    pub fn of(scalar: &Scalar) -> Self {
        if scalar.style != ScalarStyle::Plain {
            return Self::Text;
        }
        if scalar.is_empty_plain() {
            return Self::Null;
        }
        match serde_yaml::from_str::<Value>(&scalar.value) {
            Ok(Value::Null) => Self::Null,
            Ok(Value::Bool(_)) => Self::Bool,
            Ok(Value::Number(_)) => Self::Number,
            _ => Self::Text,
        }
    }

    pub fn describe(self) -> &'static str {
        match self {
            Self::Text => "a string",
            Self::Number => "a number",
            Self::Bool => "a boolean",
            Self::Null => "empty",
        }
    }
}

pub fn placeholder_text(provided: &Provided) -> Result<String, String> {
    match provided {
        Provided::Text(text) => Ok(text.clone()),
        Provided::Typed(value) => typed_text(value),
    }
}

pub fn render_placeholder(text: &str, flow: bool) -> String {
    render_string(text, flow)
}

pub fn render_leaf(provided: &Provided, leaf: LeafType, flow: bool) -> Result<String, String> {
    match leaf {
        LeafType::Text => match provided {
            Provided::Typed(Value::Null) => Err("an empty value is not a string".to_owned()),
            _ => Ok(render_string(&placeholder_text(provided)?, flow)),
        },
        LeafType::Number => match parsed(provided)? {
            Value::Number(number) => Ok(number.to_string()),
            _ => Err("the value is not a number".to_owned()),
        },
        LeafType::Bool => match parsed(provided)? {
            Value::Bool(boolean) => Ok(boolean.to_string()),
            _ => Err("the value is not 'true' or 'false'".to_owned()),
        },
        LeafType::Null => match inferred(provided)? {
            Value::String(text) => Ok(render_string(&text, flow)),
            Value::Number(number) => Ok(number.to_string()),
            Value::Bool(boolean) => Ok(boolean.to_string()),
            Value::Null => Ok(NULL_TEXT.to_owned()),
            _ => Err("the value is not a single scalar".to_owned()),
        },
    }
}

fn typed_text(value: &Value) -> Result<String, String> {
    match value {
        Value::String(text) => Ok(text.clone()),
        Value::Number(number) => Ok(number.to_string()),
        Value::Bool(boolean) => Ok(boolean.to_string()),
        Value::Null => Ok(String::new()),
        _ => Err("the value is not a single scalar".to_owned()),
    }
}

fn parsed(provided: &Provided) -> Result<Value, String> {
    match provided {
        Provided::Text(text) => serde_yaml::from_str::<Value>(text.trim())
            .map_err(|_| "the value cannot be read as YAML".to_owned()),
        Provided::Typed(value) => Ok(value.clone()),
    }
}

fn inferred(provided: &Provided) -> Result<Value, String> {
    let Provided::Text(text) = provided else {
        return parsed(provided);
    };
    let trimmed = text.trim();
    let quoted = trimmed.starts_with(['\'', '"']);
    match serde_yaml::from_str::<Value>(trimmed) {
        Ok(value @ (Value::Null | Value::Bool(_) | Value::Number(_))) if !trimmed.is_empty() => {
            Ok(value)
        }
        Ok(Value::String(unquoted)) if quoted => Ok(Value::String(unquoted)),
        _ => Ok(Value::String(text.clone())),
    }
}

fn render_string(text: &str, flow: bool) -> String {
    if !flow && let Ok(rendered) = serde_yaml::to_string(&Value::String(text.to_owned())) {
        let rendered = rendered.trim_end_matches('\n');
        if !rendered.contains('\n') {
            return rendered.to_owned();
        }
    }
    serde_json::to_string(text).unwrap_or_else(|_| format!("{text:?}"))
}
