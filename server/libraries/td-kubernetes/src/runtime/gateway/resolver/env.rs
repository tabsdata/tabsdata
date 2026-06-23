//
// Copyright 2026 Tabs Data Inc.
//

//! `${env:VAR}` resolution — substitutes process environment variables.
//!
//! Syntax:
//! - `${env:NAME}`  — required; errors if the variable is not set.
//! - `${env:NAME?}` — optional; resolves to the empty string if not set.
//!
//! Multiple placeholders per leaf are substituted left-to-right. Resolved
//! values and the final leaf are trimmed of surrounding whitespace.

use std::env;

use once_cell::sync::Lazy;
use regex::Regex;
use td_error::TdError;

use crate::base::error::gateway::ResolverError;

static PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"\$\{env:(\w+)(\?)?}").expect("invalid env regex"));

#[allow(dead_code)]
pub(super) fn resolve(text: &str) -> Result<String, TdError> {
    let mut current = text.to_string();
    while let Some(captures) = PATTERN.captures(&current) {
        let placeholder = captures.get(0).unwrap().as_str().to_string();
        let variable = captures.get(1).unwrap().as_str().to_string();
        let optional = captures.get(2).is_some();
        let value = match env::var(&variable) {
            Ok(value) => value.trim().to_string(),
            Err(_) if optional => String::new(),
            Err(_) => {
                return Err(ResolverError::EnvironmentVariableNotFound { variable }.into());
            }
        };
        current = current.replace(&placeholder, &value);
    }
    Ok(current.trim().to_string())
}

pub(super) fn materialize(text: &str) -> Result<String, TdError> {
    let mut current = text.to_string();
    while let Some(captures) = PATTERN.captures(&current) {
        let placeholder = captures.get(0).unwrap().as_str().to_string();
        let variable = captures.get(1).unwrap().as_str().to_string();
        let optional = captures.get(2).is_some();
        let value = match env::var(&variable) {
            Ok(value) => value.trim().to_string(),
            Err(_) if optional => String::new(),
            Err(_) => {
                return Err(ResolverError::EnvironmentVariableNotFound { variable }.into());
            }
        };
        current = current.replace(&placeholder, &value);
    }
    Ok(current.trim().to_string())
}
