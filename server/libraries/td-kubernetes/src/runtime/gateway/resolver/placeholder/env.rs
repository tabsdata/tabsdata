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
use crate::runtime::gateway::resolver::kind::PlaceholderKind;
use crate::runtime::gateway::resolver::walker::{
    ConcreteResolver, PATH_SEPARATOR, Resolution, ResolveContext, Rule,
};

static PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\$\{env:(\w+)(\?)?}")
        .expect("The environment-variable placeholder pattern is invalid.")
});

#[allow(dead_code)]
pub(crate) fn resolve(text: &str) -> Result<String, TdError> {
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

pub(crate) struct EnvResolver;

impl EnvResolver {
    pub(crate) fn new() -> Self {
        Self
    }

    fn variable(&self, context: &ResolveContext) -> Result<String, ResolverError> {
        context.placeholder.category.clone().ok_or_else(|| {
            ResolverError::MissingPlaceholderCategory {
                path: context.path.join(PATH_SEPARATOR),
                kind: self.kind().as_str().to_owned(),
            }
        })
    }
}

impl ConcreteResolver for EnvResolver {
    fn kind(&self) -> PlaceholderKind {
        PlaceholderKind::Env
    }

    fn resolve(&self, context: &ResolveContext) -> Result<Resolution, ResolverError> {
        Ok(Resolution {
            rendered: context.placeholder.token(),
            secret: None,
        })
    }

    fn materialize(&self, context: &ResolveContext) -> Result<Resolution, ResolverError> {
        let variable = self.variable(context)?;
        let optional = context.placeholder.rule == Rule::Optional;
        let value = match env::var(&variable) {
            Ok(value) => value.trim().to_string(),
            Err(_) if optional => String::new(),
            Err(_) => return Err(ResolverError::EnvironmentVariableNotFound { variable }),
        };
        Ok(Resolution {
            rendered: value,
            secret: None,
        })
    }
}

pub(crate) fn materialize(text: &str) -> Result<String, TdError> {
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
