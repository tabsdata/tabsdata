//
// Copyright 2026 Tabsdata Inc.
//

use once_cell::sync::Lazy;
use regex::Regex;
use td_common::worker::{Decorator, WorkerName};

use crate::base::constants::{KUBE_NAME_SEPARATOR, Scope, TABSDATA_NAME_PREFIX};
use crate::base::error::naming::{NameSegment, NamingError};

pub fn worker_pod_stem(worker: &WorkerName) -> &'static str {
    match worker {
        WorkerName::Function => "function",
        WorkerName::StageTrigger => "stream",
        WorkerName::CronTrigger => "cron",
    }
}

pub fn worker_name_prefix(worker: &WorkerName, decorator: Option<&Decorator>) -> &'static str {
    match worker {
        WorkerName::Function => match decorator {
            Some(Decorator::Publisher) => "pub",
            Some(Decorator::Transformer) => "trf",
            Some(Decorator::Subscriber) => "sub",
            None => "fun",
        },
        WorkerName::StageTrigger => "stm",
        WorkerName::CronTrigger => "crn",
    }
}

const KUBE_IDENTIFIER_MAX_LENGTH: usize = 253;

static DNS_SUBDOMAIN_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$").expect("invalid dns subdomain pattern")
});

pub fn id_from_file(file: &str) -> &str {
    match file.find('.') {
        Some(index) => &file[..index],
        None => file,
    }
}

pub fn to_kube_resource_name(
    scope: Scope,
    id: &str,
    kind: &str,
    name: &str,
) -> Result<String, NamingError> {
    let scope_str = scope.as_str();
    let mut assembled = String::with_capacity(
        TABSDATA_NAME_PREFIX.len()
            + scope_str.len()
            + KUBE_NAME_SEPARATOR.len()
            + id.len()
            + KUBE_NAME_SEPARATOR.len()
            + kind.len()
            + KUBE_NAME_SEPARATOR.len()
            + name.len(),
    );
    assembled.push_str(TABSDATA_NAME_PREFIX);
    assembled.push_str(scope_str);
    assembled.push_str(KUBE_NAME_SEPARATOR);
    assembled.push_str(id);
    assembled.push_str(KUBE_NAME_SEPARATOR);
    assembled.push_str(kind);
    assembled.push_str(KUBE_NAME_SEPARATOR);
    assembled.push_str(name);
    if !DNS_SUBDOMAIN_PATTERN.is_match(&assembled) {
        return Err(NamingError::InvalidConfigmapName { name: assembled });
    }
    if assembled.len() > KUBE_IDENTIFIER_MAX_LENGTH {
        return Err(NamingError::AssembledIdentifierTooLong {
            identifier: assembled,
            limit: KUBE_IDENTIFIER_MAX_LENGTH,
        });
    }
    Ok(assembled)
}

pub fn to_connection_object_name(connection_id: &str) -> Result<String, NamingError> {
    let assembled = format!(
        "{TABSDATA_NAME_PREFIX}{}{KUBE_NAME_SEPARATOR}{}",
        Scope::Connection.as_str(),
        normalize_segment(NameSegment::Component, connection_id)?,
    );
    if !DNS_SUBDOMAIN_PATTERN.is_match(&assembled) {
        return Err(NamingError::InvalidConfigmapName { name: assembled });
    }
    if assembled.len() > KUBE_IDENTIFIER_MAX_LENGTH {
        return Err(NamingError::AssembledIdentifierTooLong {
            identifier: assembled,
            limit: KUBE_IDENTIFIER_MAX_LENGTH,
        });
    }
    Ok(assembled)
}

pub fn to_fn_pod_name(prefix: &str, worker_id: &str) -> Result<String, NamingError> {
    let assembled = format!(
        "{}{KUBE_NAME_SEPARATOR}{}",
        normalize_segment(NameSegment::Component, prefix)?,
        normalize_segment(NameSegment::WorkerId, worker_id)?,
    );
    if !DNS_SUBDOMAIN_PATTERN.is_match(&assembled) {
        return Err(NamingError::AssembledIdentifierInvalid {
            identifier: assembled,
        });
    }
    if assembled.len() > KUBE_IDENTIFIER_MAX_LENGTH {
        return Err(NamingError::AssembledIdentifierTooLong {
            identifier: assembled,
            limit: KUBE_IDENTIFIER_MAX_LENGTH,
        });
    }
    Ok(assembled)
}

pub fn name_from_kube_resource_name<'a>(
    kube_name: &'a str,
    scope: Scope,
    kind: &str,
) -> Result<&'a str, NamingError> {
    let scope_prefix = format!(
        "{TABSDATA_NAME_PREFIX}{}{KUBE_NAME_SEPARATOR}",
        scope.as_str()
    );
    let body = kube_name
        .strip_prefix(scope_prefix.as_str())
        .ok_or_else(|| NamingError::InvalidConfigmapName {
            name: kube_name.to_owned(),
        })?;
    let kind_marker = format!("{KUBE_NAME_SEPARATOR}{kind}{KUBE_NAME_SEPARATOR}");
    let position =
        body.rfind(kind_marker.as_str())
            .ok_or_else(|| NamingError::InvalidConfigmapName {
                name: kube_name.to_owned(),
            })?;
    Ok(&body[position + kind_marker.len()..])
}

fn normalize_segment(segment: NameSegment, value: &str) -> Result<String, NamingError> {
    // Reduce the value to a valid Kubernetes name segment: lowercase, keep only
    // ASCII alphanumerics. Logical ids carry separators (e.g. `WKR20260713.3`)
    // that a name segment forbids but that survive on labels/env, so drop them
    // here rather than reject. Callers passing already-clean ids are unaffected.
    let normalized: String = value
        .to_lowercase()
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .collect();
    if normalized.is_empty() {
        return Err(NamingError::InvalidNameSegment {
            segment,
            value: value.to_owned(),
            reason: "must contain at least one ASCII alphanumeric character".to_owned(),
        });
    }
    Ok(normalized)
}

#[cfg(test)]
mod tests;
