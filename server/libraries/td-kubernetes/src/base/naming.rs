//
// Copyright 2026 Tabs Data Inc.
//

use once_cell::sync::Lazy;
use regex::Regex;
use td_common::worker::WorkerName;

use crate::base::constants::{KUBE_NAME_SEPARATOR, Scope, TABSDATA_NAME_PREFIX};
use crate::base::error::naming::{NameSegment, NamingError};

pub fn worker_pod_stem(worker: &WorkerName) -> &'static str {
    match worker {
        WorkerName::Function => "function",
        WorkerName::StageTrigger => "stream",
        WorkerName::CronTrigger => "cron",
    }
}

const KUBE_IDENTIFIER_MAX_LENGTH: usize = 253;

static IDENTIFIER_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-z0-9]+$").expect("invalid identifier pattern"));

static DNS_SUBDOMAIN_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$").expect("invalid dns subdomain pattern")
});

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

pub fn id_from_file(file: &str) -> &str {
    match file.find('.') {
        Some(index) => &file[..index],
        None => file,
    }
}

pub fn to_fn_pod_name(worker: &str, worker_id: &str) -> Result<String, NamingError> {
    let scope_str = Scope::Fn.as_str();
    let mut assembled = String::with_capacity(
        TABSDATA_NAME_PREFIX.len()
            + scope_str.len()
            + KUBE_NAME_SEPARATOR.len()
            + worker.len()
            + KUBE_NAME_SEPARATOR.len()
            + worker_id.len(),
    );
    assembled.push_str(TABSDATA_NAME_PREFIX);
    assembled.push_str(scope_str);
    assembled.push_str(KUBE_NAME_SEPARATOR);
    assembled.push_str(&normalize_segment(NameSegment::Component, worker)?);
    assembled.push_str(KUBE_NAME_SEPARATOR);
    assembled.push_str(&normalize_segment(NameSegment::WorkerId, worker_id)?);
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
    let lowered = value.to_lowercase();
    if !IDENTIFIER_PATTERN.is_match(&lowered) {
        return Err(NamingError::InvalidNameSegment {
            segment,
            value: value.to_owned(),
            reason: "must contain only ASCII alphanumeric characters".to_owned(),
        });
    }
    Ok(lowered)
}
