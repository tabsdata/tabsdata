//
// Copyright 2026 Tabs Data Inc.
//

use once_cell::sync::Lazy;
use regex::Regex;

use crate::base::constants::{KUBE_NAME_SEPARATOR, TABSDATA_CONFIGMAP_PREFIX};
use crate::base::error::naming::{NameSegment, NamingError};

const KUBE_IDENTIFIER_MAX_LENGTH: usize = 253;

static IDENTIFIER_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-z0-9]+$").expect("invalid identifier pattern"));

static DNS_SUBDOMAIN_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$").expect("invalid dns subdomain pattern")
});

pub fn configmap_name(family: &str, file: &str) -> Result<String, NamingError> {
    let mut name = String::with_capacity(
        TABSDATA_CONFIGMAP_PREFIX.len() + family.len() + KUBE_NAME_SEPARATOR.len() + file.len(),
    );
    name.push_str(TABSDATA_CONFIGMAP_PREFIX);
    name.push_str(family);
    name.push_str(KUBE_NAME_SEPARATOR);
    name.push_str(file);
    if !DNS_SUBDOMAIN_PATTERN.is_match(&name) {
        return Err(NamingError::InvalidConfigmapName { name });
    }
    if name.len() > KUBE_IDENTIFIER_MAX_LENGTH {
        return Err(NamingError::AssembledIdentifierTooLong {
            identifier: name,
            limit: KUBE_IDENTIFIER_MAX_LENGTH,
        });
    }
    Ok(name)
}

pub fn to_kube_identifier(
    component: &str,
    name: &str,
    runid: Option<&str>,
) -> Result<String, NamingError> {
    let mut identifier = normalize_segment(NameSegment::Component, component)?;
    identifier.push_str(KUBE_NAME_SEPARATOR);
    identifier.push_str(&normalize_segment(NameSegment::Name, name)?);
    if let Some(runid) = runid {
        identifier.push_str(KUBE_NAME_SEPARATOR);
        identifier.push_str(&normalize_segment(NameSegment::Runid, runid)?);
    }
    if identifier.len() > KUBE_IDENTIFIER_MAX_LENGTH {
        return Err(NamingError::AssembledIdentifierTooLong {
            identifier,
            limit: KUBE_IDENTIFIER_MAX_LENGTH,
        });
    }
    Ok(identifier)
}

pub fn from_kube_identifier(
    identifier: &str,
) -> Result<(String, String, Option<String>), NamingError> {
    let segments: Vec<&str> = identifier.split(KUBE_NAME_SEPARATOR).collect();
    match segments.as_slice() {
        [component, name] => Ok((
            normalize_segment(NameSegment::Component, component)?,
            normalize_segment(NameSegment::Name, name)?,
            None,
        )),
        [component, name, runid] => Ok((
            normalize_segment(NameSegment::Component, component)?,
            normalize_segment(NameSegment::Name, name)?,
            Some(normalize_segment(NameSegment::Runid, runid)?),
        )),
        _ => Err(NamingError::InvalidSegmentCount {
            identifier: identifier.to_owned(),
            qsegments: segments.len(),
        }),
    }
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
