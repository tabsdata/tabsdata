//
// Copyright 2026 Tabs Data Inc.
//

use once_cell::sync::Lazy;
use regex::Regex;

use crate::base::constants::{
    Family, KUBE_NAME_SEPARATOR, TABSDATA_CONFIGMAP_PREFIX, TABSDATA_NAME_PREFIX,
};
use crate::base::error::naming::{NameSegment, NamingError};

const KUBE_IDENTIFIER_MAX_LENGTH: usize = 253;

static IDENTIFIER_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[a-z0-9]+$").expect("invalid identifier pattern"));

static DNS_SUBDOMAIN_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$").expect("invalid dns subdomain pattern")
});

pub fn to_configmap_name(family: &Family, file: &str) -> Result<String, NamingError> {
    let item = family.item;
    let mut name = String::with_capacity(
        TABSDATA_CONFIGMAP_PREFIX.len() + item.len() + KUBE_NAME_SEPARATOR.len() + file.len(),
    );
    name.push_str(TABSDATA_CONFIGMAP_PREFIX);
    name.push_str(item);
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

pub fn to_configmap_file<'a>(family: &Family, name: &'a str) -> Result<&'a str, NamingError> {
    let mut prefix = String::with_capacity(
        TABSDATA_CONFIGMAP_PREFIX.len() + family.item.len() + KUBE_NAME_SEPARATOR.len(),
    );
    prefix.push_str(TABSDATA_CONFIGMAP_PREFIX);
    prefix.push_str(family.item);
    prefix.push_str(KUBE_NAME_SEPARATOR);
    name.strip_prefix(prefix.as_str())
        .ok_or_else(|| NamingError::InvalidConfigmapName {
            name: name.to_owned(),
        })
}

pub fn to_kube_identifier(
    component: &str,
    name: &str,
    runid: Option<&str>,
) -> Result<String, NamingError> {
    let mut identifier = String::from(TABSDATA_NAME_PREFIX);
    identifier.push_str(&normalize_segment(NameSegment::Component, component)?);
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
    let body = identifier.strip_prefix(TABSDATA_NAME_PREFIX).ok_or_else(|| {
        NamingError::InvalidSegmentCount {
            identifier: identifier.to_owned(),
            qsegments: 0,
        }
    })?;
    let segments: Vec<&str> = body.split(KUBE_NAME_SEPARATOR).collect();
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
