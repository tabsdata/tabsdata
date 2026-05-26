//
// Copyright 2026 Tabs Data Inc.
//

use once_cell::sync::Lazy;
use regex::Regex;

use crate::base::constants::KUBE_IDENTIFIER_PREFIX;
use crate::base::error::naming::NamingError;

const KUBE_IDENTIFIER_MAX_LENGTH: usize = 253;

static IDENTIFIER_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"^[A-Za-z0-9]+$").expect("invalid identifier pattern"));

static KUBE_IDENTIFIER_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$")
        .expect("invalid kube identifier pattern")
});

static POD_SUFFIX_PATTERN: Lazy<Regex> =
    Lazy::new(|| Regex::new(r"-[a-z0-9]{5}$").expect("invalid pod suffix pattern"));

pub fn to_kube_identifier(identifier: &str) -> Result<String, NamingError> {
    if !IDENTIFIER_PATTERN.is_match(identifier) {
        return Err(NamingError::InvalidIdentifier(
            identifier.to_owned(),
            "must contain only ASCII alphanumeric characters".to_owned(),
        ));
    }
    let name = format!("{KUBE_IDENTIFIER_PREFIX}{}", identifier.to_lowercase());
    if name.len() > KUBE_IDENTIFIER_MAX_LENGTH {
        return Err(NamingError::InvalidIdentifier(
            identifier.to_owned(),
            format!("produced name exceeds {KUBE_IDENTIFIER_MAX_LENGTH} characters"),
        ));
    }
    if !KUBE_IDENTIFIER_PATTERN.is_match(&name) {
        return Err(NamingError::InvalidIdentifier(
            identifier.to_owned(),
            "produced name is not a valid kube identifier".to_owned(),
        ));
    }
    Ok(name)
}

pub fn from_kube_identifier(name: &str) -> Result<String, NamingError> {
    if name.len() > KUBE_IDENTIFIER_MAX_LENGTH {
        return Err(NamingError::InvalidKubeIdentifier(
            name.to_owned(),
            format!("exceeds {KUBE_IDENTIFIER_MAX_LENGTH} characters"),
        ));
    }
    if !KUBE_IDENTIFIER_PATTERN.is_match(name) {
        return Err(NamingError::InvalidKubeIdentifier(
            name.to_owned(),
            "not a valid kube identifier".to_owned(),
        ));
    }
    let stripped = name.strip_prefix(KUBE_IDENTIFIER_PREFIX).ok_or_else(|| {
        NamingError::MissingPrefix(name.to_owned(), KUBE_IDENTIFIER_PREFIX.to_owned())
    })?;
    let unsuffixed = match POD_SUFFIX_PATTERN.find(stripped) {
        Some(suffix) => &stripped[..suffix.start()],
        None => stripped,
    };
    Ok(unsuffixed.to_uppercase())
}
