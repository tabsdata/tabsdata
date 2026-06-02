//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;
use std::path::Path;

use k8s_openapi::api::core::v1::Secret;

use crate::base::error::secret::SecretError;

#[derive(Debug)]
pub struct ParsedSecret {
    pub name: String,
    pub data: BTreeMap<String, String>,
    pub labels: BTreeMap<String, String>,
}

pub fn parse_secret_manifest(path: &Path) -> Result<ParsedSecret, SecretError> {
    let file = path.display().to_string();
    let text = std::fs::read_to_string(path).map_err(|error| SecretError::StageReadFailed {
        path: file.clone(),
        cause: error.to_string(),
    })?;
    let secret: Secret =
        serde_yaml::from_str(&text).map_err(|error| SecretError::ManifestParseFailed {
            file: file.clone(),
            cause: error.to_string(),
        })?;
    let name = secret
        .metadata
        .name
        .clone()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SecretError::ManifestMissingName { file: file.clone() })?;
    let mut data: BTreeMap<String, String> = BTreeMap::new();
    for (key, value) in secret.data.into_iter().flatten() {
        let utf8 = String::from_utf8(value.0).map_err(|error| {
            SecretError::ManifestParseFailed {
                file: file.clone(),
                cause: format!("Secret 'data.{key}' is not valid UTF-8: {error}"),
            }
        })?;
        data.insert(key, utf8);
    }
    for (key, value) in secret.string_data.into_iter().flatten() {
        data.insert(key, value);
    }
    let labels = secret.metadata.labels.unwrap_or_default();
    Ok(ParsedSecret { name, data, labels })
}

#[cfg(test)]
mod tests;
