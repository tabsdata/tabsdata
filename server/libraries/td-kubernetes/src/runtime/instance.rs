//
// Copyright 2026 Tabsdata Inc.
//

pub mod cluster;
pub mod endpoint;
pub mod image;
pub mod injection;
pub mod launcher;
pub mod manifest;
pub mod ports;
pub mod profile;
pub mod staging;
pub mod tabsdata;
pub mod tier;

use td_python::tabsdata::tabsdata_version;

use crate::base::error::manifest::ManifestError;
use crate::runtime::instance::manifest::{VersionMismatch, check_versions, deserialize_manifest};

pub fn verify_manifest_compatibility(manifest_yaml: &str) -> Result<(), ManifestError> {
    let manifest = deserialize_manifest(manifest_yaml)
        .map_err(|error| ManifestError::ParseFailed(error.to_string()))?;
    let running = tabsdata_version()
        .map_err(|error| ManifestError::TabsdataVersionResolveFailed(error.to_string()))?;
    match check_versions(&manifest, &running) {
        Ok(()) => Ok(()),
        Err(VersionMismatch::Parse { value, source }) => {
            Err(ManifestError::VersionParseFailed(value, source.to_string()))
        }
        Err(VersionMismatch::Mismatch { running, entries }) => {
            let entries = entries
                .iter()
                .map(|entry| format!("  - {} (at version {})", entry.path, entry.version))
                .collect::<Vec<_>>()
                .join("\n");
            Err(ManifestError::VersionMismatches { running, entries })
        }
    }
}

pub fn verify_instance_version(instance: &str, found: &str) -> Result<(), ManifestError> {
    let running = tabsdata_version()
        .map_err(|error| ManifestError::TabsdataVersionResolveFailed(error.to_string()))?;
    if found == running {
        return Ok(());
    }
    Err(ManifestError::InstanceVersionMismatch {
        instance: instance.to_owned(),
        found: found.to_owned(),
        running,
    })
}
