//
// Copyright 2026 Tabsdata Inc.
//

use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use pep440_rs::Version;
use serde::{Deserialize, Serialize};

use crate::base::error::manifest::{EntryMismatch, ManifestError, VersionMismatch};

pub const MANIFEST_SCHEMA: u32 = 1;
pub const MANIFEST_FILE: &str = "manifest.yaml";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub schema: u32,
    pub version: String,
    pub created: DateTime<Utc>,
    pub updated: DateTime<Utc>,
    pub entries: BTreeMap<String, ManifestEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ManifestEntry {
    pub state: EntryState,
    pub version: String,
    pub created: DateTime<Utc>,
    pub source: String,
    pub versions: Vec<VersionStamp>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryState {
    Active,
    Deleted,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionStamp {
    pub version: String,
    pub when: DateTime<Utc>,
    pub changed: bool,
}

impl Manifest {
    pub fn initial_install<I>(tabsdata_version: &str, now: DateTime<Utc>, entries: I) -> Self
    where
        I: IntoIterator<Item = (String, String)>,
    {
        let stamp = VersionStamp {
            version: tabsdata_version.to_owned(),
            when: now,
            changed: true,
        };
        let entries = entries
            .into_iter()
            .map(|(path, source)| {
                let entry = ManifestEntry {
                    state: EntryState::Active,
                    version: tabsdata_version.to_owned(),
                    created: now,
                    source,
                    versions: vec![stamp.clone()],
                };
                (path, entry)
            })
            .collect();
        Self {
            schema: MANIFEST_SCHEMA,
            version: tabsdata_version.to_owned(),
            created: now,
            updated: now,
            entries,
        }
    }
}

fn parse_version(value: &str) -> Result<Version, VersionMismatch> {
    Version::from_str(value).map_err(|error| VersionMismatch::Parse {
        value: value.to_owned(),
        cause: error.to_string(),
    })
}

#[cfg(test)]
mod tests;

impl Manifest {
    pub fn stamp_entry(&mut self, path: &str, version: &str, now: DateTime<Utc>, changed: bool) {
        let stamp = VersionStamp {
            version: version.to_owned(),
            when: now,
            changed,
        };
        match self.entries.get_mut(path) {
            Some(entry) => {
                entry.version = version.to_owned();
                entry.versions.push(stamp);
            }
            None => {
                self.entries.insert(
                    path.to_owned(),
                    ManifestEntry {
                        state: EntryState::Active,
                        version: version.to_owned(),
                        created: now,
                        source: path.to_owned(),
                        versions: vec![stamp],
                    },
                );
            }
        }
        self.updated = now;
    }

    pub fn drop_entry(&mut self, path: &str, now: DateTime<Utc>) -> bool {
        let removed = self.entries.remove(path).is_some();
        if removed {
            self.updated = now;
        }
        removed
    }

    pub fn settle(&mut self, version: &str, now: DateTime<Utc>) {
        self.version = version.to_owned();
        self.updated = now;
    }
}

pub fn serialize_manifest(manifest: &Manifest) -> Result<String, ManifestError> {
    serde_yaml::to_string(manifest)
        .map_err(|error| ManifestError::SerializeFailed(error.to_string()))
}

pub fn deserialize_manifest(yaml: &str) -> Result<Manifest, ManifestError> {
    serde_yaml::from_str(yaml).map_err(|error| ManifestError::ParseFailed(error.to_string()))
}

pub fn check_versions(manifest: &Manifest, running: &str) -> Result<(), VersionMismatch> {
    let running_version = parse_version(running)?;
    let mut mismatches = Vec::new();
    for (path, entry) in &manifest.entries {
        if entry.state == EntryState::Deleted {
            continue;
        }
        let entry_version = parse_version(&entry.version)?;
        if entry_version != running_version {
            mismatches.push(EntryMismatch {
                path: path.clone(),
                version: entry.version.clone(),
            });
        }
    }
    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(VersionMismatch::Mismatch {
            running: running.to_owned(),
            entries: mismatches,
        })
    }
}
