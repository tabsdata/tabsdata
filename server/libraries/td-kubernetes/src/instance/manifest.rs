//
// Copyright 2026 Tabs Data Inc.
//

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use semver::Version;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const MANIFEST_SCHEMA: u32 = 1;
pub const MANIFEST_FILE: &str = "_manifest.yaml";

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
    pub fn initial_install<I>(
        tabsdata_version: &str,
        now: DateTime<Utc>,
        entry_paths: I,
    ) -> Self
    where
        I: IntoIterator<Item = String>,
    {
        let stamp = VersionStamp {
            version: tabsdata_version.to_owned(),
            when: now,
            changed: true,
        };
        let entries = entry_paths
            .into_iter()
            .map(|path| {
                let entry = ManifestEntry {
                    state: EntryState::Active,
                    version: tabsdata_version.to_owned(),
                    created: now,
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

#[derive(Debug, Clone)]
pub struct EntryMismatch {
    pub path: String,
    pub version: String,
}

#[derive(Debug, Error)]
pub enum VersionMismatch {
    #[error("Failed to parse version '{value}': {source}")]
    Parse {
        value: String,
        #[source]
        source: semver::Error,
    },
    #[error(
        "Profile entries do not match the running tabsdata SDK version '{running}' on major.minor: {entries:?}"
    )]
    Mismatch {
        running: String,
        entries: Vec<EntryMismatch>,
    },
}

pub fn check_versions(manifest: &Manifest, running: &str) -> Result<(), VersionMismatch> {
    let running_version = Version::parse(running).map_err(|source| VersionMismatch::Parse {
        value: running.to_owned(),
        source,
    })?;
    let mut mismatches = Vec::new();
    for (path, entry) in &manifest.entries {
        if entry.state == EntryState::Deleted {
            continue;
        }
        let entry_version =
            Version::parse(&entry.version).map_err(|source| VersionMismatch::Parse {
                value: entry.version.clone(),
                source,
            })?;
        if entry_version.major != running_version.major
            || entry_version.minor != running_version.minor
        {
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
