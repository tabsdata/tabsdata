//
// Copyright 2026 Tabs Data Inc.
//

//! [`DatabaseMode`] — where the database process runs, and the two predicates
//! the rest of the crate keys deployment behavior on.

use serde::{Deserialize, Serialize};

/// Where the database process runs.
///
/// `Embedded` the database runs in-process.
/// `Sidecar` the database runs as an alongside process.
/// `Remote` the database runs as a remote process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseMode {
    #[default]
    Embedded,
    Sidecar,
    Remote,
}

impl DatabaseMode {
    /// Whether we hold full control over the database — `Embedded` and `Sidecar`.
    /// In both we have a privileged system connection and provision/upgrade the
    /// schemas; `Remote` we never manage (app-only, externally provisioned).
    pub fn is_managed(self) -> bool {
        matches!(self, DatabaseMode::Embedded | DatabaseMode::Sidecar)
    }

    /// Whether we spawn the database server in-process — `Embedded` only. Embedded
    /// is also [`managed`](Self::is_managed); the extra bit is that we boot and own
    /// the process, and owner passwords are generated and persisted in sidecar
    /// files. `Sidecar` is managed but not spawned by us, so its owner passwords
    /// must be supplied via the `components:` overrides.
    pub fn is_embedded(self) -> bool {
        matches!(self, DatabaseMode::Embedded)
    }
}
