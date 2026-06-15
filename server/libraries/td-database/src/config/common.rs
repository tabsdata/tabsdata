//
// Copyright 2026 Tabs Data Inc.
//

//! Definitions common to all database backends. Backend-specific configuration
//! lives in the per-engine submodules (`postgres`, `sqlite`, `mysql`).

use std::fmt;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

/// Schema the tests isolate their objects in. Uses the real apiserver schema so
/// tests run against the same schema name the production manager and resolver
/// expect, avoiding a test-only schema that diverges from production. Single
/// source for the test fixtures and the config unit tests.
#[cfg(any(test, feature = "test-utils"))]
pub const TEST_SCHEMA: &str = "td_apiserver";

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
    pub fn is_embedded(self) -> bool {
        matches!(self, DatabaseMode::Embedded | DatabaseMode::Sidecar)
    }
}

/// PostgreSQL schema name. Mandatory on the PostgreSQL path — there is no
/// `public` default, so each component's objects stay isolated in their own
/// schema. SQLite and MySQL have no `search_path` and never carry one.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Schema(String);

impl Schema {
    pub fn new(name: impl Into<String>) -> Self {
        Schema(name.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Deref for Schema {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for Schema {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for Schema {
    fn from(name: String) -> Self {
        Schema(name)
    }
}

impl From<&str> for Schema {
    fn from(name: &str) -> Self {
        Schema(name.to_owned())
    }
}
