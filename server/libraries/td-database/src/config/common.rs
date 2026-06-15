//
// Copyright 2026 Tabs Data Inc.
//

//! Definitions common to all database backends. Backend-specific configuration
//! lives in the per-engine submodules (`postgres`, `sqlite`, `mysql`).

use std::fmt;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

/// Schema the tests isolate their objects in, mirroring the production contract
/// that every PostgreSQL connection names an explicit schema (no `public`
/// fallback). Single source for the test fixtures and the config unit tests.
#[cfg(any(test, feature = "test-utils"))]
pub const TEST_SCHEMA: &str = "td_test";

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
