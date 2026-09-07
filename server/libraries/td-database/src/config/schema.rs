//
// Copyright 2026 Tabsdata Inc.
//

//! [`Schema`] — a PostgreSQL schema name, plus the shared test schema constant.

use std::fmt;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

/// PostgreSQL schema name. Mandatory on the serving path — there is no
/// `public` default, so each component's objects stay isolated in their own
/// schema.
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
