//
// Copyright 2026 Tabsdata Inc.
//

//! The schema system, split by concern: `version` (authoring contract), `source`
//! (pool-facing view), `entity` (per-entity unit), `backend`/`sql` (dialect
//! rendering), and `current`/`archive`/`deltas` (the built-in schema).

pub mod archive;
pub mod backend;
pub mod current;
pub mod deltas;
mod entity;
mod source;
pub(crate) mod sql;
mod version;

#[cfg(test)]
pub(crate) mod tests;

pub use current::Source as CurrentSource;
pub(crate) use entity::Entity;
pub use source::{Derived, SchemaSource, derived_scripts, install_script, tabsdata_system_ddl};
pub use version::SchemaDelta;
