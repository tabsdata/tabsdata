//
// Copyright 2026 Tabs Data Inc.
//

//! The pool-facing schema view: [`SchemaSource`] (object-safe schema content the
//! pool drives), the shared `tabsdata_system` ledger DDL ([`tabsdata_system_ddl`]),
//! and [`CurrentSource`] for the built-in schema.

use super::backend::Backend;
use super::version::SchemaVersion;
use super::{current, deltas};
use crate::TabsdataSystem;
use sea_query::{ColumnDef, Table};

/// Object-safe view of a schema's content the connection pool drives, so the pool
/// upgrades *any* component's schema without knowing which it is.
///
/// [`build`](Self::build)/[`delta`](Self::delta) return **only the schema's own
/// content**: the pool creates the `tabsdata_system` ledger ([`tabsdata_system_ddl`])
/// and writes `db_version` itself, around them. The ledger resolves per-schema via
/// each connection's pinned `search_path`.
pub trait SchemaSource: Send + Sync + 'static {
    /// Version this binary builds to.
    fn current_version(&self) -> usize;
    /// Statements creating the schema's own content from scratch.
    fn build(&self, backend: Backend) -> Vec<String>;
    /// Statements migrating the schema's own content to `target`. Defaults to none.
    fn delta(&self, _target: usize, _backend: Backend) -> Vec<String> {
        vec![]
    }
}

/// The shared `tabsdata_system(name, value)` ledger DDL for `backend`. The pool
/// runs it (idempotently) before any schema's content — the single place the
/// ledger is created.
pub fn tabsdata_system_ddl(backend: Backend) -> String {
    backend.build_schema(
        Table::create()
            .table(TabsdataSystem::Table)
            .if_not_exists()
            .col(ColumnDef::new(TabsdataSystem::Name).text().not_null().primary_key())
            .col(ColumnDef::new(TabsdataSystem::Value).text().null())
            .to_owned(),
    )
}

/// [`SchemaSource`] for the built-in schema ([`current`] + [`deltas`]).
pub struct CurrentSource;

impl SchemaSource for CurrentSource {
    fn current_version(&self) -> usize {
        current::Schema::VERSION
    }
    fn build(&self, backend: Backend) -> Vec<String> {
        current::Schema::build(backend)
    }
    fn delta(&self, target: usize, backend: Backend) -> Vec<String> {
        deltas::get(target, backend)
    }
}
