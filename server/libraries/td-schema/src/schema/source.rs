//
// Copyright 2026 Tabsdata Inc.
//

//! The pool-facing schema view: [`SchemaSource`] (structured schema content),
//! the script builders the pool drives ([`install_script`],
//! [`derived_scripts`]), and the shared `tabsdata_system` ledger DDL
//! ([`tabsdata_system_ddl`]). Each schema module (`current/`, `archive/vN/`)
//! implements the trait with its own `Source`.

use crate::TabsdataSystem;
use crate::schema::render::render_schema;
use sea_query::{ColumnDef, Index, IndexCreateStatement, Table};
use tracing::debug;

/// A derived object (view or trigger) as its create/drop statement pair, both
/// rendered from the same definition so they cannot disagree.
pub struct Derived {
    pub create: Vec<String>,
    pub drop: Vec<String>,
}

/// One rung of the ladder: the statements that migrate the content, and the
/// drops for the derived objects that rung retires. A version's own drop list
/// ([`derived_scripts`]) cannot name what it no longer declares, so the ladder
/// takes those from the rung that retired them.
#[derive(Default)]
pub struct Delta {
    pub up: Vec<String>,
    pub retired: Vec<String>,
}

/// Object-safe, structured view of a schema's content, so the pool composes the
/// install and the derived-object lifecycle for *any* component's schema — a
/// source cannot have a view or trigger the pool does not know about.
///
/// Parts return **only the schema's own content**: the pool creates the
/// `tabsdata_system` ledger ([`tabsdata_system_ddl`]) and writes `db_version`
/// itself, around them. The ledger resolves per-schema via each connection's
/// pinned `search_path`. A [`delta`](Self::delta) additionally bumps
/// `db_version` itself — see [`SchemaDelta`](super::SchemaDelta).
pub trait SchemaSource: Send + Sync + 'static {
    /// Version this binary builds to.
    fn current_version(&self) -> usize;
    /// `CREATE TABLE` statements, FK-safe order.
    fn tables(&self) -> Vec<String>;
    /// Triggers, creation order. Defaults to none.
    fn triggers(&self) -> Vec<Derived> {
        vec![]
    }
    /// Views, creation order (consumers after producers). Defaults to none.
    fn views(&self) -> Vec<Derived> {
        vec![]
    }
    /// Indexes, as derived objects: dropped before an upgrade ladder and
    /// rebuilt from the target after it, so a delta carries only columns and
    /// data and an upgraded database indexes exactly like a fresh install.
    /// Defaults to none.
    fn indexes(&self) -> Vec<Derived> {
        vec![]
    }
    /// Extended statistics, as derived objects on the same lifecycle as
    /// indexes. Defaults to none.
    fn statistics(&self) -> Vec<Derived> {
        vec![]
    }
    /// Seed inserts. Defaults to none.
    fn seeds(&self) -> Vec<String> {
        vec![]
    }
    /// PostgreSQL row-level security statements. Defaults to none.
    fn rls(&self) -> Vec<String> {
        vec![]
    }
    /// The migration of the schema's own content to `target`. Defaults to a
    /// no-op step.
    fn delta(&self, _target: usize) -> Delta {
        Delta::default()
    }
}

/// The fresh-install script: tables, triggers, views, indexes, seeds, RLS.
/// Triggers precede seeds so seed inserts fire them; RLS runs last so
/// `FORCE RLS` never rejects the un-scoped seed inserts.
pub fn install_script(source: &dyn SchemaSource) -> Vec<String> {
    let mut out = source.tables();
    let tables = out.len();
    out.extend(source.triggers().into_iter().flat_map(|d| d.create));
    out.extend(source.views().into_iter().flat_map(|d| d.create));
    out.extend(source.indexes().into_iter().flat_map(|d| d.create));
    out.extend(source.statistics().into_iter().flat_map(|d| d.create));
    out.extend(source.seeds());
    out.extend(source.rls());
    debug!(version = source.current_version(), tables, total = out.len(), "Install script composed");
    out
}

/// The derived-object lifecycle scripts, split from a single render of the
/// parts: drops (views in reverse creation order, so no `CASCADE` is needed,
/// then triggers, indexes and statistics) and creates (indexes, statistics,
/// triggers, then views).
/// The drop runs before a delta ladder — standing views block destructive
/// deltas on PostgreSQL, and a dropped table takes its indexes with it.
pub fn derived_scripts(source: &dyn SchemaSource) -> (Vec<String>, Vec<String>) {
    let triggers = source.triggers();
    let views = source.views();
    let indexes = source.indexes();
    let statistics = source.statistics();
    let mut drop: Vec<String> = views.iter().rev().flat_map(|d| d.drop.clone()).collect();
    drop.extend(triggers.iter().flat_map(|d| d.drop.clone()));
    drop.extend(indexes.iter().flat_map(|d| d.drop.clone()));
    drop.extend(statistics.iter().flat_map(|d| d.drop.clone()));
    let mut create: Vec<String> = indexes.into_iter().flat_map(|d| d.create).collect();
    create.extend(statistics.into_iter().flat_map(|d| d.create));
    create.extend(triggers.into_iter().flat_map(|d| d.create));
    create.extend(views.into_iter().flat_map(|d| d.create));
    (drop, create)
}

/// An index as a derived object: the create statement and the matching
/// `DROP INDEX IF EXISTS` (its table may already be gone by the time the
/// ladder's drops run).
pub fn index_derived(stmt: IndexCreateStatement) -> Derived {
    let name = stmt.get_index_spec().get_name().expect("every index is named").to_string();
    Derived {
        create: vec![render_schema(stmt)],
        drop: vec![render_schema(Index::drop().name(name).if_exists().to_owned())],
    }
}

/// The shared `tabsdata_system(name, value)` ledger DDL for `backend`. The pool
/// runs it (idempotently) before any schema's content — the single place the
/// ledger is created.
pub fn tabsdata_system_ddl() -> String {
    render_schema(
        Table::create()
            .table(TabsdataSystem::Table)
            .if_not_exists()
            .col(ColumnDef::new(TabsdataSystem::Name).text().not_null().primary_key())
            .col(ColumnDef::new(TabsdataSystem::Value).text().null())
            .to_owned(),
    )
}
