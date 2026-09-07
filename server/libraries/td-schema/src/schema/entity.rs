//
// Copyright 2026 Tabsdata Inc.
//

//! [`Entity`] — one schema unit (a table with its indices, views, triggers, seeds)
//! that `current/` assembles in FK-safe order.

use super::source::Derived;
use super::sql::ViewDef;
use sea_query::{IndexCreateStatement, InsertStatement, TableCreateStatement};

/// One coherent piece of the schema: a table (or an event-log + `_current`
/// snapshot pair) plus its indices, views, triggers, and seeds.
///
/// Implementors are zero-sized `Defs` markers; `current/mod.rs`'s ordered
/// `ENTITIES` slice fixes FK-safe table order and per-phase emission order. Every
/// method defaults to empty — override only the phases this entity participates in.
pub(crate) trait Entity {
    /// `CREATE TABLE` statements.
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![]
    }
    /// `CREATE INDEX` statements on this entity's tables.
    fn create_indices(&self) -> Vec<IndexCreateStatement> {
        vec![]
    }
    /// `CREATE VIEW` statements driven from this entity's tables (cross-entity
    /// views live in the last `ENTITIES` member, so a single pass stays dep-safe).
    fn create_views(&self) -> Vec<ViewDef> {
        vec![]
    }
    /// Triggers maintaining this entity, rendered.
    fn create_db_triggers(&self) -> Vec<Derived> {
        vec![]
    }
    /// `INSERT` seed statements, emitted in `ENTITIES` order.
    fn seeds(&self) -> Vec<InsertStatement> {
        vec![]
    }
    /// Seed statements that are not plain inserts, emitted after `seeds`.
    fn seed_statements(&self) -> Vec<String> {
        vec![]
    }
}
