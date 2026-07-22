//
// Copyright 2026 Tabsdata Inc.
//

//! [`Entity`] — one schema unit (a table with its indices, views, triggers, seeds)
//! that `current/` assembles in FK-safe order.

use super::backend::Backend;
use super::sql::{SeedCurrent, TriggerSpec, ViewDef};
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
    fn create_views(&self, _b: &Backend) -> Vec<ViewDef> {
        vec![]
    }
    /// Trigger specs maintaining this entity.
    fn create_db_triggers(&self) -> Vec<TriggerSpec> {
        vec![]
    }
    /// `INSERT` seed statements, emitted in `ENTITIES` order.
    fn seeds(&self) -> Vec<InsertStatement> {
        vec![]
    }
    /// `_current` snapshot seeding specs.
    fn seed_current(&self) -> Vec<SeedCurrent> {
        vec![]
    }
}
