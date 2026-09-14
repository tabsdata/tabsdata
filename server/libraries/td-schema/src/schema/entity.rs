//
// Copyright 2026 Tabsdata Inc.
//

//! [`Entity`] — one schema unit (a table with its indices, views, triggers, seeds)
//! that `current/` assembles in FK-safe order.

use super::source::Derived;
use super::sql::{PartialIndexDef, StatisticsDef, ViewDef};
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
    /// `CREATE INDEX … WHERE` statements: an index the planner may only use
    /// where its predicate holds. Rendered rather than built, so they arrive
    /// separately from [`create_indices`](Self::create_indices).
    fn create_partial_indices(&self) -> Vec<PartialIndexDef> {
        vec![]
    }
    /// `CREATE STATISTICS` objects on this entity's tables: the planner cannot
    /// see that a fact row's dimension columns are correlated, and without the
    /// real group count it sorts and spills where it should hash.
    fn create_statistics(&self) -> Vec<StatisticsDef> {
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
