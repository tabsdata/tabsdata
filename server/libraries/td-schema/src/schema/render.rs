//
// Copyright 2026 Tabsdata Inc.
//

//! Rendering primitives: sea_query statements to PostgreSQL text, plus the
//! small expression helpers the entity definitions share.

use sea_query::{Expr, Iden, PostgresQueryBuilder, QueryStatementWriter, SchemaStatementBuilder};

/// Render a schema statement (CREATE TABLE, ALTER TABLE, CREATE INDEX, etc.).
pub fn render_schema(stmt: impl SchemaStatementBuilder) -> String {
    stmt.to_string(PostgresQueryBuilder)
}

/// Render a query statement (INSERT, UPDATE, SELECT, DELETE) with values
/// inlined (not parameterized).
pub fn render_query(stmt: impl QueryStatementWriter) -> String {
    stmt.to_string(PostgresQueryBuilder)
}

/// Render an `UPDATE tabsdata_system SET value = 'N' WHERE name = 'db_version'`
/// statement. This must be the last statement in every delta.
pub(crate) fn update_db_version(version: usize) -> String {
    format!("UPDATE tabsdata_system SET value = '{version}' WHERE name = 'db_version'")
}

/// `COALESCE(name_col, '[' || id_col || ']')` — the common "name or ID fallback"
/// pattern used across views, returned as an [`Expr`] for use in select lists.
/// Both arguments are `(table, column)` [`Iden`] tuples so call sites name
/// the columns via the same enums used elsewhere in the schema.
pub(crate) fn name_or_id<T1: Iden, C1: Iden, T2: Iden, C2: Iden>(name_col: (T1, C1), id_col: (T2, C2)) -> Expr {
    let name = format!("{}.{}", name_col.0.to_string(), name_col.1.to_string());
    let id = format!("{}.{}", id_col.0.to_string(), id_col.1.to_string());
    Expr::cust(format!("COALESCE({name}, '[' || {id} || ']')"))
}

/// JSON object aggregation from key/value pairs. Wrapped in COALESCE so
/// that an empty input row set produces an empty JSON object instead of
/// NULL — `jsonb_object_agg` returns NULL on no rows, and callers
/// consistently want `{}`. Without this, sqlx fails to decode the resulting
/// NULL into `Json<HashMap<...>>`.
pub(crate) fn json_object_agg(key: &str, val: &str) -> String {
    format!("COALESCE(jsonb_object_agg({key}, {val}), '{{}}'::jsonb)")
}
