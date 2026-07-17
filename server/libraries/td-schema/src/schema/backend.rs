//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{Expr, Iden, QueryStatementWriter, SchemaStatementBuilder};

/// Database backend for SQL generation.
///
/// This enum is intentionally separate from `td_database::DatabaseBackend` to keep
/// `td-schema` independent of `td-database`. The conversion between the two is done
/// in `td-database`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Sqlite,
    Postgres,
    MySql,
}

impl Backend {
    pub const ALL: [Backend; 3] = [Backend::Sqlite, Backend::Postgres, Backend::MySql];

    /// Render a schema statement (CREATE TABLE, ALTER TABLE, CREATE INDEX, etc.) to SQL.
    pub fn build_schema(&self, stmt: impl SchemaStatementBuilder) -> String {
        match self {
            Self::Sqlite => stmt.to_string(sea_query::SqliteQueryBuilder),
            Self::Postgres => stmt.to_string(sea_query::PostgresQueryBuilder),
            Self::MySql => stmt.to_string(sea_query::MysqlQueryBuilder),
        }
    }

    /// Render a query statement (INSERT, UPDATE, SELECT, DELETE) to SQL with values
    /// inlined (not parameterized).
    pub fn build_query(&self, stmt: impl QueryStatementWriter) -> String {
        match self {
            Self::Sqlite => stmt.to_string(sea_query::SqliteQueryBuilder),
            Self::Postgres => stmt.to_string(sea_query::PostgresQueryBuilder),
            Self::MySql => stmt.to_string(sea_query::MysqlQueryBuilder),
        }
    }

    /// Render an `UPDATE tabsdata_system SET value = 'N' WHERE name = 'db_version'`
    /// statement. This must be the last statement in every delta.
    #[allow(dead_code)] // delta machinery is in place but no deltas exist yet at the v1 baseline
    pub(crate) fn update_db_version(&self, version: usize) -> String {
        format!("UPDATE tabsdata_system SET value = '{version}' WHERE name = 'db_version'")
    }

    // --- Dialect helpers ---

    /// String concatenation: `||` on SQLite/Postgres, `CONCAT()` on MySQL.
    pub(crate) fn concat(&self, parts: &[&str]) -> String {
        match self {
            Self::MySql => format!("CONCAT({})", parts.join(", ")),
            _ => parts.join(" || "),
        }
    }

    /// `COALESCE(name_col, '[' || id_col || ']')` — the common "name or ID fallback"
    /// pattern used across views, returned as a [`Expr`] for use in select lists.
    /// Both arguments are `(table, column)` [`Iden`] tuples so call sites name
    /// the columns via the same enums used elsewhere in the schema.
    pub(crate) fn name_or_id<T1: Iden, C1: Iden, T2: Iden, C2: Iden>(&self, name_col: (T1, C1), id_col: (T2, C2)) -> Expr {
        let name = format!("{}.{}", name_col.0.to_string(), name_col.1.to_string());
        let id = format!("{}.{}", id_col.0.to_string(), id_col.1.to_string());
        Expr::cust(format!("COALESCE({}, {})", name, self.concat(&["'['", &id, "']'"])))
    }

    /// JSON object aggregation from key/value pairs. Wrapped in COALESCE so
    /// that an empty input row set produces an empty JSON object instead of
    /// NULL — every backend's native aggregator returns NULL on no rows, and
    /// callers consistently want `{}`. Without this, sqlx fails to decode the
    /// resulting NULL into `Json<HashMap<...>>` (postgres surfaces the issue
    /// readily; sqlite's looser decoding masks it until the same code runs on
    /// postgres).
    pub(crate) fn json_object_agg(&self, key: &str, val: &str) -> String {
        match self {
            Self::Sqlite => {
                format!("COALESCE(json_group_object({key}, {val}), '{{}}')")
            }
            Self::Postgres => {
                format!("COALESCE(jsonb_object_agg({key}, {val}), '{{}}'::jsonb)")
            }
            Self::MySql => {
                format!("COALESCE(JSON_OBJECTAGG({key}, {val}), JSON_OBJECT())")
            }
        }
    }
}
