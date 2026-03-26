//
// Copyright 2026 Tabs Data Inc.
//

#![allow(dead_code)] // some methods are not required yet

use sea_query::{Expr, QueryStatementWriter, SchemaStatementBuilder, SelectStatement, WithClause};

/// Database backend for SQL generation.
///
/// This enum is intentionally separate from `td_database::DatabaseBackend` to keep
/// `td-schema` independent of `td-database`. The conversion between the two is done
/// in `td-database`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    #[cfg(feature = "sqlite")]
    Sqlite,
    #[cfg(feature = "postgres")]
    Postgres,
    #[cfg(feature = "mysql")]
    MySql,
}

/// A view definition: name + SELECT statement + optional CTE.
pub(crate) struct ViewDef {
    pub name: &'static str,
    pub select: SelectStatement,
    pub with_clause: Option<WithClause>,
}

impl ViewDef {
    pub(crate) fn new(name: &'static str, select: SelectStatement) -> Self {
        Self {
            name,
            select,
            with_clause: None,
        }
    }

    pub(crate) fn with_cte(name: &'static str, select: SelectStatement, with: WithClause) -> Self {
        Self {
            name,
            select,
            with_clause: Some(with),
        }
    }
}

/// Number of backend variants.
pub const BACKEND_COUNT: usize = 3;

impl Backend {
    /// Return a stable index for this backend (Sqlite=0, Postgres=1, MySql=2).
    pub const fn index(self) -> usize {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite => 0,
            #[cfg(feature = "postgres")]
            Self::Postgres => 1,
            #[cfg(feature = "mysql")]
            Self::MySql => 2,
        }
    }

    /// Render a schema statement (CREATE TABLE, ALTER TABLE, CREATE INDEX, etc.) to SQL.
    pub fn build_schema(&self, stmt: impl SchemaStatementBuilder) -> String {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite => stmt.to_string(sea_query::SqliteQueryBuilder),
            #[cfg(feature = "postgres")]
            Self::Postgres => stmt.to_string(sea_query::PostgresQueryBuilder),
            #[cfg(feature = "mysql")]
            Self::MySql => stmt.to_string(sea_query::MysqlQueryBuilder),
        }
    }

    /// Render a query statement (INSERT, UPDATE, SELECT, DELETE) to SQL with values
    /// inlined (not parameterized).
    pub fn build_query(&self, stmt: impl QueryStatementWriter) -> String {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite => stmt.to_string(sea_query::SqliteQueryBuilder),
            #[cfg(feature = "postgres")]
            Self::Postgres => stmt.to_string(sea_query::PostgresQueryBuilder),
            #[cfg(feature = "mysql")]
            Self::MySql => stmt.to_string(sea_query::MysqlQueryBuilder),
        }
    }

    /// Render a view definition to a `CREATE VIEW name AS ...` statement.
    pub(crate) fn create_view(&self, view: &ViewDef) -> String {
        let sql = match &view.with_clause {
            Some(with) => self.build_query(view.select.clone().with(with.clone())),
            None => self.build_query(view.select.clone()),
        };
        format!("CREATE VIEW {} AS {}", view.name, sql)
    }

    /// Render a `DROP VIEW IF EXISTS name` statement.
    ///
    /// Useful in deltas when recreating views after table changes.
    pub(crate) fn drop_view(&self, name: &str) -> String {
        format!("DROP VIEW IF EXISTS {name}")
    }

    /// Render an `UPDATE tabsdata_system SET value = 'N' WHERE name = 'db_version'`
    /// statement. This must be the last statement in every delta.
    pub(crate) fn update_db_version(&self, version: usize) -> String {
        format!("UPDATE tabsdata_system SET value = '{version}' WHERE name = 'db_version'")
    }

    // --- Dialect helpers ---

    /// String concatenation: `||` on SQLite/Postgres, `CONCAT()` on MySQL.
    pub(crate) fn concat(&self, parts: &[&str]) -> String {
        match self {
            #[cfg(feature = "mysql")]
            Self::MySql => format!("CONCAT({})", parts.join(", ")),
            _ => parts.join(" || "),
        }
    }

    /// `COALESCE(name_col, '[' || id_col || ']')` — the common "name or ID fallback"
    /// pattern used across views, returned as a [`Expr`] for use in select lists.
    pub(crate) fn name_or_id(&self, name_col: &str, id_col: &str) -> Expr {
        Expr::cust(format!(
            "COALESCE({}, {})",
            name_col,
            self.concat(&["'['", id_col, "']'"])
        ))
    }

    /// JSON object aggregation as a [`Expr`] for use in select lists.
    pub(crate) fn json_agg_expr(&self, key: &str, val: &str) -> Expr {
        Expr::cust(self.json_object_agg(key, val))
    }

    /// JSON object aggregation from key/value pairs.
    pub(crate) fn json_object_agg(&self, key: &str, val: &str) -> String {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite => format!("json_group_object({key}, {val})"),
            #[cfg(feature = "postgres")]
            Self::Postgres => format!("jsonb_object_agg({key}, {val})"),
            #[cfg(feature = "mysql")]
            Self::MySql => format!("JSON_OBJECTAGG({key}, {val})"),
        }
    }

    /// Check if the database exists query for the `tabsdata_system` table.
    pub(crate) fn table_exists_query(&self, table_name: &str) -> String {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite => {
                format!("SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            }
            #[cfg(feature = "postgres")]
            Self::Postgres => format!(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = 'public' AND table_name = '{table_name}'"
            ),
            #[cfg(feature = "mysql")]
            Self::MySql => format!(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_name = '{table_name}'"
            ),
        }
    }

    /// Render a trigger that maintains a `_current` table from an append-only
    /// source table. On each INSERT into the source, the trigger upserts the
    /// new row into the target `_current` table, keyed by the partition column.
    ///
    /// The DML body varies per backend; the CREATE TRIGGER wrapper is raw SQL
    /// because sea_query does not support trigger creation.
    pub(crate) fn create_trigger(&self, trigger: &TriggerDef) -> String {
        let cols = trigger.columns.join(", ");
        let new_cols: Vec<String> = trigger.columns.iter().map(|c| format!("NEW.{c}")).collect();
        let new_vals = new_cols.join(", ");

        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite => {
                format!(
                    "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                     FOR EACH ROW BEGIN\n  \
                       DELETE FROM {target} WHERE {partition} = NEW.{partition};\n  \
                       INSERT INTO {target} ({cols}) VALUES ({new_vals});\n\
                     END",
                    name = trigger.name,
                    source = trigger.source_table,
                    target = trigger.target_table,
                    partition = trigger.partition_col,
                )
            }
            #[cfg(feature = "postgres")]
            Self::Postgres => {
                let updates: Vec<String> = trigger
                    .columns
                    .iter()
                    .filter(|c| **c != trigger.partition_col)
                    .map(|c| format!("{c} = EXCLUDED.{c}"))
                    .collect();
                let update_set = updates.join(", ");

                format!(
                    "CREATE OR REPLACE FUNCTION {name}_fn() RETURNS TRIGGER AS $$\n\
                     BEGIN\n  \
                       INSERT INTO {target} ({cols}) VALUES ({new_vals})\n  \
                       ON CONFLICT ({partition}) DO UPDATE SET {update_set};\n  \
                       RETURN NEW;\n\
                     END;\n\
                     $$ LANGUAGE plpgsql;\n\
                     CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                     FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
                    name = trigger.name,
                    source = trigger.source_table,
                    target = trigger.target_table,
                    partition = trigger.partition_col,
                )
            }
            #[cfg(feature = "mysql")]
            Self::MySql => {
                let updates: Vec<String> = trigger
                    .columns
                    .iter()
                    .filter(|c| **c != trigger.partition_col)
                    .map(|c| format!("{c} = NEW.{c}"))
                    .collect();
                let update_set = updates.join(", ");

                format!(
                    "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                     FOR EACH ROW BEGIN\n  \
                       INSERT INTO {target} ({cols}) VALUES ({new_vals})\n  \
                       ON DUPLICATE KEY UPDATE {update_set};\n\
                     END",
                    name = trigger.name,
                    source = trigger.source_table,
                    target = trigger.target_table,
                )
            }
        }
    }

    /// Render a `DROP TRIGGER IF EXISTS` statement (plus function drop for Postgres).
    pub(crate) fn drop_trigger(&self, trigger: &TriggerDef) -> String {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite => format!("DROP TRIGGER IF EXISTS {}", trigger.name),
            #[cfg(feature = "postgres")]
            Self::Postgres => format!(
                "DROP TRIGGER IF EXISTS {name} ON {source};\n\
                 DROP FUNCTION IF EXISTS {name}_fn()",
                name = trigger.name,
                source = trigger.source_table,
            ),
            #[cfg(feature = "mysql")]
            Self::MySql => format!("DROP TRIGGER IF EXISTS {}", trigger.name),
        }
    }
}

/// A trigger definition for maintaining a `_current` table from an append-only
/// source table. Analogous to [`ViewDef`] for views.
pub(crate) struct TriggerDef {
    /// Trigger name, e.g. `"functions__maintain_current"`.
    pub name: &'static str,
    /// Source (append-only) table name, e.g. `"functions"`.
    pub source_table: &'static str,
    /// Target `_current` table name, e.g. `"functions_current"`.
    pub target_table: &'static str,
    /// The partition column name, e.g. `"function_id"`.
    pub partition_col: &'static str,
    /// All column names (must match both source and target tables).
    pub columns: Vec<&'static str>,
}
