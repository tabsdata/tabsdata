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
    Sqlite,
    Postgres,
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
            Self::Sqlite => 0,
            Self::Postgres => 1,
            Self::MySql => 2,
        }
    }

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
            Self::Sqlite => format!("json_group_object({key}, {val})"),
            Self::Postgres => format!("jsonb_object_agg({key}, {val})"),
            Self::MySql => format!("JSON_OBJECTAGG({key}, {val})"),
        }
    }

    /// Check if the database exists query for the `tabsdata_system` table.
    pub(crate) fn table_exists_query(&self, table_name: &str) -> String {
        match self {
            Self::Sqlite => {
                format!("SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
            }
            Self::Postgres => format!(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_schema = 'public' AND table_name = '{table_name}'"
            ),
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
    ///
    /// Returned as `Vec<String>` so Postgres can emit its supporting function
    /// (`CREATE OR REPLACE FUNCTION …`) and the trigger itself (`CREATE TRIGGER
    /// … EXECUTE FUNCTION …`) as separate statements — sqlx-postgres uses the
    /// extended query protocol and rejects multi-statement strings. SQLite and
    /// MySQL each return a single-element vec.
    pub(crate) fn create_trigger(&self, trigger: &TriggerDef) -> Vec<String> {
        let cols = trigger.columns.join(", ");
        let new_cols: Vec<String> = trigger.columns.iter().map(|c| format!("NEW.{c}")).collect();
        let new_vals = new_cols.join(", ");

        match self {
            Self::Sqlite => vec![format!(
                "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                 FOR EACH ROW BEGIN\n  \
                   DELETE FROM {target} WHERE {partition} = NEW.{partition};\n  \
                   INSERT INTO {target} ({cols}) VALUES ({new_vals});\n\
                 END",
                name = trigger.name,
                source = trigger.source_table,
                target = trigger.target_table,
                partition = trigger.partition_col,
            )],
            Self::Postgres => {
                let updates: Vec<String> = trigger
                    .columns
                    .iter()
                    .filter(|c| **c != trigger.partition_col)
                    .map(|c| format!("{c} = EXCLUDED.{c}"))
                    .collect();
                let update_set = updates.join(", ");

                vec![
                    format!(
                        "CREATE OR REPLACE FUNCTION {name}_fn() RETURNS TRIGGER AS $$\n\
                         BEGIN\n  \
                           INSERT INTO {target} ({cols}) VALUES ({new_vals})\n  \
                           ON CONFLICT ({partition}) DO UPDATE SET {update_set};\n  \
                           RETURN NEW;\n\
                         END;\n\
                         $$ LANGUAGE plpgsql",
                        name = trigger.name,
                        target = trigger.target_table,
                        partition = trigger.partition_col,
                    ),
                    format!(
                        "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                         FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
                        name = trigger.name,
                        source = trigger.source_table,
                    ),
                ]
            }
            Self::MySql => {
                let updates: Vec<String> = trigger
                    .columns
                    .iter()
                    .filter(|c| **c != trigger.partition_col)
                    .map(|c| format!("{c} = NEW.{c}"))
                    .collect();
                let update_set = updates.join(", ");

                vec![format!(
                    "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                     FOR EACH ROW BEGIN\n  \
                       INSERT INTO {target} ({cols}) VALUES ({new_vals})\n  \
                       ON DUPLICATE KEY UPDATE {update_set};\n\
                     END",
                    name = trigger.name,
                    source = trigger.source_table,
                    target = trigger.target_table,
                )]
            }
        }
    }

    /// Render `DROP TRIGGER IF EXISTS` (plus function drop for Postgres). Same
    /// single-statement-per-entry rule as [`create_trigger`].
    pub(crate) fn drop_trigger(&self, trigger: &TriggerDef) -> Vec<String> {
        match self {
            Self::Sqlite => vec![format!("DROP TRIGGER IF EXISTS {}", trigger.name)],
            Self::Postgres => vec![
                format!(
                    "DROP TRIGGER IF EXISTS {name} ON {source}",
                    name = trigger.name,
                    source = trigger.source_table,
                ),
                format!("DROP FUNCTION IF EXISTS {name}_fn()", name = trigger.name),
            ],
            Self::MySql => vec![format!("DROP TRIGGER IF EXISTS {}", trigger.name)],
        }
    }

    /// Render a `BEFORE UPDATE` trigger that refuses any transition out of a
    /// sticky-terminal set. Same-state transitions (idempotent) are allowed;
    /// any other target raises an error that the service layer surfaces.
    pub(crate) fn create_sticky_terminals_trigger(
        &self,
        trigger: &StickyTerminalsTriggerDef,
    ) -> Vec<String> {
        let values = trigger
            .terminal_values
            .iter()
            .map(|v| format!("'{v}'"))
            .collect::<Vec<_>>()
            .join(", ");
        match self {
            Self::Sqlite => vec![format!(
                "CREATE TRIGGER {name} BEFORE UPDATE OF {col} ON {table}\n\
                 FOR EACH ROW WHEN OLD.{col} IN ({values}) AND NEW.{col} != OLD.{col}\n\
                 BEGIN\n  \
                   SELECT RAISE(ABORT, '{table}.{col} is sticky in terminal state');\n\
                 END",
                name = trigger.name,
                table = trigger.source_table,
                col = trigger.status_col,
            )],
            Self::Postgres => vec![
                format!(
                    "CREATE OR REPLACE FUNCTION {name}_fn() RETURNS TRIGGER AS $$\n\
                     BEGIN\n  \
                       IF OLD.{col} IN ({values}) AND NEW.{col} != OLD.{col} THEN\n    \
                         RAISE EXCEPTION '{table}.{col} is sticky in terminal state';\n  \
                       END IF;\n  \
                       RETURN NEW;\n\
                     END;\n\
                     $$ LANGUAGE plpgsql",
                    name = trigger.name,
                    table = trigger.source_table,
                    col = trigger.status_col,
                ),
                format!(
                    "CREATE TRIGGER {name} BEFORE UPDATE OF {col} ON {table}\n\
                     FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
                    name = trigger.name,
                    table = trigger.source_table,
                    col = trigger.status_col,
                ),
            ],
            Self::MySql => vec![format!(
                "CREATE TRIGGER {name} BEFORE UPDATE ON {table}\n\
                 FOR EACH ROW\n\
                 BEGIN\n  \
                   IF OLD.{col} IN ({values}) AND NEW.{col} != OLD.{col} THEN\n    \
                     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '{table}.{col} is sticky in terminal state';\n  \
                   END IF;\n\
                 END",
                name = trigger.name,
                table = trigger.source_table,
                col = trigger.status_col,
            )],
        }
    }

    /// Render an `AFTER INSERT` trigger that maintains a `_current` table
    /// *guarded by a version column*. Differs from [`create_trigger`] in two
    /// ways:
    ///
    /// 1. The `_current` row is only replaced when `NEW.{version_col}` is
    ///    strictly greater than the existing row's `{version_col}`, with a
    ///    deterministic tie-break on `{tie_break_col}` (lexicographic) for
    ///    equal versions.
    /// 2. Out-of-order inserts (e.g. a back-dated patch) leave the current
    ///    view untouched.
    pub(crate) fn create_versioned_current_trigger(
        &self,
        trigger: &VersionedTriggerDef,
    ) -> Vec<String> {
        let cols = trigger.columns.join(", ");
        let new_cols: Vec<String> = trigger.columns.iter().map(|c| format!("NEW.{c}")).collect();
        let new_vals = new_cols.join(", ");
        match self {
            Self::Sqlite => vec![format!(
                "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                 FOR EACH ROW WHEN NOT EXISTS (\n  \
                   SELECT 1 FROM {target}\n  \
                   WHERE {partition} = NEW.{partition}\n    \
                     AND ({version} > NEW.{version}\n         \
                          OR ({version} = NEW.{version} AND {tie_break} >= NEW.{tie_break}))\n\
                 )\n\
                 BEGIN\n  \
                   DELETE FROM {target} WHERE {partition} = NEW.{partition};\n  \
                   INSERT INTO {target} ({cols}) VALUES ({new_vals});\n\
                 END",
                name = trigger.name,
                source = trigger.source_table,
                target = trigger.target_table,
                partition = trigger.partition_col,
                version = trigger.version_col,
                tie_break = trigger.tie_break_col,
            )],
            Self::Postgres => {
                let updates: Vec<String> = trigger
                    .columns
                    .iter()
                    .filter(|c| **c != trigger.partition_col)
                    .map(|c| format!("{c} = EXCLUDED.{c}"))
                    .collect();
                let update_set = updates.join(", ");
                vec![
                    format!(
                        "CREATE OR REPLACE FUNCTION {name}_fn() RETURNS TRIGGER AS $$\n\
                         BEGIN\n  \
                           INSERT INTO {target} ({cols}) VALUES ({new_vals})\n  \
                           ON CONFLICT ({partition}) DO UPDATE SET {update_set}\n  \
                           WHERE EXCLUDED.{version} > {target}.{version}\n     \
                              OR (EXCLUDED.{version} = {target}.{version}\n         \
                                  AND EXCLUDED.{tie_break} > {target}.{tie_break});\n  \
                           RETURN NEW;\n\
                         END;\n\
                         $$ LANGUAGE plpgsql",
                        name = trigger.name,
                        target = trigger.target_table,
                        partition = trigger.partition_col,
                        version = trigger.version_col,
                        tie_break = trigger.tie_break_col,
                    ),
                    format!(
                        "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                         FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
                        name = trigger.name,
                        source = trigger.source_table,
                    ),
                ]
            }
            Self::MySql => {
                // MySQL trigger bodies can't run `IF` over a SELECT FROM the
                // target table inside the trigger that fired the INSERT.
                // Use a stored procedure-style guard via `INSERT … SELECT`
                // and `ON DUPLICATE KEY UPDATE` with a `IF(NEW.version >
                // OLD.version, NEW.col, OLD.col)` per column.
                let updates: Vec<String> = trigger
                    .columns
                    .iter()
                    .filter(|c| **c != trigger.partition_col)
                    .map(|c| {
                        format!(
                            "{c} = IF(NEW.{v} > {target}.{v} OR (NEW.{v} = {target}.{v} AND NEW.{tb} > {target}.{tb}), NEW.{c}, {target}.{c})",
                            v = trigger.version_col,
                            tb = trigger.tie_break_col,
                            target = trigger.target_table,
                        )
                    })
                    .collect();
                let update_set = updates.join(", ");
                vec![format!(
                    "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                     FOR EACH ROW BEGIN\n  \
                       INSERT INTO {target} ({cols}) VALUES ({new_vals})\n  \
                       ON DUPLICATE KEY UPDATE {update_set};\n\
                     END",
                    name = trigger.name,
                    source = trigger.source_table,
                    target = trigger.target_table,
                )]
            }
        }
    }

    /// Render `BEFORE UPDATE` and `BEFORE DELETE` triggers that refuse any
    /// mutation on an insert-only event log. Returns two statements (the
    /// caller flattens them into the schema stream).
    pub(crate) fn create_immutable_triggers(&self, trigger: &ImmutableTriggerDef) -> Vec<String> {
        let upd_name = format!("{}_no_update", trigger.name);
        let del_name = format!("{}_no_delete", trigger.name);
        match self {
            Self::Sqlite => vec![
                format!(
                    "CREATE TRIGGER {upd_name} BEFORE UPDATE ON {table}\n\
                     FOR EACH ROW BEGIN\n  \
                       SELECT RAISE(ABORT, '{table} is insert-only');\n\
                     END",
                    table = trigger.source_table,
                ),
                format!(
                    "CREATE TRIGGER {del_name} BEFORE DELETE ON {table}\n\
                     FOR EACH ROW BEGIN\n  \
                       SELECT RAISE(ABORT, '{table} is insert-only');\n\
                     END",
                    table = trigger.source_table,
                ),
            ],
            Self::Postgres => vec![
                format!(
                    "CREATE OR REPLACE FUNCTION {name}_fn() RETURNS TRIGGER AS $$\n\
                     BEGIN\n  \
                       RAISE EXCEPTION '{table} is insert-only';\n\
                     END;\n\
                     $$ LANGUAGE plpgsql",
                    name = trigger.name,
                    table = trigger.source_table,
                ),
                format!(
                    "CREATE TRIGGER {upd_name} BEFORE UPDATE ON {table}\n\
                     FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
                    name = trigger.name,
                    table = trigger.source_table,
                ),
                format!(
                    "CREATE TRIGGER {del_name} BEFORE DELETE ON {table}\n\
                     FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
                    name = trigger.name,
                    table = trigger.source_table,
                ),
            ],
            Self::MySql => vec![
                format!(
                    "CREATE TRIGGER {upd_name} BEFORE UPDATE ON {table}\n\
                     FOR EACH ROW\n\
                     BEGIN\n  \
                       SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '{table} is insert-only';\n\
                     END",
                    table = trigger.source_table,
                ),
                format!(
                    "CREATE TRIGGER {del_name} BEFORE DELETE ON {table}\n\
                     FOR EACH ROW\n\
                     BEGIN\n  \
                       SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '{table} is insert-only';\n\
                     END",
                    table = trigger.source_table,
                ),
            ],
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

/// An `AFTER INSERT` trigger that maintains a `_current` table with a
/// version-column guard: out-of-order inserts (older `version_col` value)
/// do not regress the current view. Tie-broken deterministically on
/// `tie_break_col`. Used for time-versioned event logs whose `defined_on`
/// can arrive out of order (e.g. back-dated patches).
pub(crate) struct VersionedTriggerDef {
    /// Trigger name, e.g. `"function_patches__maintain_current"`.
    pub name: &'static str,
    /// Source (append-only) table name.
    pub source_table: &'static str,
    /// Target `_current` table name.
    pub target_table: &'static str,
    /// Partition column (the natural-key column of the `_current` view).
    pub partition_col: &'static str,
    /// Version column (typically `"defined_on"`).
    pub version_col: &'static str,
    /// Tie-break column for equal `version_col` values (typically `"id"`).
    pub tie_break_col: &'static str,
    /// All column names (must match both source and target tables).
    pub columns: Vec<&'static str>,
}

/// A `BEFORE UPDATE` guard that refuses any transition out of a set of
/// "sticky terminal" status values. Lifts service-layer sticky-Committed
/// semantics into a DDL invariant.
pub(crate) struct StickyTerminalsTriggerDef {
    /// Trigger name, e.g. `"logical_function_runs__sticky_terminals"`.
    pub name: &'static str,
    /// Table name, e.g. `"logical_function_runs"`.
    pub source_table: &'static str,
    /// Status column name, e.g. `"status"`.
    pub status_col: &'static str,
    /// Persisted enum codes that are sticky-final (e.g. `&["C", "X", "Y"]`
    /// for Committed/Canceled/Yanked). The domain crate that owns the
    /// status enum is expected to carry a unit test pinning these codes
    /// to the enum variants — `td-schema` cannot import the enum
    /// (foundational crate ordering).
    pub terminal_values: &'static [&'static str],
}

/// A `BEFORE UPDATE` / `BEFORE DELETE` guard that refuses mutation entirely.
/// Used to lift "insert-only event log" semantics into DDL.
pub(crate) struct ImmutableTriggerDef {
    /// Base name; `_no_update` and `_no_delete` suffixes are appended.
    pub name: &'static str,
    /// Table name.
    pub source_table: &'static str,
}
