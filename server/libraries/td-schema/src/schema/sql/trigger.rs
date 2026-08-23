//
// Copyright 2026 Tabsdata Inc.
//

//! Trigger specs and their dialect-specific renderers. `TriggerSpec` is the
//! enum entities return from `Entity::create_db_triggers`; it dispatches to
//! the matching variant's `render` method.
//!
//! Table and column references are typed as [`DynIden`] so callers source
//! them from the same `Iden` enums that declare the schema — no parallel
//! `&'static str` lists to keep in sync.

use sea_query::DynIden;

use crate::schema::backend::Backend;

fn join_idens(idens: &[DynIden], sep: &str) -> String {
    idens.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(sep)
}

fn join_new_idens(idens: &[DynIden], sep: &str) -> String {
    idens.iter().map(|i| format!("NEW.{}", i)).collect::<Vec<_>>().join(sep)
}

fn join_excluded_update(idens: &[DynIden], skip_partition: &str) -> String {
    idens
        .iter()
        .filter_map(|c| {
            let c = c.to_string();
            (c != skip_partition).then(|| format!("{c} = EXCLUDED.{c}"))
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// A trigger that maintains a `_current` snapshot table from an append-only
/// source table.
pub(crate) struct TriggerDef {
    /// Trigger name, e.g. `"functions__maintain_current"`.
    pub name: String,
    /// Source (append-only) table.
    pub source_table: DynIden,
    /// Target `_current` table.
    pub target_table: DynIden,
    /// The partition column.
    pub partition_col: DynIden,
    /// All columns (must match both source and target tables).
    pub columns: Vec<DynIden>,
}

impl TriggerDef {
    /// Render `CREATE TRIGGER` for this maintain-current trigger.
    ///
    /// Returned as `Vec<String>` so Postgres can emit its supporting function
    /// (`CREATE OR REPLACE FUNCTION …`) and the trigger itself as separate
    /// statements — sqlx-postgres uses the extended query protocol and rejects
    /// multi-statement strings. SQLite and MySQL each return a single-element vec.
    pub(crate) fn render(&self, b: &Backend) -> Vec<String> {
        let name = &self.name;
        let source = self.source_table.to_string();
        let target = self.target_table.to_string();
        let partition = self.partition_col.to_string();
        let cols = join_idens(&self.columns, ", ");
        let new_vals = join_new_idens(&self.columns, ", ");

        match b {
            Backend::Sqlite => vec![format!(
                "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                 FOR EACH ROW BEGIN\n  \
                   DELETE FROM {target} WHERE {partition} = NEW.{partition};\n  \
                   INSERT INTO {target} ({cols}) VALUES ({new_vals});\n\
                 END",
            )],
            Backend::Postgres => {
                let update_set = join_excluded_update(&self.columns, &partition);
                vec![
                    format!(
                        "CREATE OR REPLACE FUNCTION {name}_fn() RETURNS TRIGGER AS $$\n\
                         BEGIN\n  \
                           INSERT INTO {target} ({cols}) VALUES ({new_vals})\n  \
                           ON CONFLICT ({partition}) DO UPDATE SET {update_set};\n  \
                           RETURN NEW;\n\
                         END;\n\
                         $$ LANGUAGE plpgsql",
                    ),
                    format!(
                        "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                         FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
                    ),
                ]
            }
            Backend::MySql => {
                let update_set = self
                    .columns
                    .iter()
                    .filter_map(|c| {
                        let c = c.to_string();
                        (c != partition).then(|| format!("{c} = NEW.{c}"))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                vec![format!(
                    "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                     FOR EACH ROW BEGIN\n  \
                       INSERT INTO {target} ({cols}) VALUES ({new_vals})\n  \
                       ON DUPLICATE KEY UPDATE {update_set};\n\
                     END",
                )]
            }
        }
    }
}

/// An `AFTER INSERT` trigger that maintains a `_current` table *guarded by a
/// version column*: out-of-order inserts (older `version_col`) do not regress
/// the current view. Tie-broken on `tie_break_col`.
pub(crate) struct VersionedTriggerDef {
    pub name: String,
    pub source_table: DynIden,
    pub target_table: DynIden,
    pub partition_col: DynIden,
    pub version_col: DynIden,
    pub tie_break_col: DynIden,
    pub columns: Vec<DynIden>,
}

impl VersionedTriggerDef {
    pub(crate) fn render(&self, b: &Backend) -> Vec<String> {
        let name = &self.name;
        let source = self.source_table.to_string();
        let target = self.target_table.to_string();
        let partition = self.partition_col.to_string();
        let version = self.version_col.to_string();
        let tie_break = self.tie_break_col.to_string();
        let cols = join_idens(&self.columns, ", ");
        let new_vals = join_new_idens(&self.columns, ", ");

        match b {
            Backend::Sqlite => vec![format!(
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
            )],
            Backend::Postgres => {
                let update_set = join_excluded_update(&self.columns, &partition);
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
                    ),
                    format!(
                        "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                         FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
                    ),
                ]
            }
            Backend::MySql => {
                let update_set = self
                    .columns
                    .iter()
                    .filter_map(|c| {
                        let c = c.to_string();
                        (c != partition).then(|| {
                            format!(
                                "{c} = IF(NEW.{version} > {target}.{version} OR (NEW.{version} = {target}.{version} AND NEW.{tie_break} > {target}.{tie_break}), NEW.{c}, {target}.{c})",
                            )
                        })
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                vec![format!(
                    "CREATE TRIGGER {name} AFTER INSERT ON {source}\n\
                     FOR EACH ROW BEGIN\n  \
                       INSERT INTO {target} ({cols}) VALUES ({new_vals})\n  \
                       ON DUPLICATE KEY UPDATE {update_set};\n\
                     END",
                )]
            }
        }
    }
}

/// A `BEFORE UPDATE` guard that refuses any transition out of a set of
/// "sticky terminal" status values.
pub(crate) struct StickyTerminalsTriggerDef {
    pub name: String,
    pub source_table: DynIden,
    pub status_col: DynIden,
    /// Persisted enum codes that are sticky-final (e.g. `&["C", "X", "Y"]`).
    /// These are domain status discriminants from `td-objects`, not column
    /// names — kept as `&str` because they have no Iden representation here.
    pub terminal_values: &'static [&'static str],
}

impl StickyTerminalsTriggerDef {
    pub(crate) fn render(&self, b: &Backend) -> Vec<String> {
        let name = &self.name;
        let table = self.source_table.to_string();
        let col = self.status_col.to_string();
        let values = self.terminal_values.iter().map(|v| format!("'{v}'")).collect::<Vec<_>>().join(", ");

        match b {
            Backend::Sqlite => vec![format!(
                "CREATE TRIGGER {name} BEFORE UPDATE OF {col} ON {table}\n\
                 FOR EACH ROW WHEN OLD.{col} IN ({values}) AND NEW.{col} != OLD.{col}\n\
                 BEGIN\n  \
                   SELECT RAISE(ABORT, '{table}.{col} is sticky in terminal state');\n\
                 END",
            )],
            Backend::Postgres => vec![
                format!(
                    "CREATE OR REPLACE FUNCTION {name}_fn() RETURNS TRIGGER AS $$\n\
                     BEGIN\n  \
                       IF OLD.{col} IN ({values}) AND NEW.{col} != OLD.{col} THEN\n    \
                         RAISE EXCEPTION '{table}.{col} is sticky in terminal state';\n  \
                       END IF;\n  \
                       RETURN NEW;\n\
                     END;\n\
                     $$ LANGUAGE plpgsql",
                ),
                format!(
                    "CREATE TRIGGER {name} BEFORE UPDATE OF {col} ON {table}\n\
                     FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
                ),
            ],
            Backend::MySql => vec![format!(
                "CREATE TRIGGER {name} BEFORE UPDATE ON {table}\n\
                 FOR EACH ROW\n\
                 BEGIN\n  \
                   IF OLD.{col} IN ({values}) AND NEW.{col} != OLD.{col} THEN\n    \
                     SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '{table}.{col} is sticky in terminal state';\n  \
                   END IF;\n\
                 END",
            )],
        }
    }
}

/// A `BEFORE UPDATE` / `BEFORE DELETE` guard that refuses any mutation,
/// lifting "insert-only event log" semantics into DDL.
pub(crate) struct ImmutableTriggerDef {
    /// Base name; `_no_update` and `_no_delete` suffixes are appended.
    pub name: String,
    pub source_table: DynIden,
}

impl ImmutableTriggerDef {
    pub(crate) fn render(&self, b: &Backend) -> Vec<String> {
        let name = &self.name;
        let table = self.source_table.to_string();
        let upd_name = format!("{name}_no_update");
        let del_name = format!("{name}_no_delete");

        match b {
            Backend::Sqlite => vec![
                format!(
                    "CREATE TRIGGER {upd_name} BEFORE UPDATE ON {table}\n\
                     FOR EACH ROW BEGIN\n  \
                       SELECT RAISE(ABORT, '{table} is insert-only');\n\
                     END",
                ),
                format!(
                    "CREATE TRIGGER {del_name} BEFORE DELETE ON {table}\n\
                     FOR EACH ROW BEGIN\n  \
                       SELECT RAISE(ABORT, '{table} is insert-only');\n\
                     END",
                ),
            ],
            Backend::Postgres => vec![
                format!(
                    "CREATE OR REPLACE FUNCTION {name}_fn() RETURNS TRIGGER AS $$\n\
                     BEGIN\n  \
                       RAISE EXCEPTION '{table} is insert-only';\n\
                     END;\n\
                     $$ LANGUAGE plpgsql",
                ),
                format!(
                    "CREATE TRIGGER {upd_name} BEFORE UPDATE ON {table}\n\
                     FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
                ),
                format!(
                    "CREATE TRIGGER {del_name} BEFORE DELETE ON {table}\n\
                     FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
                ),
            ],
            Backend::MySql => vec![
                format!(
                    "CREATE TRIGGER {upd_name} BEFORE UPDATE ON {table}\n\
                     FOR EACH ROW\n\
                     BEGIN\n  \
                       SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '{table} is insert-only';\n\
                     END",
                ),
                format!(
                    "CREATE TRIGGER {del_name} BEFORE DELETE ON {table}\n\
                     FOR EACH ROW\n\
                     BEGIN\n  \
                       SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '{table} is insert-only';\n\
                     END",
                ),
            ],
        }
    }
}

/// Typed wrapper over the four trigger flavours. Entities return these from
/// `Entity::create_db_triggers`; [`TriggerSpec::render`] dispatches to the
/// matching variant.
pub(crate) enum TriggerSpec {
    MaintainCurrent(TriggerDef),
    VersionedCurrent(VersionedTriggerDef),
    StickyTerminals(StickyTerminalsTriggerDef),
    Immutable(ImmutableTriggerDef),
}

impl TriggerSpec {
    pub(crate) fn render(&self, b: &Backend) -> Vec<String> {
        match self {
            TriggerSpec::MaintainCurrent(d) => d.render(b),
            TriggerSpec::VersionedCurrent(d) => d.render(b),
            TriggerSpec::StickyTerminals(d) => d.render(b),
            TriggerSpec::Immutable(d) => d.render(b),
        }
    }

    /// The `DROP TRIGGER`s that precede a re-create. PostgreSQL's supporting
    /// function is deliberately left alone: [`render`](Self::render) emits it as
    /// `CREATE OR REPLACE`, and dropping it would fail while its triggers exist.
    pub(crate) fn render_drop(&self, b: &Backend) -> Vec<String> {
        match self {
            TriggerSpec::MaintainCurrent(d) => vec![drop_trigger(&d.name, &d.source_table, b)],
            TriggerSpec::VersionedCurrent(d) => vec![drop_trigger(&d.name, &d.source_table, b)],
            TriggerSpec::StickyTerminals(d) => vec![drop_trigger(&d.name, &d.source_table, b)],
            TriggerSpec::Immutable(d) => vec![
                drop_trigger(&format!("{}_no_update", d.name), &d.source_table, b),
                drop_trigger(&format!("{}_no_delete", d.name), &d.source_table, b),
            ],
        }
    }
}

/// Only PostgreSQL scopes a trigger name to its table; elsewhere the name is
/// schema-global.
fn drop_trigger(name: &str, table: &DynIden, b: &Backend) -> String {
    match b {
        Backend::Postgres => format!("DROP TRIGGER IF EXISTS {name} ON {table}"),
        _ => format!("DROP TRIGGER IF EXISTS {name}"),
    }
}

#[cfg(test)]
mod tests {
    use sea_query::{Alias, IntoIden};

    use super::*;

    fn alias(s: &'static str) -> DynIden {
        Alias::new(s).into_iden()
    }

    fn aliases(strs: &[&'static str]) -> Vec<DynIden> {
        strs.iter().copied().map(alias).collect()
    }

    fn maintain() -> TriggerDef {
        TriggerDef {
            name: "foos__maintain_current".to_string(),
            source_table: alias("foos"),
            target_table: alias("foos_current"),
            partition_col: alias("foo_id"),
            columns: aliases(&["id", "foo_id", "name"]),
        }
    }

    #[test]
    fn maintain_current_sqlite_emits_one_after_insert_trigger() {
        let out = maintain().render(&Backend::Sqlite);
        assert_eq!(out.len(), 1);
        let s = &out[0];
        assert!(s.contains("CREATE TRIGGER foos__maintain_current AFTER INSERT ON foos"), "{s}");
        assert!(s.contains("DELETE FROM foos_current WHERE foo_id = NEW.foo_id"), "{s}");
        assert!(s.contains("INSERT INTO foos_current (id, foo_id, name) VALUES (NEW.id, NEW.foo_id, NEW.name)"), "{s}");
    }

    #[test]
    fn maintain_current_postgres_emits_function_and_trigger() {
        let out = maintain().render(&Backend::Postgres);
        assert_eq!(out.len(), 2, "postgres emits FUNCTION + TRIGGER");
        assert!(out[0].contains("CREATE OR REPLACE FUNCTION foos__maintain_current_fn()"), "{}", out[0]);
        assert!(out[0].contains("ON CONFLICT (foo_id) DO UPDATE SET id = EXCLUDED.id, name = EXCLUDED.name"), "{}", out[0]);
        assert!(out[1].contains("CREATE TRIGGER foos__maintain_current AFTER INSERT ON foos"), "{}", out[1]);
        assert!(out[1].contains("EXECUTE FUNCTION foos__maintain_current_fn()"), "{}", out[1]);
    }

    #[test]
    fn maintain_current_mysql_uses_on_duplicate_key_update() {
        let out = maintain().render(&Backend::MySql);
        assert_eq!(out.len(), 1);
        let s = &out[0];
        assert!(s.contains("CREATE TRIGGER foos__maintain_current AFTER INSERT ON foos"), "{s}");
        assert!(s.contains("ON DUPLICATE KEY UPDATE id = NEW.id, name = NEW.name"), "{s}");
    }

    fn versioned() -> VersionedTriggerDef {
        VersionedTriggerDef {
            name: "foos__maintain_current".to_string(),
            source_table: alias("foos"),
            target_table: alias("foos_current"),
            partition_col: alias("foo_id"),
            version_col: alias("defined_on"),
            tie_break_col: alias("id"),
            columns: aliases(&["id", "foo_id", "defined_on"]),
        }
    }

    #[test]
    fn versioned_current_sqlite_guards_with_when_not_exists() {
        let out = versioned().render(&Backend::Sqlite);
        assert_eq!(out.len(), 1);
        let s = &out[0];
        assert!(s.contains("WHEN NOT EXISTS"), "{s}");
        assert!(s.contains("defined_on > NEW.defined_on"), "{s}");
        assert!(s.contains("OR (defined_on = NEW.defined_on AND id >= NEW.id)"), "{s}");
    }

    #[test]
    fn versioned_current_postgres_guards_with_where_clause() {
        let out = versioned().render(&Backend::Postgres);
        assert_eq!(out.len(), 2);
        assert!(
            out[0].contains(
                "WHERE EXCLUDED.defined_on > foos_current.defined_on\n     \
                 OR (EXCLUDED.defined_on = foos_current.defined_on"
            ),
            "{}",
            out[0]
        );
    }

    fn sticky() -> StickyTerminalsTriggerDef {
        StickyTerminalsTriggerDef {
            name: "foos__sticky".to_string(),
            source_table: alias("foos"),
            status_col: alias("status"),
            terminal_values: &["C", "X"],
        }
    }

    #[test]
    fn sticky_terminals_sqlite_raises_on_transition_out_of_terminal() {
        let out = sticky().render(&Backend::Sqlite);
        assert_eq!(out.len(), 1);
        let s = &out[0];
        assert!(s.contains("BEFORE UPDATE OF status ON foos"), "{s}");
        assert!(s.contains("OLD.status IN ('C', 'X')"), "{s}");
        assert!(s.contains("RAISE(ABORT,"), "{s}");
    }

    #[test]
    fn sticky_terminals_mysql_uses_signal_sqlstate() {
        let out = sticky().render(&Backend::MySql);
        assert_eq!(out.len(), 1);
        assert!(out[0].contains("SIGNAL SQLSTATE '45000'"), "{}", out[0]);
    }

    fn immutable() -> ImmutableTriggerDef {
        ImmutableTriggerDef {
            name: "foos__immutable".to_string(),
            source_table: alias("foos"),
        }
    }

    #[test]
    fn immutable_emits_paired_no_update_and_no_delete_triggers() {
        // SQLite + MySQL: 2 statements (one UPDATE guard, one DELETE guard).
        // Postgres: 3 (function + 2 triggers).
        let sqlite = immutable().render(&Backend::Sqlite);
        assert_eq!(sqlite.len(), 2);
        assert!(sqlite[0].contains("CREATE TRIGGER foos__immutable_no_update BEFORE UPDATE ON foos"), "{}", sqlite[0]);
        assert!(sqlite[1].contains("CREATE TRIGGER foos__immutable_no_delete BEFORE DELETE ON foos"), "{}", sqlite[1]);

        let postgres = immutable().render(&Backend::Postgres);
        assert_eq!(postgres.len(), 3);
        assert!(postgres[0].contains("CREATE OR REPLACE FUNCTION foos__immutable_fn()"), "{}", postgres[0]);
    }

    #[test]
    fn trigger_spec_dispatches_to_variant() {
        let m = TriggerSpec::MaintainCurrent(maintain()).render(&Backend::Sqlite);
        assert!(m[0].contains("foos__maintain_current"), "{}", m[0]);

        let s = TriggerSpec::StickyTerminals(sticky()).render(&Backend::Sqlite);
        assert!(s[0].contains("foos__sticky"), "{}", s[0]);

        let i = TriggerSpec::Immutable(immutable()).render(&Backend::Sqlite);
        assert_eq!(i.len(), 2);

        let v = TriggerSpec::VersionedCurrent(versioned()).render(&Backend::Sqlite);
        assert!(v[0].contains("WHEN NOT EXISTS"), "{}", v[0]);
    }
}
