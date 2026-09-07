//
// Copyright 2026 Tabsdata Inc.
//

//! Trigger specs and their renderers. `TriggerSpec` is the enum entities
//! return from `Entity::create_db_triggers`; it dispatches to the matching
//! variant's `render` method.
//!
//! Table and column references are typed as [`DynIden`] so callers source
//! them from the same `Iden` enums that declare the schema — no parallel
//! `&'static str` lists to keep in sync.

use sea_query::DynIden;

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
    /// Returned as `Vec<String>` because the supporting function
    /// (`CREATE OR REPLACE FUNCTION …`) and the trigger itself must be separate
    /// statements — sqlx-postgres uses the extended query protocol and rejects
    /// multi-statement strings.
    pub(crate) fn render(&self) -> Vec<String> {
        let name = &self.name;
        let source = self.source_table.to_string();
        let target = self.target_table.to_string();
        let partition = self.partition_col.to_string();
        let cols = join_idens(&self.columns, ", ");
        let new_vals = join_new_idens(&self.columns, ", ");
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
    pub(crate) fn render(&self) -> Vec<String> {
        let name = &self.name;
        let source = self.source_table.to_string();
        let target = self.target_table.to_string();
        let partition = self.partition_col.to_string();
        let version = self.version_col.to_string();
        let tie_break = self.tie_break_col.to_string();
        let cols = join_idens(&self.columns, ", ");
        let new_vals = join_new_idens(&self.columns, ", ");
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
    pub(crate) fn render(&self) -> Vec<String> {
        let name = &self.name;
        let table = self.source_table.to_string();
        let col = self.status_col.to_string();
        let values = self.terminal_values.iter().map(|v| format!("'{v}'")).collect::<Vec<_>>().join(", ");

        vec![
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
        ]
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
    pub(crate) fn render(&self) -> Vec<String> {
        let name = &self.name;
        let table = self.source_table.to_string();
        let upd_name = format!("{name}_no_update");
        let del_name = format!("{name}_no_delete");

        vec![
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
        ]
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
    pub(crate) fn render(&self) -> Vec<String> {
        match self {
            TriggerSpec::MaintainCurrent(d) => d.render(),
            TriggerSpec::VersionedCurrent(d) => d.render(),
            TriggerSpec::StickyTerminals(d) => d.render(),
            TriggerSpec::Immutable(d) => d.render(),
        }
    }

    /// The `DROP TRIGGER`s that precede a re-create. The supporting function is
    /// deliberately left alone: [`render`](Self::render) emits it as
    /// `CREATE OR REPLACE`, and dropping it would fail while its triggers exist.
    pub(crate) fn render_drop(&self) -> Vec<String> {
        match self {
            TriggerSpec::MaintainCurrent(d) => vec![drop_trigger(&d.name, &d.source_table)],
            TriggerSpec::VersionedCurrent(d) => vec![drop_trigger(&d.name, &d.source_table)],
            TriggerSpec::StickyTerminals(d) => vec![drop_trigger(&d.name, &d.source_table)],
            TriggerSpec::Immutable(d) => vec![
                drop_trigger(&format!("{}_no_update", d.name), &d.source_table),
                drop_trigger(&format!("{}_no_delete", d.name), &d.source_table),
            ],
        }
    }
}

/// A trigger name is scoped to its table, so the drop names both.
fn drop_trigger(name: &str, table: &DynIden) -> String {
    format!("DROP TRIGGER IF EXISTS {name} ON {table}")
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
    fn maintain_current_emits_function_and_trigger() {
        let out = maintain().render();
        assert_eq!(out.len(), 2, "emits FUNCTION + TRIGGER");
        assert!(out[0].contains("CREATE OR REPLACE FUNCTION foos__maintain_current_fn()"), "{}", out[0]);
        assert!(out[0].contains("INSERT INTO foos_current (id, foo_id, name) VALUES (NEW.id, NEW.foo_id, NEW.name)"), "{}", out[0]);
        assert!(out[0].contains("ON CONFLICT (foo_id) DO UPDATE SET id = EXCLUDED.id, name = EXCLUDED.name"), "{}", out[0]);
        assert!(out[1].contains("CREATE TRIGGER foos__maintain_current AFTER INSERT ON foos"), "{}", out[1]);
        assert!(out[1].contains("EXECUTE FUNCTION foos__maintain_current_fn()"), "{}", out[1]);
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
    fn versioned_current_guards_with_where_clause() {
        let out = versioned().render();
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
    fn sticky_terminals_raises_on_transition_out_of_terminal() {
        let out = sticky().render();
        assert_eq!(out.len(), 2);
        assert!(out[0].contains("OLD.status IN ('C', 'X')"), "{}", out[0]);
        assert!(out[0].contains("RAISE EXCEPTION 'foos.status is sticky in terminal state'"), "{}", out[0]);
        assert!(out[1].contains("BEFORE UPDATE OF status ON foos"), "{}", out[1]);
    }

    fn immutable() -> ImmutableTriggerDef {
        ImmutableTriggerDef {
            name: "foos__immutable".to_string(),
            source_table: alias("foos"),
        }
    }

    #[test]
    fn immutable_emits_paired_no_update_and_no_delete_triggers() {
        let out = immutable().render();
        assert_eq!(out.len(), 3, "function + 2 triggers");
        assert!(out[0].contains("CREATE OR REPLACE FUNCTION foos__immutable_fn()"), "{}", out[0]);
        assert!(out[1].contains("CREATE TRIGGER foos__immutable_no_update BEFORE UPDATE ON foos"), "{}", out[1]);
        assert!(out[2].contains("CREATE TRIGGER foos__immutable_no_delete BEFORE DELETE ON foos"), "{}", out[2]);
    }

    #[test]
    fn trigger_spec_dispatches_to_variant() {
        let m = TriggerSpec::MaintainCurrent(maintain()).render();
        assert!(m[0].contains("foos__maintain_current"), "{}", m[0]);

        let s = TriggerSpec::StickyTerminals(sticky()).render();
        assert!(s[0].contains("foos__sticky"), "{}", s[0]);

        let i = TriggerSpec::Immutable(immutable()).render();
        assert_eq!(i.len(), 3);

        let v = TriggerSpec::VersionedCurrent(versioned()).render();
        assert!(v[0].contains("WHERE EXCLUDED.defined_on"), "{}", v[0]);
    }

    #[test]
    fn drops_name_the_table() {
        let out = TriggerSpec::MaintainCurrent(maintain()).render_drop();
        assert_eq!(out, vec!["DROP TRIGGER IF EXISTS foos__maintain_current ON foos"]);

        let out = TriggerSpec::Immutable(immutable()).render_drop();
        assert_eq!(
            out,
            vec![
                "DROP TRIGGER IF EXISTS foos__immutable_no_update ON foos",
                "DROP TRIGGER IF EXISTS foos__immutable_no_delete ON foos",
            ]
        );
    }
}
