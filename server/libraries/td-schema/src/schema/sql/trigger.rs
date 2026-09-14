//
// Copyright 2026 Tabsdata Inc.
//

//! Trigger definitions and their renderers. Each def renders to a
//! [`Derived`] (`CREATE` statements plus the matching `DROP TRIGGER`s),
//! which is what `Entity::create_db_triggers` returns.

use sea_query::DynIden;

use crate::schema::source::Derived;

fn join_idens(idens: &[DynIden], sep: &str) -> String {
    idens.iter().map(|i| i.to_string()).collect::<Vec<_>>().join(sep)
}

/// `BEFORE INSERT` trigger maintaining SCD2 intervals: closes the covering
/// predecessor at the new `valid_from` and splices the new row's `valid_to`
/// against its `(valid_from, pk)` successor. Same-instant versions degrade to
/// zero-length intervals, so the higher `pk` wins.
///
/// In the trigger rather than the insert path so `valid_to` has one writer
/// regardless of which crate, delta, seed or bulk statement inserts, and so
/// the per-partition advisory lock (a row lock cannot cover the
/// empty-partition race) is taken in the same statement that needs it. It
/// costs one lock entry per distinct partition inserted, held to commit.
pub struct Scd2TriggerDef {
    /// Trigger name, e.g. `"functions__scd2_close"`.
    pub name: String,
    pub source_table: DynIden,
    pub partition_col: DynIden,
    pub valid_from_col: DynIden,
    pub valid_to_col: DynIden,
    /// Tie-breaker for same-instant versions.
    pub pk_col: DynIden,
}

impl Scd2TriggerDef {
    pub(crate) fn derived(&self) -> Derived {
        Derived {
            create: self.render(),
            drop: vec![drop_trigger(&self.name, &self.source_table), drop_trigger_function(&self.name)],
        }
    }

    pub fn render(&self) -> Vec<String> {
        let name = &self.name;
        let table = self.source_table.to_string();
        let partition = self.partition_col.to_string();
        let valid_from = self.valid_from_col.to_string();
        let valid_to = self.valid_to_col.to_string();
        let pk = self.pk_col.to_string();

        vec![
            format!(
                "CREATE OR REPLACE FUNCTION {name}_fn() RETURNS TRIGGER AS $$\n\
                 BEGIN\n  \
                   PERFORM pg_advisory_xact_lock(hashtextextended('{table}:' || NEW.{partition}, 0));\n  \
                   SELECT COALESCE(MIN({valid_from}), 'infinity') INTO NEW.{valid_to}\n  \
                   FROM {table}\n  \
                   WHERE {partition} = NEW.{partition}\n    \
                     AND ({valid_from}, {pk}) > (NEW.{valid_from}, NEW.{pk});\n  \
                   UPDATE {table} SET {valid_to} = NEW.{valid_from}\n  \
                   WHERE {partition} = NEW.{partition}\n    \
                     AND ({valid_from}, {pk}) < (NEW.{valid_from}, NEW.{pk})\n    \
                     AND {valid_to} > NEW.{valid_from};\n  \
                   RETURN NEW;\n\
                 END;\n\
                 $$ LANGUAGE plpgsql",
            ),
            format!(
                "CREATE TRIGGER {name} BEFORE INSERT ON {table}\n\
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
    pub(crate) fn derived(&self) -> Derived {
        Derived {
            create: self.render(),
            drop: vec![drop_trigger(&self.name, &self.source_table), drop_trigger_function(&self.name)],
        }
    }

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

/// An `ImmutableTriggerDef` with an SCD2 carve-out: UPDATEs are refused only
/// when they touch one of `columns` (everything but `valid_to`), so the
/// [`Scd2TriggerDef`] close-out — which sets only `valid_to` — passes while
/// the event log stays otherwise insert-only. DELETE is refused outright.
pub(crate) struct ImmutableColumnsTriggerDef {
    /// Base name; `_no_update` and `_no_delete` suffixes are appended.
    pub name: String,
    pub source_table: DynIden,
    /// The immutable columns (`BEFORE UPDATE OF` set).
    pub columns: Vec<DynIden>,
}

impl ImmutableColumnsTriggerDef {
    pub(crate) fn derived(&self) -> Derived {
        Derived {
            create: self.render(),
            drop: vec![
                drop_trigger(&format!("{}_no_update", self.name), &self.source_table),
                drop_trigger(&format!("{}_no_delete", self.name), &self.source_table),
                drop_trigger_function(&self.name),
            ],
        }
    }

    pub(crate) fn render(&self) -> Vec<String> {
        let name = &self.name;
        let table = self.source_table.to_string();
        let upd_name = format!("{name}_no_update");
        let del_name = format!("{name}_no_delete");
        let cols = join_idens(&self.columns, ", ");

        vec![
            format!(
                "CREATE OR REPLACE FUNCTION {name}_fn() RETURNS TRIGGER AS $$\n\
                 BEGIN\n  \
                   RAISE EXCEPTION '{table} is insert-only';\n\
                 END;\n\
                 $$ LANGUAGE plpgsql",
            ),
            format!(
                "CREATE TRIGGER {upd_name} BEFORE UPDATE OF {cols} ON {table}\n\
                 FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
            ),
            format!(
                "CREATE TRIGGER {del_name} BEFORE DELETE ON {table}\n\
                 FOR EACH ROW EXECUTE FUNCTION {name}_fn()",
            ),
        ]
    }
}

/// A trigger name is scoped to its table, so the drop names both.
pub(crate) fn drop_trigger(name: &str, table: &DynIden) -> String {
    format!("DROP TRIGGER IF EXISTS {name} ON {table}")
}

/// Every def renders its body as `CREATE OR REPLACE FUNCTION {name}_fn()`, so
/// the drop pairs with it. Dropped after the triggers that execute it.
pub(crate) fn drop_trigger_function(name: &str) -> String {
    format!("DROP FUNCTION IF EXISTS {name}_fn()")
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

    fn scd2() -> Scd2TriggerDef {
        Scd2TriggerDef {
            name: "foos__scd2_close".to_string(),
            source_table: alias("foos"),
            partition_col: alias("foo_id"),
            valid_from_col: alias("defined_on"),
            valid_to_col: alias("valid_to"),
            pk_col: alias("id"),
        }
    }

    #[test]
    fn scd2_close_out_splices_and_closes_predecessor() {
        let out = scd2().render();
        assert_eq!(out.len(), 2, "function + BEFORE INSERT trigger");
        let f = &out[0];
        assert!(f.contains("pg_advisory_xact_lock(hashtextextended('foos:' || NEW.foo_id, 0))"), "{f}");
        assert!(f.contains("SELECT COALESCE(MIN(defined_on), 'infinity') INTO NEW.valid_to"), "{f}");
        assert!(f.contains("AND (defined_on, id) > (NEW.defined_on, NEW.id)"), "{f}");
        assert!(f.contains("UPDATE foos SET valid_to = NEW.defined_on"), "{f}");
        assert!(f.contains("AND (defined_on, id) < (NEW.defined_on, NEW.id)"), "{f}");
        assert!(f.contains("AND valid_to > NEW.defined_on"), "{f}");
        assert!(out[1].contains("CREATE TRIGGER foos__scd2_close BEFORE INSERT ON foos"), "{}", out[1]);
    }

    fn immutable_columns() -> ImmutableColumnsTriggerDef {
        ImmutableColumnsTriggerDef {
            name: "foos__immutable".to_string(),
            source_table: alias("foos"),
            columns: aliases(&["id", "foo_id", "name"]),
        }
    }

    #[test]
    fn immutable_columns_guards_only_the_listed_columns() {
        let out = immutable_columns().render();
        assert_eq!(out.len(), 3, "function + 2 triggers");
        assert!(out[1].contains("CREATE TRIGGER foos__immutable_no_update BEFORE UPDATE OF id, foo_id, name ON foos"), "{}", out[1]);
        assert!(out[2].contains("CREATE TRIGGER foos__immutable_no_delete BEFORE DELETE ON foos"), "{}", out[2]);
    }

    #[test]
    fn drops_name_the_table() {
        assert_eq!(
            sticky().derived().drop,
            vec![
                "DROP TRIGGER IF EXISTS foos__sticky ON foos",
                "DROP FUNCTION IF EXISTS foos__sticky_fn()",
            ]
        );
        assert_eq!(
            immutable_columns().derived().drop,
            vec![
                "DROP TRIGGER IF EXISTS foos__immutable_no_update ON foos",
                "DROP TRIGGER IF EXISTS foos__immutable_no_delete ON foos",
                "DROP FUNCTION IF EXISTS foos__immutable_fn()",
            ]
        );
    }
}
