//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{DynIden, Expr, ExprTrait, Func, Iden, Order, OverStatement, Query, WindowStatement};

use crate::schema::backend::Backend;

/// Synthetic alias for the ranked subquery in the wrapping `INSERT … SELECT`.
#[derive(Iden, Clone, Copy)]
struct Ranked;

/// Window-function output column carrying the per-partition rank.
#[derive(Iden, Clone, Copy)]
struct Rn;

/// Tiebreaker column appended to the ranking order.
#[derive(Iden, Clone, Copy)]
struct Id;

/// `row_number()` ranking window function.
#[derive(Iden, Clone, Copy)]
struct RowNumber;

/// Declarative spec for seeding a `_current` snapshot table from its event-log
/// source via `ROW_NUMBER` ranking. Renders to a single
/// `INSERT INTO {target} ({cols}) SELECT {cols} FROM (ranked subquery) WHERE rn = 1`
/// statement. Tables and columns are typed as [`DynIden`] so callers source
/// them from the same `Iden` enums that declare the schema.
pub(crate) struct SeedCurrent {
    /// Target `_current` table.
    pub target: DynIden,
    /// Source event-log table.
    pub source: DynIden,
    /// Partition column (the natural key shared by source and target).
    pub partition_col: DynIden,
    /// Ordering column whose DESC max identifies the row to keep per partition.
    pub version_col: DynIden,
    /// All columns to copy (must exist on both source and target).
    pub columns: Vec<DynIden>,
}

impl SeedCurrent {
    /// Render this spec for `b`. The whole statement — ranked subquery, the
    /// wrapping `INSERT … SELECT`, and the subquery alias — is built through
    /// sea_query so identifier quoting and dialect differences are the query
    /// builder's responsibility, not a hand-assembled `format!` string.
    pub(crate) fn render(&self, b: &Backend) -> String {
        let mut window = WindowStatement::new();
        window.partition_by(self.partition_col.clone()).order_by(self.version_col.clone(), Order::Desc).order_by(Id, Order::Desc);

        let mut ranked = Query::select();
        for c in &self.columns {
            ranked.column(c.clone());
        }
        ranked.from(self.source.clone()).expr_window_as(Func::cust(RowNumber).args(vec![]), window, Rn);

        let mut selected = Query::select();
        for c in &self.columns {
            selected.column(c.clone());
        }
        selected.from_subquery(ranked.take(), Ranked).and_where(Expr::col(Rn).eq(Expr::val(1)));

        let mut insert = Query::insert();
        insert
            .into_table(self.target.clone())
            .columns(self.columns.iter().cloned())
            // The select list is built from the same `self.columns`, so the
            // column/value counts always match and this never errors.
            .select_from(selected.take())
            .expect("seed INSERT columns match the ranked SELECT by construction");

        b.build_query(insert.take())
    }
}

#[cfg(test)]
mod tests {
    use sea_query::{Alias, IntoIden};

    use super::*;

    fn alias(s: &'static str) -> DynIden {
        Alias::new(s).into_iden()
    }

    fn sample() -> SeedCurrent {
        SeedCurrent {
            target: alias("foos_current"),
            source: alias("foos"),
            partition_col: alias("foo_id"),
            version_col: alias("defined_on"),
            columns: ["id", "foo_id", "name", "defined_on"].iter().copied().map(alias).collect(),
        }
    }

    #[test]
    fn render_emits_insert_select_with_rn_filter() {
        let sql = sample().render(&Backend::Sqlite);
        assert!(
            sql.starts_with(
                r#"INSERT INTO "foos_current" ("id", "foo_id", "name", "defined_on") SELECT "id", "foo_id", "name", "defined_on" FROM ("#
            ),
            "{sql}"
        );
        assert!(sql.ends_with(r#") AS "ranked" WHERE "rn" = 1"#), "{sql}");
    }

    #[test]
    fn render_uses_row_number_partition_and_order() {
        let sql = sample().render(&Backend::Sqlite);
        assert!(sql.contains("row_number()"), "{sql}");
        assert!(sql.contains("PARTITION BY \"foo_id\""), "{sql}");
        assert!(sql.contains("ORDER BY \"defined_on\" DESC, \"id\" DESC"), "{sql}");
    }
}
