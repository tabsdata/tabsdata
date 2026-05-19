//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{Alias, DynIden, Expr, Func, Order, OverStatement, Query, WindowStatement};

use crate::schema::backend::Backend;

/// Declarative spec for seeding a `_current` snapshot table from its event-log
/// source via `ROW_NUMBER` ranking. Renders to a single
/// `INSERT INTO {target} ({cols}) SELECT {cols} FROM (ranked CTE) WHERE rn = 1`
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
    /// Render this spec for `b`. The wrapping `INSERT…SELECT` uses `format!`
    /// because dialect quoting differs; the inner ranked CTE is built through
    /// sea_query.
    pub(crate) fn render(&self, b: &Backend) -> String {
        let window = WindowStatement::new()
            .partition_by(Alias::new(self.partition_col.to_string()))
            .order_by_expr(Expr::col(Alias::new(self.version_col.to_string())), Order::Desc)
            .order_by_expr(Expr::col(Alias::new("id")), Order::Desc)
            .take();

        let mut ranked = Query::select();
        for c in &self.columns {
            ranked.column(Alias::new(c.to_string()));
        }
        ranked.from(Alias::new(self.source.to_string())).expr_window_as(
            Func::cust(Alias::new("ROW_NUMBER")).args(vec![]),
            window,
            Alias::new("rn"),
        );
        let ranked_sql = b.build_query(ranked.take());

        let cols_csv = self.columns.iter().map(|c| c.to_string()).collect::<Vec<_>>().join(", ");
        format!("INSERT INTO {target} ({cols_csv}) SELECT {cols_csv} FROM ({ranked_sql}) WHERE rn = 1", target = self.target,)
    }
}

#[cfg(test)]
mod tests {
    use sea_query::IntoIden;

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
            sql.starts_with("INSERT INTO foos_current (id, foo_id, name, defined_on) SELECT id, foo_id, name, defined_on FROM ("),
            "{sql}"
        );
        assert!(sql.ends_with(") WHERE rn = 1"), "{sql}");
    }

    #[test]
    fn render_uses_row_number_partition_and_order() {
        let sql = sample().render(&Backend::Sqlite);
        assert!(sql.contains("ROW_NUMBER()"), "{sql}");
        assert!(sql.contains("PARTITION BY \"foo_id\""), "{sql}");
        assert!(sql.contains("ORDER BY \"defined_on\" DESC, \"id\" DESC"), "{sql}");
    }
}
