//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{Alias, DynIden, Expr, ExprTrait, Order, Query, SelectStatement};

/// Discriminant of `LogicalPointerStatus::Bound` (defined in td-objects).
/// Hoisted here as a `&str` because td-schema sits below td-objects in the
/// dep graph — keeps view definitions free of bare `"B"` literals.
const POINTER_STATUS_BOUND: &str = "B";

/// Correlated subquery that picks the concrete id of the latest `Bound`
/// pointer-event for a logical identity. Used by the LFR / LTDV
/// `__with_names` views.
///
/// `status`, `set_on`, and `id` are convention-named columns shared by every
/// pointer table and are hardcoded internally.
pub(crate) struct LatestBoundPointer {
    /// Pointer-event table (e.g. `LogicalFunctionRunPointers::Table`).
    pub pointer_table: DynIden,
    /// Column on `pointer_table` holding the concrete id
    /// (e.g. `PhysicalFunctionRunId`).
    pub concrete_id_col: DynIden,
    /// Column on `pointer_table` holding the logical id
    /// (e.g. `LogicalFunctionRunId`).
    pub logical_id_col: DynIden,
    /// Outer table to correlate against (e.g. `LogicalFunctionRuns::Table`).
    pub outer_table: DynIden,
    /// Outer id column to correlate against (e.g. `LogicalFunctionRuns::Id`).
    pub outer_id_col: DynIden,
}

impl LatestBoundPointer {
    pub(crate) fn into_subquery(self) -> SelectStatement {
        Query::select()
            .column((self.pointer_table.clone(), self.concrete_id_col))
            .from(self.pointer_table.clone())
            .and_where(Expr::col((self.pointer_table.clone(), self.logical_id_col)).equals((self.outer_table, self.outer_id_col)))
            .and_where(Expr::col((self.pointer_table.clone(), Alias::new("status"))).eq(POINTER_STATUS_BOUND))
            .order_by((self.pointer_table.clone(), Alias::new("set_on")), Order::Desc)
            .order_by((self.pointer_table, Alias::new("id")), Order::Desc)
            .limit(1)
            .to_owned()
    }
}

#[cfg(test)]
mod tests {
    use sea_query::IntoIden;

    use crate::schema::backend::Backend;

    use super::*;

    fn alias(s: &'static str) -> DynIden {
        Alias::new(s).into_iden()
    }

    fn sample() -> LatestBoundPointer {
        LatestBoundPointer {
            pointer_table: alias("logical_function_run_pointers"),
            concrete_id_col: alias("physical_function_run_id"),
            logical_id_col: alias("logical_function_run_id"),
            outer_table: alias("logical_function_runs"),
            outer_id_col: alias("id"),
        }
    }

    #[test]
    fn into_subquery_selects_concrete_id_from_pointer_table() {
        let sql = Backend::Sqlite.build_query(sample().into_subquery());
        assert!(
            sql.starts_with("SELECT \"logical_function_run_pointers\".\"physical_function_run_id\" FROM \"logical_function_run_pointers\""),
            "{sql}"
        );
    }

    #[test]
    fn into_subquery_correlates_with_outer_and_filters_by_bound_status() {
        let sql = Backend::Sqlite.build_query(sample().into_subquery());
        assert!(sql.contains("\"logical_function_run_pointers\".\"logical_function_run_id\" = \"logical_function_runs\".\"id\""), "{sql}");
        assert!(sql.contains("\"logical_function_run_pointers\".\"status\" = 'B'"), "{sql}");
    }

    #[test]
    fn into_subquery_orders_set_on_desc_then_id_desc_with_limit_one() {
        let sql = Backend::Sqlite.build_query(sample().into_subquery());
        assert!(
            sql.contains(
                "ORDER BY \"logical_function_run_pointers\".\"set_on\" DESC, \
                 \"logical_function_run_pointers\".\"id\" DESC"
            ),
            "{sql}"
        );
        assert!(sql.ends_with("LIMIT 1"), "{sql}");
    }
}
