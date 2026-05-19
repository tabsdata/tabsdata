//
// Copyright 2026 Tabs Data Inc.
//

//! Per-DAO queries for `DependencyDB` whose shape can't be expressed
//! with the generic [`crate::Versioned`] surface.
//!
//! Each method here hard-codes a column the DAO author knows is
//! partition-stable — `function_id` doesn't change across versions of
//! a `dependency_id` because a dependency belongs to one function for
//! its entire lifetime. That fact lets us push the filter into the
//! ranked CTE (instead of applying it at the outer scope), shrinking
//! the window from "the whole `dependencies` table" to "rows for this
//! function". The privacy of
//! [`crate::primitives::versioned::partition_stable_ranked_cte`] is
//! what keeps the invariant safe; the public surface only ever accepts
//! a typed `FunctionId`.

use crate::QueryExecutor;
use crate::primitives::versioned::{Cte, partition_stable_ranked_cte};
use sea_query::{Expr, ExprTrait, Query};
use ta_objects::Dao;
use ta_objects::errors::SqlError;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::dxo::dependency::DependencyDB;
use td_objects::types::basic::{AtTime, FunctionId};

query_handle! {
    /// Entry trait providing [`dependencies`](DependenciesEntry::dependencies)
    /// on every [`QueryExecutor`](crate::QueryExecutor). Returns a
    /// [`Dependencies`] handle.
    pub trait DependenciesEntry {
        fn dependencies(self) -> Dependencies<Self> {
            Dependencies { executor: self }
        }
    }
}

/// `dependencies`-table operations. Reach via
/// [`DependenciesEntry::dependencies`]:
///
/// ```ignore
/// conn.dependencies().active_for_function_at(&function_id, &at).await?;
/// ```
pub struct Dependencies<E> {
    pub(crate) executor: E,
}

impl<E> Dependencies<E>
where
    E: QueryExecutor,
{
    /// Active dependencies of `function_id` at `on`.
    ///
    /// Returns the latest version of every `dependency_id` belonging
    /// to `function_id` whose state is `Active` and whose
    /// `defined_on <= on`. `function_id` is partition-stable on
    /// `DependencyDB`, so the filter is pushed into the ranked CTE —
    /// only the function's dependency rows are scanned and ranked.
    pub async fn active_for_function_at(
        self,
        function_id: &FunctionId,
        on: &AtTime,
    ) -> Result<Vec<DependencyDB>, SqlError> {
        let with = partition_stable_ranked_cte::<DependencyDB>(function_id, on)?;
        let state_cond = DependencyDB::active().condition().clone();
        let mut query = Query::select()
            .columns(DependencyDB::columns())
            .from(Cte::Ranked)
            .and_where(Expr::col(Cte::Rn).eq(1_i64))
            .cond_where(state_cond)
            .order_by_columns(DependencyDB::order_by())
            .take()
            .with(with);
        let rows: Vec<DependencyDB> = query
            .build_query_as()
            .fetch_all(self.executor)
            .await
            .map_err(SqlError::on_read::<DependencyDB>)?;
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::SqliteQueryBuilder;
    use ta_objects::SeaValue;

    #[test]
    fn test_active_for_function_at_sql() {
        // Reach the helper directly to snapshot the SQL: the per-DAO
        // method's CTE must push function_id = ? into the WHERE
        // alongside defined_on <= ?, NOT at the outer scope.
        let function_id = FunctionId::default();
        let on = AtTime::now();
        let with = partition_stable_ranked_cte::<DependencyDB>(&function_id, &on).unwrap();
        let sql = Query::select()
            .columns(DependencyDB::columns())
            .from(Cte::Ranked)
            .and_where(Expr::col(Cte::Rn).eq(1))
            .take()
            .with(with)
            .to_string(SqliteQueryBuilder);

        let cte_body_start = sql.find("AS (SELECT").unwrap();
        let cte_body_end = sql.rfind(") SELECT").unwrap();
        let cte_body = &sql[cte_body_start..cte_body_end];
        let function_id_value = function_id.as_sea_value();
        let function_id_lit = format!("'{function_id_value}'");
        assert!(
            cte_body.contains(&format!(r#""function_id" = {function_id_lit}"#))
                || cte_body.contains(r#""function_id" ="#),
            "function_id equality must be inside the CTE: {sql}"
        );
        assert!(
            cte_body.contains(r#""defined_on" <="#),
            "defined_on bound must be inside the CTE: {sql}"
        );
        // ROW_NUMBER must be partitioned by dependency_id (the partition col).
        assert!(
            cte_body.contains(r#"PARTITION BY "dependency_id""#),
            "ranked window must partition by dependency_id: {sql}"
        );
    }
}
