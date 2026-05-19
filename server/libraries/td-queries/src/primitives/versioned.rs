//
// Copyright 2026 Tabs Data Inc.
//

//! CTE / select builders for versioned tables.
//!
//! Every helper here is `pub(crate)`. The trait impls in
//! [`crate::generic::versioned`] and [`crate::daos`] consume them; no
//! caller outside the crate can reach them, which is what keeps the
//! safe surface safe.

use sea_query::{
    Alias, CommonTableExpression, Condition, Expr, ExprTrait, Func, Iden, Order, OverStatement,
    Query, SelectStatement, WindowStatement, WithClause,
};
use ta_objects::SeaValue;
use ta_objects::VersionedDao;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::{SqlCondition, resolve_condition};

#[derive(Iden)]
pub(crate) enum Cte {
    Ranked,
    Rn,
}

pub(crate) fn select_at<D>(
    condition: impl SqlCondition,
    on: &D::VersionOrder,
    state_cond: Option<&Condition>,
) -> Result<SelectStatement, SqlError>
where
    D: VersionedDao,
{
    let mut where_cond = Condition::all()
        .add(resolve_condition::<D>(condition)?)
        .add(Expr::col(D::version_order_col()).lte(on.as_sea_value()));
    if let Some(s) = state_cond {
        where_cond = where_cond.add(s.clone());
    }
    let query = Query::select()
        .columns(D::columns())
        .from(D::table())
        .cond_where(where_cond)
        .take();
    Ok(query)
}

/// Build a CTE that pushes a **partition-stable** filter and the time
/// bound into the ranked window.
///
/// **Caller invariant:** `stable_filter` MUST reference only columns
/// whose value does not change across versions of the same partition
/// (e.g. `function_id` on `DependencyDB`/`TableDB`/`TriggerDB`, where a
/// partition row is owned by a single function for its lifetime).
/// Pushing such a filter into the CTE is provably equivalent to
/// filtering at the outer scope, but shrinks the ranking window from
/// "the whole table" to "rows matching this filter".
///
/// **`pub(crate)` by design.** Per-DAO query methods (e.g.
/// [`crate::daos::dependency::active_for_function_at`])
/// call this helper with a hard-coded filter on a column they
/// statically know is partition-stable. This privacy is the only
/// thing keeping the invariant safe — callers cannot reach this
/// function directly, so they cannot mistakenly pass a non-stable
/// column.
pub(crate) fn partition_stable_ranked_cte<D>(
    stable_filter: impl SqlCondition,
    on: &D::VersionOrder,
) -> Result<WithClause, SqlError>
where
    D: VersionedDao,
{
    let window = WindowStatement::new()
        .partition_by(D::partition_col())
        .order_by_expr(Expr::col(D::version_order_col()), Order::Desc)
        .order_by_expr(Expr::col(D::pk_col()), Order::Desc)
        .take();

    let ranked_query = Query::select()
        .columns(D::columns())
        .from(D::table())
        .cond_where(
            Condition::all()
                .add(resolve_condition::<D>(stable_filter)?)
                .add(Expr::col(D::version_order_col()).lte(on.as_sea_value())),
        )
        .expr_window_as(
            Func::cust(Alias::new("ROW_NUMBER")).args(vec![]),
            window,
            Cte::Rn,
        )
        .take();

    let with = WithClause::new()
        .cte(
            CommonTableExpression::new()
                .query(ranked_query)
                .table_name(Cte::Ranked)
                .to_owned(),
        )
        .to_owned();
    Ok(with)
}

/// Build a CTE that pushes a partition filter (one or many) plus the
/// time bound into the ranked window — only rows for those partitions
/// are scanned and ranked. Single-element slices emit `partition = ?`,
/// multi-element slices emit `partition IN (?, ?, ...)`. Use this for
/// unbounded-growth tables (append-only event logs) where ranking the
/// entire table on each fetch would scale linearly with table size.
pub(crate) fn partitioned_latest_ranked_cte<D>(
    partitions: &[D::Partition],
    on: &D::VersionOrder,
) -> Result<WithClause, SqlError>
where
    D: VersionedDao,
{
    let window = WindowStatement::new()
        .partition_by(D::partition_col())
        .order_by_expr(Expr::col(D::version_order_col()), Order::Desc)
        .order_by_expr(Expr::col(D::pk_col()), Order::Desc)
        .take();

    let partition_filter = match partitions {
        [single] => Expr::col(D::partition_col()).eq(single.as_sea_value()),
        many => {
            let in_values = many.iter().map(|p| p.as_sea_value()).collect::<Vec<_>>();
            Expr::col(D::partition_col()).is_in(in_values)
        }
    };

    let ranked_query = Query::select()
        .columns(D::columns())
        .from(D::table())
        .cond_where(
            Condition::all()
                .add(partition_filter)
                .add(Expr::col(D::version_order_col()).lte(on.as_sea_value())),
        )
        .expr_window_as(
            Func::cust(Alias::new("ROW_NUMBER")).args(vec![]),
            window,
            Cte::Rn,
        )
        .take();

    let with = WithClause::new()
        .cte(
            CommonTableExpression::new()
                .query(ranked_query)
                .table_name(Cte::Ranked)
                .to_owned(),
        )
        .to_owned();
    Ok(with)
}

/// **Slow path.** This CTE materialises ROW_NUMBER over the *entire*
/// versioned table (only the time upper-bound is applied), then the
/// caller's condition filters at the outer scope. Cost is O(table
/// size) per call, regardless of how selective the outer filter is.
///
/// Reachable only via the [`crate::Historic`] surface. The fast paths
/// — [`partitioned_latest_ranked_cte`] for partition-scoped reads
/// (via [`crate::Versioned`]), the `_current` table for at-now reads
/// (via [`crate::Current`]) — should always be preferred.
///
/// The caller's condition is intentionally **not** pushed into the CTE
/// because `SqlCondition` can be a non-partition filter (e.g. a FK
/// like `function_version_id` on the tables table). Pushing it would
/// change which row is `rn=1`, altering query semantics.
pub(crate) fn latest_ranked_cte<D>(on: &D::VersionOrder) -> Result<WithClause, SqlError>
where
    D: VersionedDao,
{
    let window = WindowStatement::new()
        .partition_by(D::partition_col())
        .order_by_expr(Expr::col(D::version_order_col()), Order::Desc)
        .order_by_expr(Expr::col(D::pk_col()), Order::Desc)
        .take();

    let ranked_query = Query::select()
        .columns(D::columns())
        .from(D::table())
        .and_where(Expr::col(D::version_order_col()).lte(on.as_sea_value()))
        .expr_window_as(
            Func::cust(Alias::new("ROW_NUMBER")).args(vec![]),
            window,
            Cte::Rn,
        )
        .take();

    let with = WithClause::new()
        .cte(
            CommonTableExpression::new()
                .query(ranked_query)
                .table_name(Cte::Ranked)
                .to_owned(),
        )
        .to_owned();
    Ok(with)
}

/// Select the latest version per partition from the ranked CTE, filtered
/// by condition and state. Common outer query for the [`crate::Historic`]
/// surface.
pub(crate) fn select_latest_from_cte<D>(
    condition: impl SqlCondition,
    state_cond: Option<&Condition>,
) -> Result<SelectStatement, SqlError>
where
    D: VersionedDao,
{
    let mut q = Query::select()
        .columns(D::columns())
        .from(Cte::Ranked)
        .and_where(Expr::col(Cte::Rn).eq(1_i64))
        .cond_where(resolve_condition::<D>(condition)?)
        .take();
    if let Some(s) = state_cond {
        q.cond_where(s.clone());
    }
    Ok(q)
}

/// Outer query for the safe partition-scoped versioned reads.
/// Partition equality is already inside the CTE; the optional state
/// filter is applied at the outer scope.
pub(crate) fn select_latest_from_partitioned_cte<D>(
    state_cond: Option<&Condition>,
) -> Result<SelectStatement, SqlError>
where
    D: VersionedDao,
{
    let mut q = Query::select()
        .columns(D::columns())
        .from(Cte::Ranked)
        .and_where(Expr::col(Cte::Rn).eq(1_i64))
        .take();
    if let Some(s) = state_cond {
        q.cond_where(s.clone());
    }
    Ok(q)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sea_query::{Asterisk, SqliteQueryBuilder};
    use ta_objects::Dao;

    #[ta_objects::typed(string)]
    struct TestId;

    #[ta_objects::typed(string)]
    struct TestPartition;

    #[ta_objects::typed(i64)]
    struct TestDefinedOn;

    #[ta_objects::typed_enum]
    enum TestVStatus {
        #[typed_enum(rename = "A")]
        Active,
    }

    #[Dao]
    #[dao(
        sql_table = "test_versioned",
        versioned(pk = "id", version_by = "defined_on", partition_by = "partition_id"),
        states(Active = TestVStatus::Active)
    )]
    struct TestVersionedDao {
        id: TestId,
        partition_id: TestPartition,
        status: TestVStatus,
        defined_on: TestDefinedOn,
    }

    #[test]
    fn test_select_at_all() {
        let id = TestPartition::try_from("p1").unwrap();
        let on = TestDefinedOn::try_from(100i64).unwrap();
        let sql = select_at::<TestVersionedDao>(&id, &on, None)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#"FROM "test_versioned""#),
            "should select from table: {sql}"
        );
        assert!(
            sql.contains(r#""partition_id" = 'p1'"#),
            "should filter by partition: {sql}"
        );
        assert!(
            sql.contains(r#""defined_on" <= 100"#),
            "should filter by version order: {sql}"
        );
    }

    #[test]
    fn test_select_at_active() {
        let id = TestPartition::try_from("p1").unwrap();
        let on = TestDefinedOn::try_from(100i64).unwrap();
        let sql =
            select_at::<TestVersionedDao>(&id, &on, Some(TestVersionedDao::active().condition()))
                .unwrap()
                .to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#""status" = 'A'"#),
            "should filter by state: {sql}"
        );
    }

    #[test]
    fn test_latest_ranked_cte() {
        let on = TestDefinedOn::try_from(100i64).unwrap();
        let with = latest_ranked_cte::<TestVersionedDao>(&on).unwrap();
        let sql = Query::select()
            .column(Asterisk)
            .from(Cte::Ranked)
            .take()
            .with(with)
            .to_string(SqliteQueryBuilder);
        assert!(sql.contains("ROW_NUMBER()"), "should use ROW_NUMBER: {sql}");
        assert!(
            sql.contains(r#""defined_on" <= 100"#),
            "should bound by version order: {sql}"
        );
        assert!(
            sql.contains(r#""ranked""#),
            "CTE should be named ranked: {sql}"
        );
    }

    #[test]
    fn test_select_latest_from_cte_all() {
        let id = TestPartition::try_from("p1").unwrap();
        let sql = select_latest_from_cte::<TestVersionedDao>(&id, None)
            .unwrap()
            .to_string(SqliteQueryBuilder);
        assert!(
            sql.contains(r#"FROM "ranked""#),
            "should select from ranked CTE: {sql}"
        );
        assert!(sql.contains(r#""rn" = 1"#), "should filter rn = 1: {sql}");
        assert!(
            sql.contains(r#""partition_id" = 'p1'"#),
            "should filter by condition: {sql}"
        );
    }

    #[test]
    fn test_partitioned_latest_ranked_cte_multi() {
        let p1 = TestPartition::try_from("p1").unwrap();
        let p2 = TestPartition::try_from("p2").unwrap();
        let p3 = TestPartition::try_from("p3").unwrap();
        let on = TestDefinedOn::try_from(100i64).unwrap();
        let with = partitioned_latest_ranked_cte::<TestVersionedDao>(&[p1, p2, p3], &on).unwrap();
        let sql = Query::select()
            .column(Asterisk)
            .from(Cte::Ranked)
            .take()
            .with(with)
            .to_string(SqliteQueryBuilder);
        assert!(sql.contains("ROW_NUMBER()"), "should use ROW_NUMBER: {sql}");
        let cte_body_start = sql.find("AS (SELECT").unwrap();
        let cte_body_end = sql.rfind(") SELECT").unwrap();
        let cte_body = &sql[cte_body_start..cte_body_end];
        // Partition IN-list and time bound must both live inside the CTE
        // so the window only ranks rows for the requested partitions.
        assert!(
            cte_body.contains(r#""partition_id" IN ('p1', 'p2', 'p3')"#),
            "partition IN list must be inside the CTE: {sql}"
        );
        assert!(
            cte_body.contains(r#""defined_on" <= 100"#),
            "version-order bound must be inside the CTE: {sql}"
        );
    }

    #[test]
    fn test_partitioned_latest_ranked_cte() {
        let partition = TestPartition::try_from("p1").unwrap();
        let on = TestDefinedOn::try_from(100i64).unwrap();
        let with = partitioned_latest_ranked_cte::<TestVersionedDao>(
            std::slice::from_ref(&partition),
            &on,
        )
        .unwrap();
        let sql = Query::select()
            .column(Asterisk)
            .from(Cte::Ranked)
            .take()
            .with(with)
            .to_string(SqliteQueryBuilder);
        assert!(sql.contains("ROW_NUMBER()"), "should use ROW_NUMBER: {sql}");
        // Partition equality must be INSIDE the CTE so the window only
        // ranks rows for this partition.
        let cte_body_start = sql.find("AS (SELECT").unwrap();
        let cte_body_end = sql.rfind(") SELECT").unwrap();
        let cte_body = &sql[cte_body_start..cte_body_end];
        assert!(
            cte_body.contains(r#""partition_id" = 'p1'"#),
            "partition filter must be inside the CTE: {sql}"
        );
        assert!(
            cte_body.contains(r#""defined_on" <= 100"#),
            "version-order bound must be inside the CTE: {sql}"
        );
    }

    #[test]
    fn test_full_versioned_query() {
        let id = TestPartition::try_from("p1").unwrap();
        let on = TestDefinedOn::try_from(100i64).unwrap();
        let sql = select_latest_from_cte::<TestVersionedDao>(
            &id,
            Some(TestVersionedDao::active().condition()),
        )
        .unwrap()
        .order_by_columns(TestVersionedDao::order_by())
        .take()
        .with(latest_ranked_cte::<TestVersionedDao>(&on).unwrap())
        .to_string(SqliteQueryBuilder);
        assert!(sql.starts_with("WITH"), "should be a WITH query: {sql}");
        assert!(sql.contains("ROW_NUMBER()"), "should use ROW_NUMBER: {sql}");
        assert!(sql.contains(r#""rn" = 1"#), "should filter rn = 1: {sql}");
        assert!(
            sql.contains(r#""status" = 'A'"#),
            "should filter by state: {sql}"
        );
    }
}
