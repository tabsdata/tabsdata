//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, CommonTableExpression, Condition, Expr, ExprTrait, Func, Iden, Order, OverStatement,
    Query, SelectStatement, WindowStatement, WithClause,
};
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::{SqlCondition, resolve_condition};
use ta_objects::{ListDto, ListFilter, SeaValue};
use ta_objects::{States, VersionedDao};
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_objects::list::params::ListQueryParams;

#[derive(Iden)]
pub(crate) enum Cte {
    Ranked,
    Rn,
}

pub trait VersionedQueries {
    fn fetch_one_at<const S: u8, D>(
        self,
        condition: impl SqlCondition,
        on: &D::VersionOrder,
    ) -> impl Future<Output = Result<D, SqlError>>
    where
        D: VersionedDao + States<S>;

    fn fetch_optional_at<const S: u8, D>(
        self,
        condition: impl SqlCondition,
        on: &D::VersionOrder,
    ) -> impl Future<Output = Result<Option<D>, SqlError>>
    where
        D: VersionedDao + States<S>;

    fn fetch_all_at<const S: u8, D>(
        self,
        condition: impl SqlCondition,
        on: &D::VersionOrder,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        D: VersionedDao + States<S>;

    fn list_versions_at<const S: u8, F: ListFilter, D>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        on: &<<D as ListDto>::Dao as VersionedDao>::VersionOrder,
        params: &ListQueryParams<D>,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        D: ListDto,
        <D as ListDto>::Dao: VersionedDao + States<S>;

    fn list_history_at<const S: u8, F: ListFilter, D>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        on: &<<D as ListDto>::Dao as VersionedDao>::VersionOrder,
        params: &ListQueryParams<D>,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        D: ListDto,
        <D as ListDto>::Dao: VersionedDao + States<S>;
}

pub(crate) fn select_at<const S: u8, D>(
    condition: impl SqlCondition,
    on: &D::VersionOrder,
) -> Result<SelectStatement, SqlError>
where
    D: VersionedDao + States<S>,
{
    let query = Query::select()
        .columns(D::columns())
        .from(D::table())
        .cond_where(
            Condition::all()
                .add(resolve_condition::<D>(condition)?)
                .add(resolve_condition::<D>(D::state())?)
                .add(Expr::col(D::version_order_col()).lte(on.as_sea_value())),
        )
        .take();
    Ok(query)
}

/// Build a CTE that ranks all versions by recency per partition.
///
/// Only the time upper-bound is applied here. The caller's condition
/// is intentionally **not** pushed into the CTE because `SqlCondition`
/// can be a non-partition filter (e.g., a FK like `function_version_id`
/// on the tables table). Moving such filters inside the CTE would change
/// which row is `rn=1`, altering query semantics. The condition is
/// applied on the outer query by [`select_latest_from_cte`] instead.
///
/// For partition-scoped queries, see `table_data_version::ranked_cte()`
/// which explicitly accepts the partition value and safely filters inside
/// the CTE.
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
/// by condition and state. This is the common outer query for
/// [`fetch_all_at`] and [`list_versions_at`].
fn select_latest_from_cte<const S: u8, D>(
    condition: impl SqlCondition,
) -> Result<SelectStatement, SqlError>
where
    D: VersionedDao + States<S>,
{
    let query = Query::select()
        .columns(D::columns())
        .from(Cte::Ranked)
        .and_where(Expr::col(Cte::Rn).eq(1))
        .cond_where(resolve_condition::<D>(condition)?)
        .cond_where(resolve_condition::<D>(D::state())?)
        .take();
    Ok(query)
}

impl<E> VersionedQueries for E
where
    E: SeaExecutor + Send,
{
    async fn fetch_one_at<const S: u8, D>(
        self,
        condition: impl SqlCondition,
        on: &D::VersionOrder,
    ) -> Result<D, SqlError>
    where
        D: VersionedDao + States<S>,
    {
        let r = select_latest_from_cte::<S, D>(condition)?
            .order_by_columns(D::order_by())
            .limit(1)
            .take()
            .with(latest_ranked_cte::<D>(on)?)
            .build_query_as()
            .fetch_one(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    async fn fetch_optional_at<const S: u8, D>(
        self,
        condition: impl SqlCondition,
        on: &D::VersionOrder,
    ) -> Result<Option<D>, SqlError>
    where
        D: VersionedDao + States<S>,
    {
        let r = select_latest_from_cte::<S, D>(condition)?
            .order_by_columns(D::order_by())
            .limit(1)
            .take()
            .with(latest_ranked_cte::<D>(on)?)
            .build_query_as()
            .fetch_optional(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    async fn fetch_all_at<const S: u8, D>(
        self,
        condition: impl SqlCondition,
        on: &D::VersionOrder,
    ) -> Result<Vec<D>, SqlError>
    where
        D: VersionedDao + States<S>,
    {
        let r = select_latest_from_cte::<S, D>(condition)?
            .order_by_columns(D::order_by())
            .take()
            .with(latest_ranked_cte::<D>(on)?)
            .build_query_as()
            .fetch_all(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    async fn list_versions_at<const S: u8, F: ListFilter, D>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        on: &<<D as ListDto>::Dao as VersionedDao>::VersionOrder,
        params: &ListQueryParams<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        D: ListDto,
        <D as ListDto>::Dao: VersionedDao + States<S>,
    {
        let with = latest_ranked_cte::<<D as ListDto>::Dao>(on)?;
        let base = select_latest_from_cte::<S, <D as ListDto>::Dao>(condition)?
            .cond_where(filter.condition::<<D as ListDto>::Dao>()?)
            .take();
        let mut query = params.apply_to(base).with(with);
        let daos: Vec<<D as ListDto>::Dao> = query
            .build_query_as()
            .fetch_all(self)
            .await
            .map_err(SqlError::on_read::<<D as ListDto>::Dao>)?;
        daos.iter()
            .map(|dao| D::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }

    async fn list_history_at<const S: u8, F: ListFilter, D>(
        self,
        condition: impl SqlCondition,
        filter: &F,
        on: &<<D as ListDto>::Dao as VersionedDao>::VersionOrder,
        params: &ListQueryParams<D>,
    ) -> Result<Vec<D>, SqlError>
    where
        D: ListDto,
        <D as ListDto>::Dao: VersionedDao + States<S>,
    {
        let base = select_at::<S, <D as ListDto>::Dao>(condition, on)?
            .cond_where(filter.condition::<<D as ListDto>::Dao>()?)
            .take();
        let mut query = params.apply_to(base);
        let daos: Vec<<D as ListDto>::Dao> = query
            .build_query_as()
            .fetch_all(self)
            .await
            .map_err(SqlError::on_read::<<D as ListDto>::Dao>)?;
        daos.iter()
            .map(|dao| D::from_dao(dao).map_err(SqlError::DtoConversionError))
            .collect()
    }
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
        let sql = select_at::<{ TestVersionedDao::All }, TestVersionedDao>(&id, &on)
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
        let sql = select_at::<{ TestVersionedDao::Active }, TestVersionedDao>(&id, &on)
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
        let sql = select_latest_from_cte::<{ TestVersionedDao::All }, TestVersionedDao>(&id)
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
    fn test_full_versioned_query() {
        let id = TestPartition::try_from("p1").unwrap();
        let on = TestDefinedOn::try_from(100i64).unwrap();
        let sql = select_latest_from_cte::<{ TestVersionedDao::Active }, TestVersionedDao>(&id)
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
