//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{
    Alias, Asterisk, CommonTableExpression, Condition, Expr, ExprTrait, Func, Iden, IntoTableRef,
    Order, OverStatement, Query, SubQueryStatement, WindowStatement, WithClause,
};
use ta_objects::SeaValue;
use ta_objects::errors::SqlError;
use ta_objects::sql_condition::resolve_condition;
use ta_objects::{DataVersionDao, States, VersionedDao};
use td_database::sea_query::builder::SeaQueryExt;
use td_database::sea_query::executor::SeaExecutor;
use td_objects::types::basic::TableDataVersionId;
use td_objects::types::table_ref::{Version, Versions};

#[derive(Iden, Copy, Clone)]
enum Cte {
    Ranked,
    Rn,
    TotalCount,
}

pub trait DataVersionsQueries {
    fn fetch_one_data_version_at<const S: u8, D>(
        self,
        partition: &<D as VersionedDao>::Partition,
        on: &<D as VersionedDao>::VersionOrder,
        versions: &Versions,
    ) -> impl Future<Output = Result<D, SqlError>>
    where
        D: DataVersionDao + States<S>;

    fn fetch_optional_data_version_at<const S: u8, D>(
        self,
        partition: &<D as VersionedDao>::Partition,
        on: &<D as VersionedDao>::VersionOrder,
        versions: &Versions,
    ) -> impl Future<Output = Result<Option<D>, SqlError>>
    where
        D: DataVersionDao + States<S>;

    fn fetch_all_data_versions_at<const S: u8, D>(
        self,
        partition: &<D as VersionedDao>::Partition,
        on: &<D as VersionedDao>::VersionOrder,
        versions: &Versions,
    ) -> impl Future<Output = Result<Vec<D>, SqlError>>
    where
        D: DataVersionDao + States<S>;

    fn fetch_relative_offset_at<const S: u8, D>(
        self,
        partition: &<D as VersionedDao>::Partition,
        on: &<D as VersionedDao>::VersionOrder,
        version: &Version,
    ) -> impl Future<Output = Result<Option<i32>, SqlError>>
    where
        D: DataVersionDao + States<S>;
}

/// Builds the ranked CTE:
/// ```sql
/// WITH ranked AS (
///     SELECT columns,
///         ROW_NUMBER() OVER (PARTITION BY partition ORDER BY version_order DESC) AS rn,
///         COUNT(*) OVER (PARTITION BY partition) AS total_count
///     FROM table
///     WHERE partition = ? AND version_order <= ?
/// )
/// ```
fn ranked_cte<D>(
    partition: &<D as VersionedDao>::Partition,
    on: &<D as VersionedDao>::VersionOrder,
) -> Result<WithClause, SqlError>
where
    D: DataVersionDao,
{
    let rn_window = WindowStatement::new()
        .partition_by(D::partition_col())
        .order_by_expr(Expr::col(D::version_order_col()), Order::Desc)
        .order_by_expr(Expr::col(D::pk_col()), Order::Desc)
        .take();

    let count_window = WindowStatement::new()
        .partition_by(D::partition_col())
        .take();

    let ranked_query = Query::select()
        .columns(D::columns())
        .expr_window_as(
            Func::cust(Alias::new("ROW_NUMBER")).args(vec![]),
            rn_window,
            Cte::Rn,
        )
        .expr_window_as(
            Func::cust(Alias::new("COUNT")).args(vec![Expr::col(Asterisk)]),
            count_window,
            Cte::TotalCount,
        )
        .from(D::table())
        .cond_where(
            Condition::all()
                .add(Expr::col(D::partition_col()).eq(partition.as_sea_value()))
                .add(Expr::col(D::version_order_col()).lte(on.as_sea_value())),
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

/// Builds WHERE conditions for version filtering.
fn version_condition<D>(versions: &Versions) -> Result<Condition, SqlError>
where
    D: DataVersionDao,
{
    match versions {
        Versions::None => Ok(Condition::all().add(Expr::col(Cte::Rn).eq(1))),
        Versions::Single(version) => Ok(Condition::all().add(single_version_expr::<D>(version)?)),
        Versions::List(versions) => {
            if versions.is_empty() {
                Ok(Condition::all().add(Expr::cust("1 = 0")))
            } else {
                let mut cond = Condition::any();
                for version in versions {
                    cond = cond.add(single_version_expr::<D>(version)?);
                }
                Ok(Condition::all().add(cond))
            }
        }
        Versions::Range(from, to) => Ok(Condition::all()
            .add(range_upper_bound::<D>(from)?)
            .add(range_lower_bound::<D>(to)?)),
        Versions::New => Err(SqlError::SelectVersionsNew),
    }
}

/// Expression for matching a single version.
fn single_version_expr<D>(version: &Version) -> Result<Expr, SqlError>
where
    D: DataVersionDao,
{
    match version {
        Version::Fixed(id) => {
            let col = D::col_for_type(id.as_type_id()).ok_or_else(|| {
                SqlError::TypeNotFound(
                    std::any::type_name_of_val(id).to_string(),
                    Box::new(D::table().into_table_ref()),
                )
            })?;
            Ok(Expr::col(col).eq(id.as_sea_value()))
        }
        Version::Head(back) => Ok(Expr::col(Cte::Rn).eq((-back + 1) as i64)),
        Version::Initial(forward) => {
            Ok(Expr::col(Cte::Rn).eq(Expr::col(Cte::TotalCount).sub(*forward as i64)))
        }
    }
}

/// Subquery to find the rn of a fixed version in the ranked CTE.
fn fixed_version_rn_subquery<D>(id: &TableDataVersionId) -> Result<Expr, SqlError>
where
    D: DataVersionDao,
{
    let col = D::col_for_type(id.as_type_id()).ok_or_else(|| {
        SqlError::TypeNotFound(
            std::any::type_name_of_val(id).to_string(),
            Box::new(D::table().into_table_ref()),
        )
    })?;
    let sub = Query::select()
        .column(Cte::Rn)
        .from(Cte::Ranked)
        .and_where(Expr::col(col).eq(id.as_sea_value()))
        .take();
    Ok(Expr::SubQuery(
        None,
        Box::new(SubQueryStatement::SelectStatement(sub)),
    ))
}

/// Upper bound for range: rn <= bound (from = older end, larger rn).
fn range_upper_bound<D>(version: &Version) -> Result<Expr, SqlError>
where
    D: DataVersionDao,
{
    match version {
        Version::Fixed(id) => Ok(Expr::col(Cte::Rn).lte(fixed_version_rn_subquery::<D>(id)?)),
        Version::Head(back) => Ok(Expr::col(Cte::Rn).lte((-back + 1) as i64)),
        Version::Initial(forward) => {
            Ok(Expr::col(Cte::Rn).lte(Expr::col(Cte::TotalCount).sub(*forward as i64)))
        }
    }
}

/// Lower bound for range: rn >= bound (to = newer end, smaller rn).
fn range_lower_bound<D>(version: &Version) -> Result<Expr, SqlError>
where
    D: DataVersionDao,
{
    match version {
        Version::Fixed(id) => Ok(Expr::col(Cte::Rn).gte(fixed_version_rn_subquery::<D>(id)?)),
        Version::Head(back) => Ok(Expr::col(Cte::Rn).gte((-back + 1) as i64)),
        Version::Initial(forward) => {
            Ok(Expr::col(Cte::Rn).gte(Expr::col(Cte::TotalCount).sub(*forward as i64)))
        }
    }
}

impl<E> DataVersionsQueries for E
where
    E: SeaExecutor + Send,
{
    async fn fetch_one_data_version_at<const S: u8, D>(
        self,
        partition: &<D as VersionedDao>::Partition,
        on: &<D as VersionedDao>::VersionOrder,
        versions: &Versions,
    ) -> Result<D, SqlError>
    where
        D: DataVersionDao + States<S>,
    {
        let r = Query::select()
            .columns(D::columns())
            .from(Cte::Ranked)
            .cond_where(
                Condition::all()
                    .add(resolve_condition::<D>(D::state())?)
                    .add(version_condition::<D>(versions)?),
            )
            .limit(1)
            .take()
            .with(ranked_cte::<D>(partition, on)?)
            .build_query_as()
            .fetch_one(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    async fn fetch_optional_data_version_at<const S: u8, D>(
        self,
        partition: &<D as VersionedDao>::Partition,
        on: &<D as VersionedDao>::VersionOrder,
        versions: &Versions,
    ) -> Result<Option<D>, SqlError>
    where
        D: DataVersionDao + States<S>,
    {
        let r = Query::select()
            .columns(D::columns())
            .from(Cte::Ranked)
            .cond_where(
                Condition::all()
                    .add(resolve_condition::<D>(D::state())?)
                    .add(version_condition::<D>(versions)?),
            )
            .limit(1)
            .take()
            .with(ranked_cte::<D>(partition, on)?)
            .build_query_as()
            .fetch_optional(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    async fn fetch_all_data_versions_at<const S: u8, D>(
        self,
        partition: &<D as VersionedDao>::Partition,
        on: &<D as VersionedDao>::VersionOrder,
        versions: &Versions,
    ) -> Result<Vec<D>, SqlError>
    where
        D: DataVersionDao + States<S>,
    {
        let mut q = Query::select();
        q.columns(D::columns()).from(Cte::Ranked).cond_where(
            Condition::all()
                .add(resolve_condition::<D>(D::state())?)
                .add(version_condition::<D>(versions)?),
        );

        // Range queries order by rn DESC (older versions first)
        if matches!(versions, Versions::Range(_, _)) {
            q.order_by(Cte::Rn, Order::Desc);
        }

        let r = q
            .take()
            .with(ranked_cte::<D>(partition, on)?)
            .build_query_as()
            .fetch_all(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    async fn fetch_relative_offset_at<const S: u8, D>(
        self,
        partition: &<D as VersionedDao>::Partition,
        on: &<D as VersionedDao>::VersionOrder,
        version: &Version,
    ) -> Result<Option<i32>, SqlError>
    where
        D: DataVersionDao + States<S>,
    {
        let r = Query::select()
            .expr(Expr::col(Cte::Rn))
            .from(Cte::Ranked)
            .cond_where(
                Condition::all()
                    .add(resolve_condition::<D>(D::state())?)
                    .add(single_version_expr::<D>(version)?),
            )
            .limit(1)
            .take()
            .with(ranked_cte::<D>(partition, on)?)
            .build_query_scalar::<i32>()
            .fetch_optional(self)
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Utc};
    use std::ops::Deref;
    use std::sync::LazyLock;
    use ta_objects::Dao;
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::types::basic::AtTime;

    #[ta_objects::typed(string)]
    #[derive(Eq, PartialEq)]
    struct TestPartition;

    #[ta_objects::typed_enum]
    #[derive(Eq, PartialEq)]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
        #[typed_enum(rename = "D")]
        Deleted,
    }

    #[Dao]
    #[derive(Eq, PartialEq)]
    #[dao(
        data_version,
        sql_table = "test_dv_table",
        versioned(pk = "id", version_by = "defined_on", partition_by = "partition_id"),
        states(
            Active = TestStatus::Active,
        )
    )]
    struct TestDvDao {
        id: TableDataVersionId,
        partition_id: TestPartition,
        status: TestStatus,
        defined_on: AtTime,
    }

    static P0: LazyLock<TestPartition> = LazyLock::new(|| TestPartition::try_from("p0").unwrap());
    static P1: LazyLock<TestPartition> = LazyLock::new(|| TestPartition::try_from("p1").unwrap());

    /// After all fixture data.
    static ON: LazyLock<AtTime> = LazyLock::new(|| {
        AtTime::try_from(
            "2026-01-01T00:00:10.000+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )
        .unwrap()
    });

    /// Between t2 and t3: includes DV_1 and DV_2 for p0 (defined_on <= ON_T2).
    static ON_T2: LazyLock<AtTime> = LazyLock::new(|| {
        AtTime::try_from(
            "2026-01-01T00:00:02.500+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )
        .unwrap()
    });

    static DV_1: LazyLock<TableDataVersionId> =
        LazyLock::new(|| TableDataVersionId::try_from("DV20260101.0000000001").unwrap());
    static DV_2: LazyLock<TableDataVersionId> =
        LazyLock::new(|| TableDataVersionId::try_from("DV20260101.0000000002").unwrap());
    static DV_3: LazyLock<TableDataVersionId> =
        LazyLock::new(|| TableDataVersionId::try_from("DV20260101.0000000003").unwrap());
    static _DV_4: LazyLock<TableDataVersionId> =
        LazyLock::new(|| TableDataVersionId::try_from("DV20260101.0000000004").unwrap());
    static DV_5: LazyLock<TableDataVersionId> =
        LazyLock::new(|| TableDataVersionId::try_from("DV20260101.0000000005").unwrap());

    // --- Versions::None ---

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_versions_none_returns_head(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = (&db)
            .fetch_one_data_version_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Versions::None,
            )
            .await?;
        assert_eq!(r.id, *DV_3);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_versions_none_optional_no_match(db: TdPool) -> Result<(), TdError> {
        let before = AtTime::try_from(
            "2025-12-31T00:00:00.000+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )?;
        let r = (&db)
            .fetch_optional_data_version_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                &before,
                &Versions::None,
            )
            .await?;
        assert!(r.is_none());
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_versions_none_fetch_all(db: TdPool) -> Result<(), TdError> {
        let r = (&db)
            .fetch_all_data_versions_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Versions::None,
            )
            .await?;
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].id, *DV_3);
        Ok(())
    }

    // --- Versions::Single(Head) ---

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_single_head(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = (&db)
            .fetch_one_data_version_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Versions::Single(Version::Head(0)),
            )
            .await?;
        assert_eq!(r.id, *DV_3);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_single_head_back_1(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = (&db)
            .fetch_one_data_version_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Versions::Single(Version::Head(-1)),
            )
            .await?;
        assert_eq!(r.id, *DV_2);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_single_head_back_2(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = (&db)
            .fetch_one_data_version_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Versions::Single(Version::Head(-2)),
            )
            .await?;
        assert_eq!(r.id, *DV_1);
        Ok(())
    }

    // --- Versions::Single(Initial) ---

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_single_initial(db: TdPool) -> Result<(), TdError> {
        // INITIAL: rn == total_count - 0 = 3 -> DV_1
        let r: TestDvDao = (&db)
            .fetch_one_data_version_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Versions::Single(Version::Initial(0)),
            )
            .await?;
        assert_eq!(r.id, *DV_1);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_single_initial_forward_1(db: TdPool) -> Result<(), TdError> {
        // INITIAL~1: rn == total_count - 1 = 2 -> DV_2
        let r: TestDvDao = (&db)
            .fetch_one_data_version_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Versions::Single(Version::Initial(1)),
            )
            .await?;
        assert_eq!(r.id, *DV_2);
        Ok(())
    }

    // --- Versions::Single(Fixed) ---

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_single_fixed(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = (&db)
            .fetch_one_data_version_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Versions::Single(Version::Fixed(DV_2.clone())),
            )
            .await?;
        assert_eq!(r.id, *DV_2);
        Ok(())
    }

    // --- Versions::List ---

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_list_empty(db: TdPool) -> Result<(), TdError> {
        let r = (&db)
            .fetch_all_data_versions_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Versions::List(vec![]),
            )
            .await?;
        assert_eq!(r.len(), 0);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_list_multiple(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::List(vec![Version::Head(0), Version::Head(-2)]);
        let r = (&db)
            .fetch_all_data_versions_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &versions,
            )
            .await?;
        assert_eq!(r.len(), 2);
        let ids: Vec<_> = r.iter().map(|d| &d.id).collect();
        assert!(ids.contains(&&*DV_3)); // HEAD
        assert!(ids.contains(&&*DV_1)); // HEAD~2
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_list_fixed_ids(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::List(vec![
            Version::Fixed(DV_1.clone()),
            Version::Fixed(DV_3.clone()),
        ]);
        let r = (&db)
            .fetch_all_data_versions_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &versions,
            )
            .await?;
        assert_eq!(r.len(), 2);
        let ids: Vec<_> = r.iter().map(|d| &d.id).collect();
        assert!(ids.contains(&&*DV_1));
        assert!(ids.contains(&&*DV_3));
        Ok(())
    }

    // --- Versions::Range ---

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_range_head(db: TdPool) -> Result<(), TdError> {
        // HEAD~2..HEAD = all 3 versions, ordered rn DESC (oldest first)
        let versions = Versions::Range(Version::Head(-2), Version::Head(0));
        let r = (&db)
            .fetch_all_data_versions_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &versions,
            )
            .await?;
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].id, *DV_1); // rn=3 (oldest)
        assert_eq!(r[1].id, *DV_2); // rn=2
        assert_eq!(r[2].id, *DV_3); // rn=1 (newest)
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_range_initial(db: TdPool) -> Result<(), TdError> {
        // INITIAL..INITIAL~1 -> rn between total_count-1=2 and total_count-0=3
        let versions = Versions::Range(Version::Initial(0), Version::Initial(1));
        let r = (&db)
            .fetch_all_data_versions_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &versions,
            )
            .await?;
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].id, *DV_1); // rn=3
        assert_eq!(r[1].id, *DV_2); // rn=2
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_range_fixed(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::Range(Version::Fixed(DV_1.clone()), Version::Fixed(DV_3.clone()));
        let r = (&db)
            .fetch_all_data_versions_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &versions,
            )
            .await?;
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].id, *DV_1);
        assert_eq!(r[1].id, *DV_2);
        assert_eq!(r[2].id, *DV_3);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_range_empty_when_reversed(db: TdPool) -> Result<(), TdError> {
        // HEAD..HEAD~2 -> rn <= 1 AND rn >= 3 -> empty
        let versions = Versions::Range(Version::Head(0), Version::Head(-2));
        let r = (&db)
            .fetch_all_data_versions_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &versions,
            )
            .await?;
        assert_eq!(r.len(), 0);
        Ok(())
    }

    // --- Versions::New ---

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_versions_new_returns_error(db: TdPool) {
        let r = (&db)
            .fetch_one_data_version_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Versions::New,
            )
            .await;
        assert!(r.is_err());
    }

    // --- State filtering ---

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_state_active_filters_deleted(db: TdPool) -> Result<(), TdError> {
        // p1: DV_5 (D, rn=1), DV_4 (A, rn=2)
        // Active + Versions::None (rn=1) -> DV_5 is deleted -> no match
        let r = (&db)
            .fetch_optional_data_version_at::<{ TestDvDao::Active }, TestDvDao>(
                P1.deref(),
                ON.deref(),
                &Versions::None,
            )
            .await?;
        assert!(r.is_none());
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_state_all_includes_deleted(db: TdPool) -> Result<(), TdError> {
        // p1: DV_5 (D, rn=1) is HEAD; All state includes it
        let r: TestDvDao = (&db)
            .fetch_one_data_version_at::<{ TestDvDao::All }, TestDvDao>(
                P1.deref(),
                ON.deref(),
                &Versions::None,
            )
            .await?;
        assert_eq!(r.id, *DV_5);
        Ok(())
    }

    // --- on parameter filtering ---

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_on_parameter_filters_newer_versions(db: TdPool) -> Result<(), TdError> {
        // on = t2: only DV_1 (t1) and DV_2 (t2) in CTE for p0
        // Versions::None = rn=1 = DV_2
        let r: TestDvDao = (&db)
            .fetch_one_data_version_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON_T2.deref(),
                &Versions::None,
            )
            .await?;
        assert_eq!(r.id, *DV_2);
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_on_parameter_initial_with_limited_versions(db: TdPool) -> Result<(), TdError> {
        // on = t2: p0 has DV_1 and DV_2 (total_count=2)
        // INITIAL = rn = total_count - 0 = 2 -> DV_1
        let r: TestDvDao = (&db)
            .fetch_one_data_version_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON_T2.deref(),
                &Versions::Single(Version::Initial(0)),
            )
            .await?;
        assert_eq!(r.id, *DV_1);
        Ok(())
    }

    // --- fetch_relative_offset_at ---

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_relative_offset_head(db: TdPool) -> Result<(), TdError> {
        let r = (&db)
            .fetch_relative_offset_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Version::Head(0),
            )
            .await?;
        assert_eq!(r, Some(1));
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_relative_offset_fixed(db: TdPool) -> Result<(), TdError> {
        let r = (&db)
            .fetch_relative_offset_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Version::Fixed(DV_2.clone()),
            )
            .await?;
        assert_eq!(r, Some(2));
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_relative_offset_initial(db: TdPool) -> Result<(), TdError> {
        let r = (&db)
            .fetch_relative_offset_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Version::Initial(0),
            )
            .await?;
        assert_eq!(r, Some(3));
        Ok(())
    }

    #[td_test::test(sqlx(fixture = crate::fixtures::DataVersion))]
    #[tokio::test]
    async fn test_relative_offset_nonexistent(db: TdPool) -> Result<(), TdError> {
        let nonexistent = TableDataVersionId::try_from("DV20260101.9999999999")?;
        let r = (&db)
            .fetch_relative_offset_at::<{ TestDvDao::All }, TestDvDao>(
                P0.deref(),
                ON.deref(),
                &Version::Fixed(nonexistent),
            )
            .await?;
        assert!(r.is_none());
        Ok(())
    }

    // --- SQL generation tests ---

    mod sql {
        use super::*;
        use sea_query::{Asterisk, Query, SqliteQueryBuilder};

        #[test]
        fn test_ranked_cte_sql() {
            let with = ranked_cte::<TestDvDao>(P0.deref(), ON.deref()).unwrap();
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Ranked)
                .take()
                .with(with)
                .to_string(SqliteQueryBuilder);
            assert!(sql.starts_with("WITH"), "should be a WITH query: {sql}");
            assert!(sql.contains("ROW_NUMBER()"), "should use ROW_NUMBER: {sql}");
            assert!(
                sql.contains("COUNT(*)"),
                "should include COUNT for total_count: {sql}"
            );
            assert!(
                sql.contains(r#""partition_id" = 'p0'"#),
                "should filter by partition: {sql}"
            );
        }

        #[test]
        fn test_version_condition_none() {
            let cond = version_condition::<TestDvDao>(&Versions::None).unwrap();
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Ranked)
                .cond_where(cond)
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains(r#""rn" = 1"#),
                "Versions::None should produce rn = 1: {sql}"
            );
        }

        #[test]
        fn test_version_condition_single_head() {
            let cond = version_condition::<TestDvDao>(&Versions::Single(Version::Head(0))).unwrap();
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Ranked)
                .cond_where(cond)
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains(r#""rn" = 1"#),
                "HEAD should produce rn = 1: {sql}"
            );
        }

        #[test]
        fn test_version_condition_single_head_back() {
            let cond =
                version_condition::<TestDvDao>(&Versions::Single(Version::Head(-2))).unwrap();
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Ranked)
                .cond_where(cond)
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains(r#""rn" = 3"#),
                "HEAD~2 should produce rn = 3: {sql}"
            );
        }

        #[test]
        fn test_version_condition_single_initial() {
            let cond =
                version_condition::<TestDvDao>(&Versions::Single(Version::Initial(0))).unwrap();
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Ranked)
                .cond_where(cond)
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains(r#""rn" = "total_count""#),
                "INITIAL should produce rn = total_count: {sql}"
            );
        }

        #[test]
        fn test_version_condition_single_fixed() {
            let dv = TableDataVersionId::try_from("DV20260101.0000000002").unwrap();
            let cond =
                version_condition::<TestDvDao>(&Versions::Single(Version::Fixed(dv))).unwrap();
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Ranked)
                .cond_where(cond)
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains(r#""id" = 'DV20260101.0000000002'"#),
                "Fixed version should match id: {sql}"
            );
        }

        #[test]
        fn test_version_condition_list_empty() {
            let cond = version_condition::<TestDvDao>(&Versions::List(vec![])).unwrap();
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Ranked)
                .cond_where(cond)
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains("1 = 0"),
                "empty list should produce 1 = 0: {sql}"
            );
        }

        #[test]
        fn test_version_condition_list_multiple() {
            let cond = version_condition::<TestDvDao>(&Versions::List(vec![
                Version::Head(0),
                Version::Head(-1),
            ]))
            .unwrap();
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Ranked)
                .cond_where(cond)
                .to_string(SqliteQueryBuilder);
            assert!(sql.contains("OR"), "list should use OR: {sql}");
        }

        #[test]
        fn test_version_condition_range() {
            let cond = version_condition::<TestDvDao>(&Versions::Range(
                Version::Head(-2),
                Version::Head(0),
            ))
            .unwrap();
            let sql = Query::select()
                .column(Asterisk)
                .from(Cte::Ranked)
                .cond_where(cond)
                .to_string(SqliteQueryBuilder);
            assert!(
                sql.contains(r#""rn" <="#) && sql.contains(r#""rn" >="#),
                "range should bound rn: {sql}"
            );
        }

        #[test]
        fn test_version_condition_new_errors() {
            let result = version_condition::<TestDvDao>(&Versions::New);
            assert!(result.is_err(), "Versions::New should error");
        }
    }
}
