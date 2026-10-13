//
// Copyright 2026 Tabsdata Inc.
//

use crate::{QueryExecutor, Serves};
use sea_query::{
    Alias, Asterisk, Condition, Expr, ExprTrait, Func, Order, Query, SelectStatement,
    SubQueryStatement,
};
use ta_objects::errors::SqlError;
use ta_objects::{HasColumn, SelectDao, TimelineDao, single_row};
use ta_objects::{SameInner, SeaValue};
use td_database::domain::Domained;
use td_database::sea_query::builder::SeaQueryExt;
use td_objects::types::basic::LogicalTableDataVersionId;
use td_objects::types::table_ref::{Version, Versions};

/// Resolves [`Versions`] selectors against a [`TimelineDao`]'s timeline in
/// `partition` at `on` through top-k scans and key probes on the
/// `(partition, version_order, pk)` index: `HEAD~k` touches `k + 1` entries,
/// never the whole timeline.
pub struct DataVersion<'a, E, V, P> {
    pub(crate) on: &'a V,
    pub(crate) partition: &'a P,
    pub(crate) executor: E,
}

fn timeline_cond<D, V>(partition: &D::Partition, on: &V) -> Condition
where
    D: SelectDao + TimelineDao,
    V: SameInner<D::VersionOrder>,
{
    Condition::all()
        .add(Expr::col(D::PARTITION).eq(partition.as_sea_expr()))
        .add(Expr::col(D::VERSION_ORDER).lte(on.as_sea_expr()))
}

fn key_expr<D>() -> Expr
where
    D: SelectDao + TimelineDao,
{
    Expr::tuple([Expr::col(D::VERSION_ORDER), Expr::col(D::PK)])
}

fn order_timeline<D>(query: &mut SelectStatement, newest_first: bool)
where
    D: SelectDao + TimelineDao,
{
    let dir = if newest_first {
        Order::Desc
    } else {
        Order::Asc
    };
    query
        .order_by(D::VERSION_ORDER, dir.clone())
        .order_by(D::PK, dir);
}

fn timeline_scan<D, V>(
    partition: &D::Partition,
    on: &V,
    newest_first: bool,
    offset: u64,
    limit: u64,
) -> SelectStatement
where
    D: SelectDao + TimelineDao,
    V: SameInner<D::VersionOrder>,
{
    let mut query = Query::select();
    query
        .columns(D::columns())
        .from(D::TABLE)
        .cond_where(timeline_cond::<D, V>(partition, on));
    order_timeline::<D>(&mut query, newest_first);
    query.offset(offset).limit(limit);
    query.take()
}

fn subquery_expr(query: SelectStatement) -> Expr {
    Expr::SubQuery(None, Box::new(SubQueryStatement::SelectStatement(query)))
}

/// Yields no row beyond the timeline, so comparisons against it match nothing.
fn key_at<D, V>(partition: &D::Partition, on: &V, newest_first: bool, offset: u64) -> Expr
where
    D: SelectDao + TimelineDao,
    V: SameInner<D::VersionOrder>,
{
    let mut probe = Query::select();
    probe
        .column(D::VERSION_ORDER)
        .column(D::PK)
        .from(D::TABLE)
        .cond_where(timeline_cond::<D, V>(partition, on));
    order_timeline::<D>(&mut probe, newest_first);
    probe.offset(offset).limit(1);
    subquery_expr(probe.take())
}

fn desc_offset(back: isize) -> Option<u64> {
    u64::try_from(-(back as i128)).ok()
}

fn asc_offset(forward: isize) -> Option<u64> {
    u64::try_from(forward as i128).ok()
}

fn single_version_expr<D, V>(partition: &D::Partition, on: &V, version: &Version) -> Expr
where
    D: SelectDao + TimelineDao + HasColumn<LogicalTableDataVersionId>,
    V: SameInner<D::VersionOrder>,
{
    match version {
        Version::Fixed(id) => {
            let col = <D as HasColumn<LogicalTableDataVersionId>>::COL;
            Expr::col(col).eq(id.as_sea_expr())
        }
        Version::Head(back) => match desc_offset(*back) {
            Some(offset) => key_expr::<D>().eq(key_at::<D, V>(partition, on, true, offset)),
            None => Expr::Constant(false.into()),
        },
        Version::Initial(forward) => match asc_offset(*forward) {
            Some(offset) => key_expr::<D>().eq(key_at::<D, V>(partition, on, false, offset)),
            None => Expr::Constant(false.into()),
        },
    }
}

fn version_condition<D, V>(
    partition: &D::Partition,
    on: &V,
    versions: &Versions,
) -> Result<Condition, SqlError>
where
    D: SelectDao + TimelineDao + HasColumn<LogicalTableDataVersionId>,
    V: SameInner<D::VersionOrder>,
{
    match versions {
        Versions::None => Ok(Condition::all().add(single_version_expr::<D, V>(
            partition,
            on,
            &Version::Head(0),
        ))),
        Versions::Single(version) => {
            Ok(Condition::all().add(single_version_expr::<D, V>(partition, on, version)))
        }
        Versions::List(versions) => {
            if versions.is_empty() {
                Ok(Condition::all().add(Expr::Constant(false.into())))
            } else {
                let mut cond = Condition::any();
                for version in versions {
                    cond = cond.add(single_version_expr::<D, V>(partition, on, version));
                }
                Ok(Condition::all().add(cond))
            }
        }
        Versions::Range(_, _) => unreachable!("ranges resolve via range_scan"),
        Versions::New => Err(SqlError::SelectVersionsNew),
    }
}

/// `limit == 0` encodes the empty range.
struct Scan {
    newest_first: bool,
    offset: u64,
    limit: u64,
}

impl Scan {
    const EMPTY: Scan = Scan {
        newest_first: true,
        offset: 0,
        limit: 0,
    };
}

/// `from` is the older bound (larger offset), `to` the newer.
fn head_range_scan(from_back: isize, to_back: isize) -> Scan {
    let newest = (-(to_back as i128)).max(0);
    let oldest = -(from_back as i128);
    if oldest < newest {
        return Scan::EMPTY;
    }
    Scan {
        newest_first: true,
        offset: newest as u64,
        limit: (oldest - newest + 1) as u64,
    }
}

fn initial_range_scan(from_forward: isize, to_forward: isize) -> Scan {
    let oldest = (from_forward as i128).max(0);
    let newest = to_forward as i128;
    if newest < oldest {
        return Scan::EMPTY;
    }
    Scan {
        newest_first: false,
        offset: oldest as u64,
        limit: (newest - oldest + 1) as u64,
    }
}

/// 1-based position from the newest end; 0 when `key` yields no row.
async fn count_from_key<D, E, V>(
    executor: &mut E,
    partition: &D::Partition,
    on: &V,
    key: Expr,
) -> Result<i64, SqlError>
where
    D: SelectDao + TimelineDao,
    V: SameInner<D::VersionOrder>,
    E: QueryExecutor + Domained<Domain = D::Db>,
{
    Query::select()
        .expr(Func::count(Expr::col(Asterisk)))
        .from(D::TABLE)
        .cond_where(timeline_cond::<D, V>(partition, on).add(key_expr::<D>().gte(key)))
        .take()
        .build_query_scalar::<i64>()
        .fetch_one(executor.reuse())
        .await
        .map_err(SqlError::on_read::<D>)
}

async fn timeline_len<D, E, V>(
    executor: &mut E,
    partition: &D::Partition,
    on: &V,
) -> Result<i128, SqlError>
where
    D: SelectDao + TimelineDao,
    V: SameInner<D::VersionOrder>,
    E: QueryExecutor + Domained<Domain = D::Db>,
{
    let len = Query::select()
        .expr(Func::count(Expr::col(Asterisk)))
        .from(D::TABLE)
        .cond_where(timeline_cond::<D, V>(partition, on))
        .take()
        .build_query_scalar::<i64>()
        .fetch_one(executor.reuse())
        .await
        .map_err(SqlError::on_read::<D>)?;
    Ok(len as i128)
}

/// 0-based position from the oldest end; `None` for an absent Fixed id.
async fn ascending_pos<D, E, V>(
    executor: &mut E,
    partition: &D::Partition,
    on: &V,
    total: i128,
    version: &Version,
) -> Result<Option<i128>, SqlError>
where
    D: SelectDao + TimelineDao + HasColumn<LogicalTableDataVersionId>,
    V: SameInner<D::VersionOrder>,
    E: QueryExecutor + Domained<Domain = D::Db>,
{
    match version {
        Version::Head(back) => Ok(Some(total - 1 + *back as i128)),
        Version::Initial(forward) => Ok(Some(*forward as i128)),
        Version::Fixed(id) => {
            let col = <D as HasColumn<LogicalTableDataVersionId>>::COL;
            let mut key_probe = Query::select();
            key_probe
                .column(D::VERSION_ORDER)
                .column(D::PK)
                .from(D::TABLE)
                .cond_where(timeline_cond::<D, V>(partition, on))
                .and_where(Expr::col(col).eq(id.as_sea_expr()))
                .limit(1);
            let rn =
                count_from_key::<D, E, V>(executor, partition, on, subquery_expr(key_probe.take()))
                    .await?;
            Ok((rn > 0).then(|| total - rn as i128))
        }
    }
}

async fn range_scan<D, E, V>(
    executor: &mut E,
    partition: &D::Partition,
    on: &V,
    from: &Version,
    to: &Version,
) -> Result<Scan, SqlError>
where
    D: SelectDao + TimelineDao + HasColumn<LogicalTableDataVersionId>,
    V: SameInner<D::VersionOrder>,
    E: QueryExecutor + Domained<Domain = D::Db>,
{
    match (from, to) {
        (Version::Head(f), Version::Head(t)) => Ok(head_range_scan(*f, *t)),
        (Version::Initial(f), Version::Initial(t)) => Ok(initial_range_scan(*f, *t)),
        _ => {
            let total = timeline_len::<D, E, V>(executor, partition, on).await?;
            let Some(pos_from) =
                ascending_pos::<D, E, V>(executor, partition, on, total, from).await?
            else {
                return Ok(Scan::EMPTY);
            };
            let Some(pos_to) = ascending_pos::<D, E, V>(executor, partition, on, total, to).await?
            else {
                return Ok(Scan::EMPTY);
            };
            let oldest = pos_from.max(0);
            let newest = pos_to.min(total - 1);
            if newest < oldest {
                return Ok(Scan::EMPTY);
            }
            Ok(Scan {
                newest_first: false,
                offset: oldest as u64,
                limit: (newest - oldest + 1) as u64,
            })
        }
    }
}

fn scan_select<D, V>(partition: &D::Partition, on: &V, scan: &Scan) -> SelectStatement
where
    D: SelectDao + TimelineDao,
    V: SameInner<D::VersionOrder>,
{
    let inner = timeline_scan::<D, V>(partition, on, scan.newest_first, scan.offset, scan.limit);
    let mut query = Query::select();
    query
        .columns(D::columns())
        .from_subquery(inner, Alias::new("positioned"));
    order_timeline::<D>(&mut query, false);
    query.take()
}

async fn select_for<D, E, V>(
    executor: &mut E,
    partition: &D::Partition,
    on: &V,
    versions: &Versions,
) -> Result<SelectStatement, SqlError>
where
    D: SelectDao + TimelineDao + HasColumn<LogicalTableDataVersionId>,
    V: SameInner<D::VersionOrder>,
    E: QueryExecutor + Domained<Domain = D::Db>,
{
    match versions {
        Versions::New => Err(SqlError::SelectVersionsNew),
        Versions::Range(from, to) => {
            let scan = range_scan::<D, E, V>(executor, partition, on, from, to).await?;
            Ok(scan_select::<D, V>(partition, on, &scan))
        }
        _ => {
            let where_cond = timeline_cond::<D, V>(partition, on)
                .add(version_condition::<D, V>(partition, on, versions)?);
            let mut query = Query::select();
            query
                .columns(D::columns())
                .from(D::TABLE)
                .cond_where(where_cond);
            Ok(query.take())
        }
    }
}

impl<E, V, P> DataVersion<'_, E, V, P> {
    pub async fn fetch_one<D>(self, versions: &Versions) -> Result<D, SqlError>
    where
        E: Serves<D>,
        D: SelectDao + TimelineDao<Partition = P> + HasColumn<LogicalTableDataVersionId>,
        V: SameInner<D::VersionOrder>,
    {
        self.fetch_optional(versions)
            .await?
            .ok_or_else(SqlError::not_found::<D>)
    }

    pub async fn fetch_optional<D>(self, versions: &Versions) -> Result<Option<D>, SqlError>
    where
        E: Serves<D>,
        D: SelectDao + TimelineDao<Partition = P> + HasColumn<LogicalTableDataVersionId>,
        V: SameInner<D::VersionOrder>,
    {
        let Self {
            on,
            partition,
            mut executor,
        } = self;
        let mut query = select_for::<D, E, V>(&mut executor, partition, on, versions).await?;
        query.limit(2);
        let rows = query
            .build_query_as()
            .fetch_all(executor.reuse())
            .await
            .map_err(SqlError::on_read::<D>)?;
        single_row::<D>(rows)
    }

    /// Ranges come back in chronological order (oldest first).
    pub async fn fetch_all<D>(self, versions: &Versions) -> Result<Vec<D>, SqlError>
    where
        E: Serves<D>,
        D: SelectDao + TimelineDao<Partition = P> + HasColumn<LogicalTableDataVersionId>,
        V: SameInner<D::VersionOrder>,
    {
        let Self {
            on,
            partition,
            mut executor,
        } = self;
        let mut query = select_for::<D, E, V>(&mut executor, partition, on, versions).await?;
        let r = query
            .build_query_as()
            .fetch_all(executor.reuse())
            .await
            .map_err(SqlError::on_read::<D>)?;
        Ok(r)
    }

    /// 1-based position from the newest end; `None` when the row doesn't exist.
    pub async fn fetch_relative_offset<D>(self, version: &Version) -> Result<Option<i64>, SqlError>
    where
        E: Serves<D>,
        D: SelectDao + TimelineDao<Partition = P> + HasColumn<LogicalTableDataVersionId>,
        V: SameInner<D::VersionOrder>,
    {
        let Self {
            on,
            partition,
            mut executor,
        } = self;
        let target = match version {
            Version::Fixed(id) => {
                let col = <D as HasColumn<LogicalTableDataVersionId>>::COL;
                let mut query = Query::select();
                query
                    .columns(D::columns())
                    .from(D::TABLE)
                    .cond_where(timeline_cond::<D, V>(partition, on))
                    .and_where(Expr::col(col).eq(id.as_sea_expr()))
                    .limit(1);
                query.take()
            }
            Version::Head(back) => match desc_offset(*back) {
                Some(offset) => timeline_scan::<D, V>(partition, on, true, offset, 1),
                None => return Ok(None),
            },
            Version::Initial(forward) => match asc_offset(*forward) {
                Some(offset) => timeline_scan::<D, V>(partition, on, false, offset, 1),
                None => return Ok(None),
            },
        };
        let mut target_key = Query::select();
        target_key
            .column(D::VERSION_ORDER)
            .column(D::PK)
            .from_subquery(target, Alias::new("target"));
        let rn = count_from_key::<D, E, V>(
            &mut executor,
            partition,
            on,
            subquery_expr(target_key.take()),
        )
        .await?;
        Ok((rn > 0).then_some(rn))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Queries;
    use chrono::{DateTime, Utc};
    use std::ops::Deref;
    use std::sync::LazyLock;
    use ta_objects::Dao;
    use ta_objects::{Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};
    use td_database::pool::TdPool;
    use td_error::TdError;
    use td_objects::types::basic::AtTime;
    use td_objects::types::basic::Scd2ValidFrom;

    #[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    struct TestPartition(String);

    #[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
    enum TestStatus {
        #[typed_enum(rename = "A")]
        Active,
        #[typed_enum(rename = "D")]
        Deleted,
    }

    #[Dao]
    #[derive(Eq, PartialEq)]
    #[dao(table = "test_dv_table", ops(select, insert), timeline)]
    struct TestDvDao {
        #[dao(pk)]
        id: LogicalTableDataVersionId,
        #[dao(partition_by)]
        partition_id: TestPartition,
        status: TestStatus,
        #[dao(timeline(version_by))]
        defined_on: Scd2ValidFrom,
    }

    static P0: LazyLock<TestPartition> = LazyLock::new(|| TestPartition::try_from("p0").unwrap());
    static P1: LazyLock<TestPartition> = LazyLock::new(|| TestPartition::try_from("p1").unwrap());

    static ON: LazyLock<AtTime> = LazyLock::new(|| {
        AtTime::from(
            "2026-01-01T00:00:10.000+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )
    });

    /// Between t2 and t3: p0 has DV_1 and DV_2.
    static ON_T2: LazyLock<AtTime> = LazyLock::new(|| {
        AtTime::from(
            "2026-01-01T00:00:02.500+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        )
    });

    static DV_1: LazyLock<LogicalTableDataVersionId> =
        LazyLock::new(|| LogicalTableDataVersionId::try_from("dv20260101-0000000001").unwrap());
    static DV_2: LazyLock<LogicalTableDataVersionId> =
        LazyLock::new(|| LogicalTableDataVersionId::try_from("dv20260101-0000000002").unwrap());
    static DV_3: LazyLock<LogicalTableDataVersionId> =
        LazyLock::new(|| LogicalTableDataVersionId::try_from("dv20260101-0000000003").unwrap());
    static _DV_4: LazyLock<LogicalTableDataVersionId> =
        LazyLock::new(|| LogicalTableDataVersionId::try_from("dv20260101-0000000004").unwrap());
    static DV_5: LazyLock<LogicalTableDataVersionId> =
        LazyLock::new(|| LogicalTableDataVersionId::try_from("dv20260101-0000000005").unwrap());

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_versions_none_returns_head(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_one::<TestDvDao>(&Versions::None)
            .await?;
        assert_eq!(r.id, *DV_3);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_versions_none_optional_no_match(db: TdPool) -> Result<(), TdError> {
        let before = AtTime::from(
            "2025-12-31T00:00:00.000+00:00"
                .parse::<DateTime<Utc>>()
                .unwrap(),
        );
        let r = db
            .timeline()
            .at(&before, P0.deref())
            .fetch_optional::<TestDvDao>(&Versions::None)
            .await?;
        assert!(r.is_none());
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_versions_none_fetch_all(db: TdPool) -> Result<(), TdError> {
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_all::<TestDvDao>(&Versions::None)
            .await?;
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].id, *DV_3);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_single_head(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_one::<TestDvDao>(&Versions::Single(Version::Head(0)))
            .await?;
        assert_eq!(r.id, *DV_3);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_single_head_back_1(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_one::<TestDvDao>(&Versions::Single(Version::Head(-1)))
            .await?;
        assert_eq!(r.id, *DV_2);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_single_head_back_2(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_one::<TestDvDao>(&Versions::Single(Version::Head(-2)))
            .await?;
        assert_eq!(r.id, *DV_1);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_single_initial(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_one::<TestDvDao>(&Versions::Single(Version::Initial(0)))
            .await?;
        assert_eq!(r.id, *DV_1);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_single_initial_forward_1(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_one::<TestDvDao>(&Versions::Single(Version::Initial(1)))
            .await?;
        assert_eq!(r.id, *DV_2);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_single_fixed(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_one::<TestDvDao>(&Versions::Single(Version::Fixed(DV_2.clone())))
            .await?;
        assert_eq!(r.id, *DV_2);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_list_empty(db: TdPool) -> Result<(), TdError> {
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_all::<TestDvDao>(&Versions::List(vec![]))
            .await?;
        assert_eq!(r.len(), 0);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_list_multiple(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::List(vec![Version::Head(0), Version::Head(-2)]);
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_all::<TestDvDao>(&versions)
            .await?;
        assert_eq!(r.len(), 2);
        let ids: Vec<_> = r.iter().map(|d| &d.id).collect();
        assert!(ids.contains(&&*DV_3));
        assert!(ids.contains(&&*DV_1));
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_list_fixed_ids(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::List(vec![
            Version::Fixed(DV_1.clone()),
            Version::Fixed(DV_3.clone()),
        ]);
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_all::<TestDvDao>(&versions)
            .await?;
        assert_eq!(r.len(), 2);
        let ids: Vec<_> = r.iter().map(|d| &d.id).collect();
        assert!(ids.contains(&&*DV_1));
        assert!(ids.contains(&&*DV_3));
        Ok(())
    }

    /// Selectors resolving to the same row return it once.
    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_list_overlapping_selectors_dedup(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::List(vec![
            Version::Head(0),
            Version::Initial(2),
            Version::Fixed(DV_3.clone()),
        ]);
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_all::<TestDvDao>(&versions)
            .await?;
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].id, *DV_3);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_list_single_row_fetch_one(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::List(vec![Version::Head(0), Version::Fixed(DV_3.clone())]);
        let r: TestDvDao = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_one::<TestDvDao>(&versions)
            .await?;
        assert_eq!(r.id, *DV_3);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_list_multiple_rows_fetch_one_errors(db: TdPool) {
        let versions = Versions::List(vec![Version::Head(0), Version::Head(-1)]);
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_one::<TestDvDao>(&versions)
            .await;
        assert!(matches!(r, Err(SqlError::MultipleRowsMatched(_))), "{r:?}");
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_range_multiple_rows_fetch_optional_errors(db: TdPool) {
        let versions = Versions::Range(Version::Head(-2), Version::Head(0));
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_optional::<TestDvDao>(&versions)
            .await;
        assert!(matches!(r, Err(SqlError::MultipleRowsMatched(_))), "{r:?}");
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_range_head(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::Range(Version::Head(-2), Version::Head(0));
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_all::<TestDvDao>(&versions)
            .await?;
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].id, *DV_1);
        assert_eq!(r[1].id, *DV_2);
        assert_eq!(r[2].id, *DV_3);
        Ok(())
    }

    /// A Head range wider than the timeline clamps to the rows that exist.
    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_range_head_clamps_to_timeline(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::Range(Version::Head(-5), Version::Head(0));
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_all::<TestDvDao>(&versions)
            .await?;
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].id, *DV_1);
        assert_eq!(r[2].id, *DV_3);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_range_initial(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::Range(Version::Initial(0), Version::Initial(1));
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_all::<TestDvDao>(&versions)
            .await?;
        assert_eq!(r.len(), 2);
        assert_eq!(r[0].id, *DV_1);
        assert_eq!(r[1].id, *DV_2);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_range_fixed(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::Range(Version::Fixed(DV_1.clone()), Version::Fixed(DV_3.clone()));
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_all::<TestDvDao>(&versions)
            .await?;
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].id, *DV_1);
        assert_eq!(r[1].id, *DV_2);
        assert_eq!(r[2].id, *DV_3);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_range_mixed_bounds(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::Range(Version::Initial(0), Version::Head(0));
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_all::<TestDvDao>(&versions)
            .await?;
        assert_eq!(r.len(), 3);
        assert_eq!(r[0].id, *DV_1);
        assert_eq!(r[2].id, *DV_3);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_range_empty_when_reversed(db: TdPool) -> Result<(), TdError> {
        let versions = Versions::Range(Version::Head(0), Version::Head(-2));
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_all::<TestDvDao>(&versions)
            .await?;
        assert_eq!(r.len(), 0);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_versions_new_returns_error(db: TdPool) {
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_one::<TestDvDao>(&Versions::New)
            .await;
        assert!(r.is_err());
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_head_includes_deleted(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = db
            .timeline()
            .at(ON.deref(), P1.deref())
            .fetch_one::<TestDvDao>(&Versions::None)
            .await?;
        assert_eq!(r.id, *DV_5);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_on_parameter_filters_newer_versions(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = db
            .timeline()
            .at(ON_T2.deref(), P0.deref())
            .fetch_one::<TestDvDao>(&Versions::None)
            .await?;
        assert_eq!(r.id, *DV_2);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_on_parameter_initial_with_limited_versions(db: TdPool) -> Result<(), TdError> {
        let r: TestDvDao = db
            .timeline()
            .at(ON_T2.deref(), P0.deref())
            .fetch_one::<TestDvDao>(&Versions::Single(Version::Initial(0)))
            .await?;
        assert_eq!(r.id, *DV_1);
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_relative_offset_head(db: TdPool) -> Result<(), TdError> {
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_relative_offset::<TestDvDao>(&Version::Head(0))
            .await?;
        assert_eq!(r, Some(1));
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_relative_offset_fixed(db: TdPool) -> Result<(), TdError> {
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_relative_offset::<TestDvDao>(&Version::Fixed(DV_2.clone()))
            .await?;
        assert_eq!(r, Some(2));
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_relative_offset_initial(db: TdPool) -> Result<(), TdError> {
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_relative_offset::<TestDvDao>(&Version::Initial(0))
            .await?;
        assert_eq!(r, Some(3));
        Ok(())
    }

    #[td_test::test(db(crate::fixtures::DataVersion))]
    async fn test_relative_offset_nonexistent(db: TdPool) -> Result<(), TdError> {
        let nonexistent = LogicalTableDataVersionId::try_from("dv20260101-9999999999")?;
        let r = db
            .timeline()
            .at(ON.deref(), P0.deref())
            .fetch_relative_offset::<TestDvDao>(&Version::Fixed(nonexistent))
            .await?;
        assert!(r.is_none());
        Ok(())
    }

    mod scans {
        use super::*;

        fn coords(scan: Scan) -> Option<(bool, u64, u64)> {
            (scan.limit > 0).then_some((scan.newest_first, scan.offset, scan.limit))
        }

        #[test]
        fn test_head_range_scan() {
            assert_eq!(coords(head_range_scan(-2, 0)), Some((true, 0, 3)));
            assert_eq!(coords(head_range_scan(0, 0)), Some((true, 0, 1)));
            assert_eq!(coords(head_range_scan(-5, -4)), Some((true, 4, 2)));
            assert_eq!(coords(head_range_scan(0, -2)), None, "reversed is empty");
            assert_eq!(coords(head_range_scan(1, 2)), None, "invalid backs");
        }

        #[test]
        fn test_initial_range_scan() {
            assert_eq!(coords(initial_range_scan(0, 2)), Some((false, 0, 3)));
            assert_eq!(coords(initial_range_scan(1, 1)), Some((false, 1, 1)));
            assert_eq!(coords(initial_range_scan(-2, 1)), Some((false, 0, 2)));
            assert_eq!(coords(initial_range_scan(2, 0)), None, "reversed is empty");
            assert_eq!(coords(initial_range_scan(0, -1)), None);
        }
    }

    mod sql {
        use super::*;
        use sea_query::PostgresQueryBuilder;
        use ta_objects::Dao;

        fn where_sql(cond: Condition) -> String {
            Query::select()
                .column(Asterisk)
                .from(TestDvDao::TABLE)
                .cond_where(cond)
                .to_string(PostgresQueryBuilder)
        }

        #[test]
        fn test_timeline_scan_sql() {
            let sql = timeline_scan::<TestDvDao, _>(P0.deref(), ON.deref(), true, 2, 1)
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"SELECT "id", "partition_id", "status", "defined_on" FROM "test_dv_table" WHERE "partition_id" = 'p0' AND "defined_on" <= '2026-01-01 00:00:10.000000 +00:00' ORDER BY "defined_on" DESC, "id" DESC LIMIT 1 OFFSET 2"#
            );
        }

        #[test]
        fn test_version_condition_head_is_key_probe() {
            let cond = version_condition::<TestDvDao, _>(
                P0.deref(),
                ON.deref(),
                &Versions::Single(Version::Head(-2)),
            )
            .unwrap();
            assert_eq!(
                where_sql(cond),
                r#"SELECT * FROM "test_dv_table" WHERE ("defined_on", "id") = (SELECT "defined_on", "id" FROM "test_dv_table" WHERE "partition_id" = 'p0' AND "defined_on" <= '2026-01-01 00:00:10.000000 +00:00' ORDER BY "defined_on" DESC, "id" DESC LIMIT 1 OFFSET 2)"#
            );
        }

        #[test]
        fn test_version_condition_initial_scans_ascending() {
            let cond = version_condition::<TestDvDao, _>(
                P0.deref(),
                ON.deref(),
                &Versions::Single(Version::Initial(1)),
            )
            .unwrap();
            assert_eq!(
                where_sql(cond),
                r#"SELECT * FROM "test_dv_table" WHERE ("defined_on", "id") = (SELECT "defined_on", "id" FROM "test_dv_table" WHERE "partition_id" = 'p0' AND "defined_on" <= '2026-01-01 00:00:10.000000 +00:00' ORDER BY "defined_on" ASC, "id" ASC LIMIT 1 OFFSET 1)"#
            );
        }

        #[test]
        fn test_version_condition_single_fixed() {
            let dv = LogicalTableDataVersionId::try_from("dv20260101-0000000002").unwrap();
            let cond = version_condition::<TestDvDao, _>(
                P0.deref(),
                ON.deref(),
                &Versions::Single(Version::Fixed(dv)),
            )
            .unwrap();
            assert_eq!(
                where_sql(cond),
                r#"SELECT * FROM "test_dv_table" WHERE "id" = 202601010000000002"#
            );
        }

        #[test]
        fn test_version_condition_invalid_offsets_match_nothing() {
            for versions in [
                Versions::Single(Version::Head(1)),
                Versions::Single(Version::Initial(-1)),
                Versions::List(vec![]),
            ] {
                let cond =
                    version_condition::<TestDvDao, _>(P0.deref(), ON.deref(), &versions).unwrap();
                assert_eq!(
                    where_sql(cond),
                    r#"SELECT * FROM "test_dv_table" WHERE FALSE"#
                );
            }
        }

        #[test]
        fn test_version_condition_list_multiple() {
            let cond = version_condition::<TestDvDao, _>(
                P0.deref(),
                ON.deref(),
                &Versions::List(vec![Version::Head(0), Version::Head(-1)]),
            )
            .unwrap();
            assert_eq!(
                where_sql(cond),
                r#"SELECT * FROM "test_dv_table" WHERE ("defined_on", "id") = (SELECT "defined_on", "id" FROM "test_dv_table" WHERE "partition_id" = 'p0' AND "defined_on" <= '2026-01-01 00:00:10.000000 +00:00' ORDER BY "defined_on" DESC, "id" DESC LIMIT 1 OFFSET 0) OR ("defined_on", "id") = (SELECT "defined_on", "id" FROM "test_dv_table" WHERE "partition_id" = 'p0' AND "defined_on" <= '2026-01-01 00:00:10.000000 +00:00' ORDER BY "defined_on" DESC, "id" DESC LIMIT 1 OFFSET 1)"#
            );
        }

        #[test]
        fn test_version_condition_new_errors() {
            let result = version_condition::<TestDvDao, _>(P0.deref(), ON.deref(), &Versions::New);
            assert!(matches!(result, Err(SqlError::SelectVersionsNew)));
        }

        #[test]
        fn test_scan_select_sql() {
            let scan = head_range_scan(-2, 0);
            let sql = scan_select::<TestDvDao, _>(P0.deref(), ON.deref(), &scan)
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"SELECT "id", "partition_id", "status", "defined_on" FROM (SELECT "id", "partition_id", "status", "defined_on" FROM "test_dv_table" WHERE "partition_id" = 'p0' AND "defined_on" <= '2026-01-01 00:00:10.000000 +00:00' ORDER BY "defined_on" DESC, "id" DESC LIMIT 3 OFFSET 0) AS "positioned" ORDER BY "defined_on" ASC, "id" ASC"#
            );
        }

        #[test]
        fn test_empty_scan_renders_limit_zero() {
            let sql = scan_select::<TestDvDao, _>(P0.deref(), ON.deref(), &Scan::EMPTY)
                .to_string(PostgresQueryBuilder);
            assert_eq!(
                sql,
                r#"SELECT "id", "partition_id", "status", "defined_on" FROM (SELECT "id", "partition_id", "status", "defined_on" FROM "test_dv_table" WHERE "partition_id" = 'p0' AND "defined_on" <= '2026-01-01 00:00:10.000000 +00:00' ORDER BY "defined_on" DESC, "id" DESC LIMIT 0 OFFSET 0) AS "positioned" ORDER BY "defined_on" ASC, "id" ASC"#
            );
        }
    }
}
