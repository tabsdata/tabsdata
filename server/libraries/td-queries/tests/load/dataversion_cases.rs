//
// Copyright 2026 Tabsdata Inc.
//

//! DataVersion-family load cases. `DataVersion` has no production
//! DAO, so this family uses a purpose-built `LoadDvDao` over a dedicated
//! table, sized on versions-per-partition (what `Versions` selectors
//! resolve positions over).

use crate::bulk::bulk_insert;
use crate::harness::{analyze, timed, timed_once};
use crate::profile::SizeProfile;
use crate::scaling::assert_subquadratic;
use sea_query::{ColumnDef, Iden, Query, Table};
use std::time::Instant;
use ta_objects::Dao;
use ta_objects::{Typed, TypedEnum, TypedOpenApi, TypedSerde, TypedSqlx};
use td_database::components::{SchemaComponent, SchemaSource};
use td_database::pool::PoolKind;
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::types::basic::{AtTime, LogicalTableDataVersionId};
use td_objects::types::table_ref::{Version, Versions};
use td_queries::Queries;
use td_schema::schema::render::{render_query, render_schema};

#[derive(Typed, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub(crate) struct DvPartition(String);

#[derive(TypedEnum, TypedSerde, TypedSqlx, TypedOpenApi, Eq, PartialEq)]
pub(crate) enum DvStatus {
    #[typed_enum(rename = "A")]
    Active,
}

#[Dao]
#[derive(Eq, PartialEq)]
#[dao(table = "load_dv_table", ops(select, insert), timeline)]
pub(crate) struct LoadDvDao {
    #[dao(pk)]
    pub id: LogicalTableDataVersionId,
    #[dao(partition_by)]
    pub partition_id: DvPartition,
    pub status: DvStatus,
    #[dao(timeline(version_by))]
    pub defined_on: AtTime,
}

/// Keyed projection of the same `load_dv_table`, for the keyed write paths.
#[Dao]
#[derive(Eq, PartialEq)]
#[dao(
    table = "load_dv_table",
    ops(select, insert, delete, upsert(LogicalTableDataVersionId))
)]
pub(crate) struct LoadDvKeyedDao {
    pub id: LogicalTableDataVersionId,
    pub partition_id: DvPartition,
    pub status: DvStatus,
    pub defined_on: AtTime,
}

/// `partitions` × `versions_per_partition` Active rows, sized by
/// `TD_QUERY_LOAD_PROFILE`.
pub struct LoadDvFixture;

#[derive(Iden)]
enum LoadDvTable {
    Table,
    Id,
    PartitionId,
    Status,
    DefinedOn,
}

fn dims() -> (usize, usize) {
    match SizeProfile::from_env() {
        SizeProfile::Smoke => (2, 50),
        SizeProfile::Load => (50, 1_000),
        SizeProfile::Stress => (200, 5_000),
    }
}

/// Shared by the env-seeded [`LoadDvFixture`] and the empty
/// [`LoadDvTableOnly`].
fn dv_create_table_sql() -> String {
    render_schema(
        Table::create()
            .table(LoadDvTable::Table)
            .col(
                ColumnDef::new(LoadDvTable::Id)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(LoadDvTable::PartitionId).text().not_null())
            .col(ColumnDef::new(LoadDvTable::Status).text().not_null())
            .col(
                ColumnDef::new(LoadDvTable::DefinedOn)
                    .timestamp_with_time_zone()
                    .not_null(),
            )
            .to_owned(),
    )
}

/// `2026-01-01T00:00:00Z + v seconds` — strictly increasing so ranking is
/// well-defined.
fn dv_defined_on(v: usize) -> String {
    format!(
        "2026-01-01T{:02}:{:02}:{:02}.000+00:00",
        v / 3600,
        (v / 60) % 60,
        v % 60
    )
}

struct LoadDvSource;

impl SchemaSource for LoadDvSource {
    fn current_version(&self) -> usize {
        1
    }
    fn tables(&self) -> Vec<String> {
        vec![dv_create_table_sql()]
    }
    fn seeds(&self) -> Vec<String> {
        let (partitions, versions) = dims();
        let mut stmts = Vec::with_capacity(partitions * versions);
        let mut seq = 1u64;
        for p in 0..partitions {
            for v in 0..versions {
                let id = format!("DV20260101.{seq:010}");
                let defined_on = dv_defined_on(v);
                let partition = format!("p{p}");
                stmts.push(render_query(
                    Query::insert()
                        .into_table(LoadDvTable::Table)
                        .columns([
                            LoadDvTable::Id,
                            LoadDvTable::PartitionId,
                            LoadDvTable::Status,
                            LoadDvTable::DefinedOn,
                        ])
                        .values_panic([id.into(), partition.into(), "A".into(), defined_on.into()])
                        .to_owned(),
                ));
                seq += 1;
            }
        }
        stmts
    }
}

impl SchemaComponent for LoadDvFixture {
    fn pools(&self) -> &'static [PoolKind] {
        &[PoolKind::Regular]
    }

    fn key(&self) -> &'static str {
        "load_dv"
    }
    fn default_schema(&self) -> &'static str {
        "td_load_dv"
    }
    fn schema_source(&self) -> &'static dyn SchemaSource {
        &LoadDvSource
    }
}

/// [`LoadDvFixture`] minus the rows, for the scaling tests, which size the
/// data themselves. DDL must go through a component: the pool's app role
/// cannot CREATE TABLE.
pub struct LoadDvTableOnly;

struct LoadDvTableSource;

impl SchemaSource for LoadDvTableSource {
    fn current_version(&self) -> usize {
        1
    }
    fn tables(&self) -> Vec<String> {
        vec![dv_create_table_sql()]
    }
}

impl SchemaComponent for LoadDvTableOnly {
    fn pools(&self) -> &'static [PoolKind] {
        &[PoolKind::Regular]
    }

    fn key(&self) -> &'static str {
        "load_dv"
    }
    fn default_schema(&self) -> &'static str {
        "td_load_dv"
    }
    fn schema_source(&self) -> &'static dyn SchemaSource {
        &LoadDvTableSource
    }
}

/// A time after every fixture row.
fn after_all() -> AtTime {
    AtTime::from(
        "2026-01-02T00:00:00.000+00:00"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap(),
    )
}

fn p0() -> DvPartition {
    DvPartition::try_from("p0").unwrap()
}

/// Fills `seeded` with the versions-per-partition axis from [`dims`].
fn record(case: &'static str, t: Instant, rows: usize) {
    let (_p, versions) = dims();
    crate::harness::record(case, SizeProfile::from_env(), t, rows, versions);
}

#[td_test::test(db(crate::dataversion_cases::LoadDvFixture))]
async fn load_data_version_fetch_one(db: TdPool) -> Result<(), TdError> {
    let on = after_all();
    let p = p0();
    let t = Instant::now();
    let _row: LoadDvDao = db
        .timeline()
        .at(&on, &p)
        .fetch_one::<LoadDvDao>(&Versions::Single(Version::Head(0)))
        .await?;
    record("data_version::fetch_one", t, 1);
    Ok(())
}

#[td_test::test(db(crate::dataversion_cases::LoadDvFixture))]
async fn load_data_version_fetch_optional(db: TdPool) -> Result<(), TdError> {
    let on = after_all();
    let p = p0();
    let t = Instant::now();
    let row = db
        .timeline()
        .at(&on, &p)
        .fetch_optional::<LoadDvDao>(&Versions::None)
        .await?;
    record("data_version::fetch_optional", t, row.is_some() as usize);
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db(crate::dataversion_cases::LoadDvFixture))]
async fn load_data_version_fetch_all(db: TdPool) -> Result<(), TdError> {
    let on = after_all();
    let p = p0();
    let t = Instant::now();
    let rows = db
        .timeline()
        .at(&on, &p)
        .fetch_all::<LoadDvDao>(&Versions::Range(Version::Head(-2), Version::Head(0)))
        .await?;
    record("data_version::fetch_all", t, rows.len());
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db(crate::dataversion_cases::LoadDvFixture))]
async fn load_data_version_fetch_relative_offset(db: TdPool) -> Result<(), TdError> {
    let on = after_all();
    let p = p0();
    let t = Instant::now();
    let offset = db
        .timeline()
        .at(&on, &p)
        .fetch_relative_offset::<LoadDvDao>(&Version::Head(0))
        .await?;
    record(
        "data_version::fetch_relative_offset",
        t,
        offset.is_some() as usize,
    );
    Ok(())
}

/// Keyed to the first seeded id, so `upsert` replaces it and
/// `insert_or_ignore` hits the conflict path.
fn keyed_row() -> Result<LoadDvKeyedDao, TdError> {
    let defined_on = AtTime::from(
        dv_defined_on(0)
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap(),
    );
    Ok(LoadDvKeyedDao {
        id: LogicalTableDataVersionId::try_from("DV20260101.0000000001")?,
        partition_id: p0(),
        status: DvStatus::Active,
        defined_on,
    })
}

#[td_test::test(db(crate::dataversion_cases::LoadDvFixture))]
async fn load_data_version_upsert(db: TdPool) -> Result<(), TdError> {
    let row = keyed_row()?;
    let t = Instant::now();
    db.upsert(&row).await?;
    record("default::upsert", t, 1);
    Ok(())
}

#[td_test::test(db(crate::dataversion_cases::LoadDvFixture))]
async fn load_data_version_insert_or_ignore(db: TdPool) -> Result<(), TdError> {
    let row = keyed_row()?;
    let t = Instant::now();
    db.insert_or_ignore(&row).await?;
    record("default::insert_or_ignore", t, 1);
    Ok(())
}

/// `count` Active versions for `p0`, version indices `start..start+count`.
pub(crate) fn dv_rows(start: usize, count: usize) -> Result<Vec<LoadDvDao>, TdError> {
    (start..start + count)
        .map(|v| {
            let id =
                LogicalTableDataVersionId::try_from(format!("DV20260101.{:010}", v + 1).as_str())?;
            let defined_on = AtTime::from(
                dv_defined_on(v)
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .unwrap(),
            );
            Ok(LoadDvDao {
                id,
                partition_id: p0(),
                status: DvStatus::Active,
                defined_on,
            })
        })
        .collect()
}

pub(crate) async fn time_dv_queries(db: &TdPool) -> Result<Vec<(&'static str, f64)>, TdError> {
    let on = after_all();
    let p = p0();
    let mut out = Vec::new();
    let _ = timed!(
        out,
        "data_version::fetch_one",
        (db).timeline()
            .at(&on, &p)
            .fetch_one::<LoadDvDao>(&Versions::Single(Version::Head(0)))
            .await?
    );
    let _ = timed!(
        out,
        "data_version::fetch_optional",
        (db).timeline()
            .at(&on, &p)
            .fetch_optional::<LoadDvDao>(&Versions::None)
            .await?
    );
    let _ = timed!(
        out,
        "data_version::fetch_all",
        (db).timeline()
            .at(&on, &p)
            .fetch_all::<LoadDvDao>(&Versions::Range(Version::Head(-2), Version::Head(0)))
            .await?
    );
    let _ = timed!(
        out,
        "data_version::fetch_relative_offset",
        (db).timeline()
            .at(&on, &p)
            .fetch_relative_offset::<LoadDvDao>(&Version::Head(0))
            .await?
    );
    // `upsert` replaces the first seeded id; `insert_or_ignore` hits its
    // conflict path.
    let row = keyed_row()?;
    timed_once!(out, "default::upsert", (db).upsert(&row).await?);
    timed_once!(
        out,
        "default::insert_or_ignore",
        (db).insert_or_ignore(&row).await?
    );
    Ok(out)
}

#[ignore = "expensive: seeds load_dv_table at two sizes and times every data_version query"]
#[td_test::test(db(crate::dataversion_cases::LoadDvTableOnly))]
async fn report_scaling_data_version(db: TdPool) -> Result<(), TdError> {
    const SMALL_V: usize = 500;
    const BIG_V: usize = 2_000;

    bulk_insert(&db, &dv_rows(0, SMALL_V)?).await?;
    analyze(&db, "after small dv seed").await;
    let t1 = time_dv_queries(&db).await?;
    bulk_insert(&db, &dv_rows(SMALL_V, BIG_V)?).await?;
    analyze(&db, "after big dv seed").await;
    let t2 = time_dv_queries(&db).await?;

    let n_small = SMALL_V as f64;
    let n_big = (SMALL_V + BIG_V) as f64;
    assert_eq!(t1.len(), t2.len(), "two passes must time the same queries");
    for ((name, a), (name2, b)) in t1.iter().zip(&t2) {
        assert_eq!(name, name2, "the two passes drifted out of order");
        assert_subquadratic(name, n_small, *a, n_big, *b);
    }
    Ok(())
}
