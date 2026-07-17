//
// Copyright 2026 Tabs Data Inc.
//

//! DataVersion-family load cases. `VersionedDataVersion` has no production
//! DAO, so this family uses a purpose-built `LoadDvDao` over a dedicated
//! table, sized on versions-per-partition (what `Versions` selectors rank
//! over).

use std::time::Instant;

use sea_query::{ColumnDef, Iden, Query, Table};
use ta_objects::{Buildable, Dao};
use td_database::components::{SchemaComponent, SchemaSource};
use td_database::pool::TdPool;
use td_error::TdError;
use td_objects::types::basic::{AtTime, LogicalTableDataVersionId};
use td_objects::types::table_ref::{Version, Versions};
use td_queries::{LiveEntry, VersionedEntry};
use td_schema::schema::backend::Backend;

use crate::bulk::bulk_insert;
use crate::harness::{timed, timed_once};
use crate::profile::SizeProfile;
use crate::scaling::assert_subquadratic;

#[ta_objects::typed(string)]
#[derive(Eq, PartialEq)]
struct DvPartition;

#[ta_objects::typed_enum]
#[derive(Eq, PartialEq)]
enum DvStatus {
    #[typed_enum(rename = "A")]
    Active,
}

#[Dao]
#[derive(Eq, PartialEq)]
#[dao(
    data_version,
    sql_table = "load_dv_table",
    versioned(pk = "id", version_by = "defined_on", partition_by = "partition_id"),
    states(Active = DvStatus::Active)
)]
pub(crate) struct LoadDvDao {
    id: LogicalTableDataVersionId,
    partition_id: DvPartition,
    status: DvStatus,
    defined_on: AtTime,
}

/// Keyed projection of the same `load_dv_table`, for the keyed write paths.
#[Dao]
#[derive(Eq, PartialEq)]
#[dao(sql_table = "load_dv_table", key(id))]
pub(crate) struct LoadDvKeyedDao {
    id: LogicalTableDataVersionId,
    partition_id: DvPartition,
    status: DvStatus,
    defined_on: AtTime,
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
fn dv_create_table_sql(b: &Backend) -> String {
    b.build_schema(
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
    fn build(&self, b: Backend) -> Vec<String> {
        let (partitions, versions) = dims();
        let mut stmts = Vec::with_capacity(partitions * versions + 1);
        stmts.push(dv_create_table_sql(&b));
        let mut seq = 1u64;
        for p in 0..partitions {
            for v in 0..versions {
                let id = format!("LDV20260101.{seq:010}");
                let defined_on = dv_defined_on(v);
                let partition = format!("p{p}");
                stmts.push(
                    b.build_query(
                        Query::insert()
                            .into_table(LoadDvTable::Table)
                            .columns([
                                LoadDvTable::Id,
                                LoadDvTable::PartitionId,
                                LoadDvTable::Status,
                                LoadDvTable::DefinedOn,
                            ])
                            .values_panic([
                                id.into(),
                                partition.into(),
                                "A".into(),
                                defined_on.into(),
                            ])
                            .to_owned(),
                    ),
                );
                seq += 1;
            }
        }
        stmts
    }
}

impl SchemaComponent for LoadDvFixture {
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
    fn build(&self, b: Backend) -> Vec<String> {
        vec![dv_create_table_sql(&b)]
    }
}

impl SchemaComponent for LoadDvTableOnly {
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
    AtTime::try_from(
        "2026-01-02T00:00:00.000+00:00"
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap(),
    )
    .unwrap()
}

fn p0() -> DvPartition {
    DvPartition::try_from("p0").unwrap()
}

/// Fills `seeded` with the versions-per-partition axis from [`dims`].
fn record(case: &'static str, db: &TdPool, t: Instant, rows: usize) {
    let (_p, versions) = dims();
    crate::harness::record(case, db, SizeProfile::from_env(), t, rows, versions);
}

#[td_test::test(db(crate::dataversion_cases::LoadDvFixture))]
async fn load_data_version_fetch_one(db: TdPool) -> Result<(), TdError> {
    let on = after_all();
    let p = p0();
    let t = Instant::now();
    let _row: LoadDvDao = (&db)
        .versioned(&on)
        .data_version(&p)
        .fetch_one::<LoadDvDao>(&Versions::Single(Version::Head(0)))
        .await?;
    record("data_version::fetch_one", &db, t, 1);
    Ok(())
}

#[td_test::test(db(crate::dataversion_cases::LoadDvFixture))]
async fn load_data_version_fetch_optional(db: TdPool) -> Result<(), TdError> {
    let on = after_all();
    let p = p0();
    let t = Instant::now();
    let row = (&db)
        .versioned(&on)
        .data_version(&p)
        .fetch_optional::<LoadDvDao>(&Versions::None)
        .await?;
    record(
        "data_version::fetch_optional",
        &db,
        t,
        row.is_some() as usize,
    );
    assert!(row.is_some());
    Ok(())
}

#[td_test::test(db(crate::dataversion_cases::LoadDvFixture))]
async fn load_data_version_fetch_all(db: TdPool) -> Result<(), TdError> {
    let on = after_all();
    let p = p0();
    let t = Instant::now();
    let rows = (&db)
        .versioned(&on)
        .data_version(&p)
        .fetch_all::<LoadDvDao>(&Versions::Range(Version::Head(-2), Version::Head(0)))
        .await?;
    record("data_version::fetch_all", &db, t, rows.len());
    assert!(!rows.is_empty());
    Ok(())
}

#[td_test::test(db(crate::dataversion_cases::LoadDvFixture))]
async fn load_data_version_fetch_relative_offset(db: TdPool) -> Result<(), TdError> {
    let on = after_all();
    let p = p0();
    let t = Instant::now();
    let offset = (&db)
        .versioned(&on)
        .data_version(&p)
        .fetch_relative_offset::<LoadDvDao>(&Version::Head(0))
        .await?;
    record(
        "data_version::fetch_relative_offset",
        &db,
        t,
        offset.is_some() as usize,
    );
    Ok(())
}

/// Keyed to the first seeded id, so `upsert` replaces it and
/// `insert_or_ignore` hits the conflict path.
fn keyed_row() -> Result<LoadDvKeyedDao, TdError> {
    let defined_on = AtTime::try_from(
        dv_defined_on(0)
            .parse::<chrono::DateTime<chrono::Utc>>()
            .unwrap(),
    )?;
    Ok(LoadDvKeyedDao::builder()
        .id(LogicalTableDataVersionId::try_from(
            "LDV20260101.0000000001",
        )?)
        .partition_id(p0())
        .status(DvStatus::Active)
        .defined_on(defined_on)
        .build()?)
}

#[td_test::test(db(crate::dataversion_cases::LoadDvFixture))]
async fn load_data_version_upsert(db: TdPool) -> Result<(), TdError> {
    let row = keyed_row()?;
    let t = Instant::now();
    db.live().upsert(&row).await?;
    record("live::upsert", &db, t, 1);
    Ok(())
}

#[td_test::test(db(crate::dataversion_cases::LoadDvFixture))]
async fn load_data_version_insert_or_ignore(db: TdPool) -> Result<(), TdError> {
    let row = keyed_row()?;
    let t = Instant::now();
    db.live().insert_or_ignore(&row).await?;
    record("live::insert_or_ignore", &db, t, 1);
    Ok(())
}

/// `count` Active versions for `p0`, version indices `start..start+count`.
pub(crate) fn dv_rows(start: usize, count: usize) -> Result<Vec<LoadDvDao>, TdError> {
    (start..start + count)
        .map(|v| {
            let id =
                LogicalTableDataVersionId::try_from(format!("LDV20260101.{:010}", v + 1).as_str())?;
            let defined_on = AtTime::try_from(
                dv_defined_on(v)
                    .parse::<chrono::DateTime<chrono::Utc>>()
                    .unwrap(),
            )?;
            Ok(LoadDvDao::builder()
                .id(id)
                .partition_id(p0())
                .status(DvStatus::Active)
                .defined_on(defined_on)
                .build()?)
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
        (db).versioned(&on)
            .data_version(&p)
            .fetch_one::<LoadDvDao>(&Versions::Single(Version::Head(0)))
            .await?
    );
    let _ = timed!(
        out,
        "data_version::fetch_optional",
        (db).versioned(&on)
            .data_version(&p)
            .fetch_optional::<LoadDvDao>(&Versions::None)
            .await?
    );
    let _ = timed!(
        out,
        "data_version::fetch_all",
        (db).versioned(&on)
            .data_version(&p)
            .fetch_all::<LoadDvDao>(&Versions::Range(Version::Head(-2), Version::Head(0)))
            .await?
    );
    let _ = timed!(
        out,
        "data_version::fetch_relative_offset",
        (db).versioned(&on)
            .data_version(&p)
            .fetch_relative_offset::<LoadDvDao>(&Version::Head(0))
            .await?
    );
    // `upsert` replaces the first seeded id; `insert_or_ignore` hits its
    // conflict path.
    let row = keyed_row()?;
    timed_once!(out, "live::upsert", (db).live().upsert(&row).await?);
    timed_once!(
        out,
        "live::insert_or_ignore",
        (db).live().insert_or_ignore(&row).await?
    );
    Ok(out)
}

#[ignore = "expensive: seeds load_dv_table at two sizes and times every data_version query"]
#[td_test::test(db(crate::dataversion_cases::LoadDvTableOnly))]
async fn report_scaling_data_version(db: TdPool) -> Result<(), TdError> {
    const SMALL_V: usize = 500;
    const BIG_V: usize = 2_000;

    bulk_insert(&db, &dv_rows(0, SMALL_V)?).await?;
    db.execute_batch(&["ANALYZE".to_string()])
        .await
        .expect("analyze after small dv seed");
    let t1 = time_dv_queries(&db).await?;
    bulk_insert(&db, &dv_rows(SMALL_V, BIG_V)?).await?;
    db.execute_batch(&["ANALYZE".to_string()])
        .await
        .expect("analyze after big dv seed");
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
