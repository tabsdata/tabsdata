//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, Iden, Query, Table};
use td_database::components::{SchemaComponent, SchemaSource};
use td_database::pool::PoolKind;
use td_objects::types::basic::LogicalTableDataVersionId;
use td_schema::schema::render::{render_query, render_schema};

#[derive(Iden)]
enum TestDvTable {
    Table,
    Id,
    PartitionId,
    Status,
    DefinedOn,
}

/// The at-rest `i64` of a logical ID, so the seeds are keyed by the same values
/// the tests' `LogicalTableDataVersionId` fixtures resolve to.
fn dv_id(id: &str) -> i64 {
    LogicalTableDataVersionId::try_from(id).unwrap().as_i64()
}

/// Test-only component: a self-contained `data_version` timeline on its own
/// schema, requested via `#[td_test::test(db(crate::fixtures::DataVersion))]`.
pub struct DataVersion;

struct DataVersionSource;

impl SchemaSource for DataVersionSource {
    fn current_version(&self) -> usize {
        1
    }
    fn tables(&self) -> Vec<String> {
        vec![render_schema(
            Table::create()
                .table(TestDvTable::Table)
                .col(
                    ColumnDef::new(TestDvTable::Id)
                        .big_integer()
                        .not_null()
                        .primary_key(),
                )
                .col(ColumnDef::new(TestDvTable::PartitionId).text().not_null())
                .col(ColumnDef::new(TestDvTable::Status).text().not_null())
                .col(
                    ColumnDef::new(TestDvTable::DefinedOn)
                        .timestamp_with_time_zone()
                        .not_null(),
                )
                .to_owned(),
        )]
    }
    fn seeds(&self) -> Vec<String> {
        let mut stmts = Vec::new();

        // Partition p0: 3 active versions
        // After ranking (ORDER BY defined_on DESC):
        //   DV_3 (t3) -> rn=1 (HEAD)
        //   DV_2 (t2) -> rn=2
        //   DV_1 (t1) -> rn=3 (INITIAL), total_count=3
        for (id, partition, status, time) in [
            (
                "dv20260101-0000000001",
                "p0",
                "A",
                "2026-01-01T00:00:01.000+00:00",
            ),
            (
                "dv20260101-0000000002",
                "p0",
                "A",
                "2026-01-01T00:00:02.000+00:00",
            ),
            (
                "dv20260101-0000000003",
                "p0",
                "A",
                "2026-01-01T00:00:03.000+00:00",
            ),
            // Partition p1: 1 active + 1 deleted
            // After ranking:
            //   DV_5 (t2, D) -> rn=1 (HEAD, deleted)
            //   DV_4 (t1, A) -> rn=2 (INITIAL), total_count=2
            (
                "dv20260101-0000000004",
                "p1",
                "A",
                "2026-01-01T00:00:01.000+00:00",
            ),
            (
                "dv20260101-0000000005",
                "p1",
                "D",
                "2026-01-01T00:00:02.000+00:00",
            ),
        ] {
            stmts.push(render_query(
                Query::insert()
                    .into_table(TestDvTable::Table)
                    .columns([
                        TestDvTable::Id,
                        TestDvTable::PartitionId,
                        TestDvTable::Status,
                        TestDvTable::DefinedOn,
                    ])
                    .values_panic([
                        dv_id(id).into(),
                        partition.into(),
                        status.into(),
                        time.into(),
                    ])
                    .to_owned(),
            ));
        }

        stmts
    }
}

impl SchemaComponent for DataVersion {
    fn pools(&self) -> &'static [PoolKind] {
        &[PoolKind::Regular]
    }

    fn key(&self) -> &'static str {
        "test_data_version"
    }
    fn default_schema(&self) -> &'static str {
        "td_test_data_version"
    }
    fn schema_source(&self) -> &'static dyn SchemaSource {
        &DataVersionSource
    }
}
