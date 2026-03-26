//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{ColumnDef, Iden, Query, Table};
use td_schema::schema::Fixture;
use td_schema::schema::backend::Backend;

#[derive(Iden)]
enum TestDvTable {
    Table,
    Id,
    PartitionId,
    Status,
    DefinedOn,
}

pub struct DataVersion;

impl Fixture for DataVersion {
    fn build(b: Backend) -> Vec<String> {
        let mut stmts = Vec::new();

        // CREATE TABLE test_dv_table
        stmts.push(
            b.build_schema(
                Table::create()
                    .table(TestDvTable::Table)
                    .col(
                        ColumnDef::new(TestDvTable::Id)
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(TestDvTable::PartitionId).text().not_null())
                    .col(ColumnDef::new(TestDvTable::Status).text().not_null())
                    .col(
                        ColumnDef::new(TestDvTable::DefinedOn)
                            .timestamp()
                            .not_null(),
                    )
                    .to_owned(),
            ),
        );

        // Partition p0: 3 active versions
        // After ranking (ORDER BY defined_on DESC):
        //   DV_3 (t3) -> rn=1 (HEAD)
        //   DV_2 (t2) -> rn=2
        //   DV_1 (t1) -> rn=3 (INITIAL), total_count=3
        for (id, partition, status, time) in [
            (
                "DV20260101.0000000001",
                "p0",
                "A",
                "2026-01-01T00:00:01.000+00:00",
            ),
            (
                "DV20260101.0000000002",
                "p0",
                "A",
                "2026-01-01T00:00:02.000+00:00",
            ),
            (
                "DV20260101.0000000003",
                "p0",
                "A",
                "2026-01-01T00:00:03.000+00:00",
            ),
            // Partition p1: 1 active + 1 deleted
            // After ranking:
            //   DV_5 (t2, D) -> rn=1 (HEAD, deleted)
            //   DV_4 (t1, A) -> rn=2 (INITIAL), total_count=2
            (
                "DV20260101.0000000004",
                "p1",
                "A",
                "2026-01-01T00:00:01.000+00:00",
            ),
            (
                "DV20260101.0000000005",
                "p1",
                "D",
                "2026-01-01T00:00:02.000+00:00",
            ),
        ] {
            stmts.push(
                b.build_query(
                    Query::insert()
                        .into_table(TestDvTable::Table)
                        .columns([
                            TestDvTable::Id,
                            TestDvTable::PartitionId,
                            TestDvTable::Status,
                            TestDvTable::DefinedOn,
                        ])
                        .values_panic([id.into(), partition.into(), status.into(), time.into()])
                        .to_owned(),
                ),
            );
        }

        stmts
    }
}
