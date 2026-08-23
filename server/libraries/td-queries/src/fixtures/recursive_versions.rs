//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, Iden, Query, Table};
use td_database::components::{SchemaComponent, SchemaSource};
use td_schema::schema::backend::Backend;

#[derive(Iden)]
enum TestTable {
    Table,
    Id,
    Status,
    Current,
    Downstream,
    DefinedOn,
}

#[derive(Iden)]
enum TestTableReference {
    Table,
    Id,
    ReferenceId,
    Status,
    DefinedOn,
}

/// Test-only component: a self-contained versioned reference graph on its own
/// schema, requested via `#[td_test::test(db(crate::fixtures::RecursiveVersions))]`.
pub struct RecursiveVersions;

struct RecursiveVersionsSource;

impl SchemaSource for RecursiveVersionsSource {
    fn current_version(&self) -> usize {
        1
    }
    fn tables(&self, b: Backend) -> Vec<String> {
        vec![
            b.build_schema(
                Table::create()
                    .table(TestTable::Table)
                    .col(
                        ColumnDef::new(TestTable::Id)
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(TestTable::Status).text().not_null())
                    .col(ColumnDef::new(TestTable::Current).text().not_null())
                    .col(ColumnDef::new(TestTable::Downstream).text().not_null())
                    .col(
                        ColumnDef::new(TestTable::DefinedOn)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .to_owned(),
            ),
        ]
    }
    fn seeds(&self, b: Backend) -> Vec<String> {
        let mut stmts = Vec::new();

        // 5 test_table rows (AAA-EEE)
        for (id, status, current, downstream, defined_on) in [
            (
                "AAA",
                "A",
                "ref_0",
                "ref_1",
                "2025-04-02T08:19:50.543+00:00",
            ),
            (
                "BBB",
                "A",
                "ref_1",
                "ref_2",
                "2025-04-02T08:19:50.543+00:00",
            ),
            (
                "CCC",
                "A",
                "ref_1",
                "ref_3",
                "2025-04-02T08:19:51.543+00:00",
            ),
            (
                "DDD",
                "A",
                "ref_1",
                "ref_4",
                "2025-04-02T08:19:52.543+00:00",
            ),
            (
                "EEE",
                "A",
                "ref_4",
                "ref_5",
                "2025-04-02T08:19:53.543+00:00",
            ),
        ] {
            stmts.push(
                b.build_query(
                    Query::insert()
                        .into_table(TestTable::Table)
                        .columns([
                            TestTable::Id,
                            TestTable::Status,
                            TestTable::Current,
                            TestTable::Downstream,
                            TestTable::DefinedOn,
                        ])
                        .values_panic([
                            id.into(),
                            status.into(),
                            current.into(),
                            downstream.into(),
                            defined_on.into(),
                        ])
                        .to_owned(),
                ),
            );
        }

        // CREATE TABLE test_table_reference
        stmts.push(
            b.build_schema(
                Table::create()
                    .table(TestTableReference::Table)
                    .col(
                        ColumnDef::new(TestTableReference::Id)
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(TestTableReference::ReferenceId)
                            .text()
                            .not_null(),
                    )
                    .col(ColumnDef::new(TestTableReference::Status).text().not_null())
                    .col(
                        ColumnDef::new(TestTableReference::DefinedOn)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .to_owned(),
            ),
        );

        // 7 test_table_reference rows (MMM-SSS)
        for (id, ref_id, status, defined_on) in [
            ("MMM", "ref_0", "A", "2025-04-02T08:19:50.543+00:00"),
            ("NNN", "ref_1", "A", "2025-04-02T08:19:50.543+00:00"),
            ("OOO", "ref_2", "A", "2025-04-02T08:19:50.543+00:00"),
            ("QQQ", "ref_3", "A", "2025-04-02T08:19:51.543+00:00"),
            ("PPP", "ref_2", "D", "2025-04-02T08:19:52.543+00:00"),
            ("RRR", "ref_4", "A", "2025-04-02T08:19:53.543+00:00"),
            ("SSS", "ref_5", "A", "2025-04-02T08:19:53.543+00:00"),
        ] {
            stmts.push(
                b.build_query(
                    Query::insert()
                        .into_table(TestTableReference::Table)
                        .columns([
                            TestTableReference::Id,
                            TestTableReference::ReferenceId,
                            TestTableReference::Status,
                            TestTableReference::DefinedOn,
                        ])
                        .values_panic([id.into(), ref_id.into(), status.into(), defined_on.into()])
                        .to_owned(),
                ),
            );
        }

        stmts
    }
}

impl SchemaComponent for RecursiveVersions {
    fn key(&self) -> &'static str {
        "test_recursive_versions"
    }
    fn default_schema(&self) -> &'static str {
        "td_test_recursive_versions"
    }
    fn schema_source(&self) -> &'static dyn SchemaSource {
        &RecursiveVersionsSource
    }
}
