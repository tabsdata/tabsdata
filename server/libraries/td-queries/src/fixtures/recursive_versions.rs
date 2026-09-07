//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, Iden, IntoIden, Query, Table};
use td_database::components::{SchemaComponent, SchemaSource};
use td_database::pool::PoolKind;
use td_schema::schema::Derived;
use td_schema::schema::render::{render_query, render_schema};
use td_schema::schema::sql::trigger::Scd2TriggerDef;

#[derive(Iden)]
enum TestTable {
    Table,
    Id,
    Status,
    Current,
    Downstream,
    ValidFrom,
    ValidTo,
}

#[derive(Iden)]
enum TestTableReference {
    Table,
    Id,
    ReferenceId,
    Status,
    ValidFrom,
    ValidTo,
}

/// Test-only component: a self-contained versioned reference graph on its own
/// schema, requested via `#[td_test::test(db(crate::fixtures::RecursiveVersions))]`.
pub struct RecursiveVersions;

struct RecursiveVersionsSource;

impl SchemaSource for RecursiveVersionsSource {
    fn current_version(&self) -> usize {
        1
    }
    fn tables(&self) -> Vec<String> {
        vec![
            render_schema(
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
                        ColumnDef::new(TestTable::ValidFrom)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TestTable::ValidTo)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .to_owned(),
            ),
            render_schema(
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
                        ColumnDef::new(TestTableReference::ValidFrom)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(
                        ColumnDef::new(TestTableReference::ValidTo)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .to_owned(),
            ),
        ]
    }
    fn triggers(&self) -> Vec<Derived> {
        [
            Scd2TriggerDef {
                name: "test_table__scd2_close".to_string(),
                source_table: TestTable::Table.into_iden(),
                partition_col: TestTable::Id.into_iden(),
                valid_from_col: TestTable::ValidFrom.into_iden(),
                valid_to_col: TestTable::ValidTo.into_iden(),
                pk_col: TestTable::Id.into_iden(),
            },
            Scd2TriggerDef {
                name: "test_table_reference__scd2_close".to_string(),
                source_table: TestTableReference::Table.into_iden(),
                partition_col: TestTableReference::ReferenceId.into_iden(),
                valid_from_col: TestTableReference::ValidFrom.into_iden(),
                valid_to_col: TestTableReference::ValidTo.into_iden(),
                pk_col: TestTableReference::Id.into_iden(),
            },
        ]
        .into_iter()
        .map(|t| Derived {
            create: t.render(),
            drop: vec![],
        })
        .collect()
    }
    fn seeds(&self) -> Vec<String> {
        let mut stmts = Vec::new();

        for (id, status, current, downstream, valid_from) in [
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
            stmts.push(render_query(
                Query::insert()
                    .into_table(TestTable::Table)
                    .columns([
                        TestTable::Id,
                        TestTable::Status,
                        TestTable::Current,
                        TestTable::Downstream,
                        TestTable::ValidFrom,
                    ])
                    .values_panic([
                        id.into(),
                        status.into(),
                        current.into(),
                        downstream.into(),
                        valid_from.into(),
                    ])
                    .to_owned(),
            ));
        }

        for (id, ref_id, status, valid_from) in [
            ("MMM", "ref_0", "A", "2025-04-02T08:19:50.543+00:00"),
            ("NNN", "ref_1", "A", "2025-04-02T08:19:50.543+00:00"),
            ("OOO", "ref_2", "A", "2025-04-02T08:19:50.543+00:00"),
            ("QQQ", "ref_3", "A", "2025-04-02T08:19:51.543+00:00"),
            ("PPP", "ref_2", "D", "2025-04-02T08:19:52.543+00:00"),
            ("RRR", "ref_4", "A", "2025-04-02T08:19:53.543+00:00"),
            ("SSS", "ref_5", "A", "2025-04-02T08:19:53.543+00:00"),
        ] {
            stmts.push(render_query(
                Query::insert()
                    .into_table(TestTableReference::Table)
                    .columns([
                        TestTableReference::Id,
                        TestTableReference::ReferenceId,
                        TestTableReference::Status,
                        TestTableReference::ValidFrom,
                    ])
                    .values_panic([id.into(), ref_id.into(), status.into(), valid_from.into()])
                    .to_owned(),
            ));
        }

        stmts
    }
}

impl SchemaComponent for RecursiveVersions {
    fn pools(&self) -> &'static [PoolKind] {
        &[PoolKind::Regular]
    }

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
