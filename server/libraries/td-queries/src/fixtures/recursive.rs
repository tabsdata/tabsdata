//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, Iden, Query, Table};
use td_database::components::{SchemaComponent, SchemaSource};
use td_database::pool::PoolKind;
use td_schema::schema::render::{render_query, render_schema};

#[derive(Iden)]
enum TestBasicReference {
    Table,
    Id,
    ReferenceId,
}

#[derive(Iden)]
enum TestBasicTable {
    Table,
    Id,
    Current,
    Downstream,
}

/// Test-only component: a self-contained reference graph on its own schema,
/// requested via `#[td_test::test(db(crate::fixtures::Recursive))]`.
pub struct Recursive;

struct RecursiveSource;

impl SchemaSource for RecursiveSource {
    fn current_version(&self) -> usize {
        1
    }
    fn tables(&self) -> Vec<String> {
        vec![
            render_schema(
                Table::create()
                    .table(TestBasicReference::Table)
                    .col(
                        ColumnDef::new(TestBasicReference::Id)
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(
                        ColumnDef::new(TestBasicReference::ReferenceId)
                            .text()
                            .not_null(),
                    )
                    .to_owned(),
            ),
            render_schema(
                Table::create()
                    .table(TestBasicTable::Table)
                    .col(
                        ColumnDef::new(TestBasicTable::Id)
                            .text()
                            .not_null()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(TestBasicTable::Current).text().not_null())
                    .col(ColumnDef::new(TestBasicTable::Downstream).text().not_null())
                    .to_owned(),
            ),
        ]
    }
    fn seeds(&self) -> Vec<String> {
        let mut stmts = Vec::new();

        // Chain: ref_0 -> ref_1 -> ref_2
        for (id, ref_id) in [("R0", "ref_0"), ("R1", "ref_1"), ("R2", "ref_2")] {
            stmts.push(render_query(
                Query::insert()
                    .into_table(TestBasicReference::Table)
                    .columns([TestBasicReference::Id, TestBasicReference::ReferenceId])
                    .values_panic([id.into(), ref_id.into()])
                    .to_owned(),
            ));
        }

        for (id, current, downstream) in [("N0", "ref_0", "ref_1"), ("N1", "ref_1", "ref_2")] {
            stmts.push(render_query(
                Query::insert()
                    .into_table(TestBasicTable::Table)
                    .columns([
                        TestBasicTable::Id,
                        TestBasicTable::Current,
                        TestBasicTable::Downstream,
                    ])
                    .values_panic([id.into(), current.into(), downstream.into()])
                    .to_owned(),
            ));
        }

        stmts
    }
}

impl SchemaComponent for Recursive {
    fn pools(&self) -> &'static [PoolKind] {
        &[PoolKind::Regular]
    }

    fn key(&self) -> &'static str {
        "test_recursive"
    }
    fn default_schema(&self) -> &'static str {
        "td_test_recursive"
    }
    fn schema_source(&self) -> &'static dyn SchemaSource {
        &RecursiveSource
    }
}
