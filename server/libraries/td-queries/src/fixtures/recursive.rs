//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{ColumnDef, Iden, Query, Table};
use td_schema::schema::Fixture;
use td_schema::schema::backend::Backend;

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

pub struct Recursive;

impl Fixture for Recursive {
    fn build(b: Backend) -> Vec<String> {
        let mut stmts = Vec::new();

        // CREATE TABLE test_basic_reference
        stmts.push(
            b.build_schema(
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
        );

        // CREATE TABLE test_basic_table
        stmts.push(
            b.build_schema(
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
        );

        // Chain: ref_0 -> ref_1 -> ref_2
        for (id, ref_id) in [("R0", "ref_0"), ("R1", "ref_1"), ("R2", "ref_2")] {
            stmts.push(
                b.build_query(
                    Query::insert()
                        .into_table(TestBasicReference::Table)
                        .columns([TestBasicReference::Id, TestBasicReference::ReferenceId])
                        .values_panic([id.into(), ref_id.into()])
                        .to_owned(),
                ),
            );
        }

        for (id, current, downstream) in [("N0", "ref_0", "ref_1"), ("N1", "ref_1", "ref_2")] {
            stmts.push(
                b.build_query(
                    Query::insert()
                        .into_table(TestBasicTable::Table)
                        .columns([
                            TestBasicTable::Id,
                            TestBasicTable::Current,
                            TestBasicTable::Downstream,
                        ])
                        .values_panic([id.into(), current.into(), downstream.into()])
                        .to_owned(),
                ),
            );
        }

        stmts
    }
}
