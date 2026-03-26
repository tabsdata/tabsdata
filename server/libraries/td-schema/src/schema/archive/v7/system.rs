//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{ColumnDef, InsertStatement, Query, Table, TableCreateStatement};

use super::Schema;
use crate::TabsdataSystem;
use crate::schema::SchemaVersion;
use crate::schema::backend::{Backend, ViewDef};

pub fn tables() -> Vec<TableCreateStatement> {
    vec![
        Table::create()
            .table(TabsdataSystem::Table)
            .col(
                ColumnDef::new(TabsdataSystem::Name)
                    .text()
                    .not_null()
                    .primary_key(),
            )
            .col(ColumnDef::new(TabsdataSystem::Value).text().null())
            .to_owned(),
    ]
}

pub fn views(_b: &Backend) -> Vec<ViewDef> {
    vec![]
}

pub fn seed() -> Vec<InsertStatement> {
    vec![
        // Initial db_version set to the current version for fresh installs.
        Query::insert()
            .into_table(TabsdataSystem::Table)
            .columns([TabsdataSystem::Name, TabsdataSystem::Value])
            .values_panic(["db_version".into(), Schema::VERSION.to_string().into()])
            .to_owned(),
    ]
}
