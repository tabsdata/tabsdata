//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{ColumnDef, InsertStatement, Query, Table, TableCreateStatement};

use super::Schema;
use crate::schema::{Entity, SchemaVersion};
use crate::{CURRENT_TABSDATA_VERSION, TABSDATA_VERSION_NAME, TabsdataSystem};

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(TabsdataSystem::Table)
                .col(ColumnDef::new(TabsdataSystem::Name).text().not_null().primary_key())
                .col(ColumnDef::new(TabsdataSystem::Value).text().null())
                .to_owned(),
        ]
    }

    fn seeds(&self) -> Vec<InsertStatement> {
        vec![
            // Major-version marker. Gate-checked at startup before db_version so
            // 1.x databases (which lack this row) are refused loudly instead of
            // misinterpreted under the reset db_version counter.
            Query::insert()
                .into_table(TabsdataSystem::Table)
                .columns([TabsdataSystem::Name, TabsdataSystem::Value])
                .values_panic([TABSDATA_VERSION_NAME.into(), CURRENT_TABSDATA_VERSION.to_string().into()])
                .to_owned(),
            // Schema counter within the current major. Resets per major bump.
            Query::insert()
                .into_table(TabsdataSystem::Table)
                .columns([TabsdataSystem::Name, TabsdataSystem::Value])
                .values_panic(["db_version".into(), Schema::VERSION.to_string().into()])
                .to_owned(),
        ]
    }
}
