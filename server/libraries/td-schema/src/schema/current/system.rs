//
// Copyright 2026 Tabs Data Inc.
//

use sea_query::{InsertStatement, Query};

use crate::schema::Entity;
use crate::{CURRENT_TABSDATA_VERSION, TABSDATA_VERSION_NAME, TabsdataSystem};

pub(super) struct Defs;

impl Entity for Defs {
    // The `tabsdata_system` table itself is created by the pool's shared
    // create/upgrade path (`td_schema::tabsdata_system_ddl`) before this build
    // runs, for every schema — not declared here. Likewise `db_version` is
    // written by the pool. This entity only seeds the apiserver-specific
    // major-version gate row.
    fn seeds(&self) -> Vec<InsertStatement> {
        vec![
            // Major-version marker. Gate-checked at startup before db_version so
            // 1.x databases (which lack this row) are refused loudly instead of
            // misinterpreted under the reset db_version counter. Seeded on fresh
            // build only (never on upgrade), so the gate stays meaningful.
            Query::insert()
                .into_table(TabsdataSystem::Table)
                .columns([TabsdataSystem::Name, TabsdataSystem::Value])
                .values_panic([TABSDATA_VERSION_NAME.into(), CURRENT_TABSDATA_VERSION.to_string().into()])
                .to_owned(),
        ]
    }
}
