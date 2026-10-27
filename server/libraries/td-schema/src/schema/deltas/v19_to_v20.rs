//
// Copyright 2026 Tabsdata Inc.
//

//! v19 → v20: the `instance_id` row of `tabsdata_system`.

use sea_query::{Expr, OnConflict, Query};

use crate::schema::render::{render_query, update_db_version};
use crate::schema::version::SchemaDelta;
use crate::{INSTANCE_ID_NAME, TabsdataSystem};

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 19;
    const TO_VERSION: usize = 20;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        vec![
            render_query(
                Query::insert()
                    .into_table(TabsdataSystem::Table)
                    .columns([TabsdataSystem::Name, TabsdataSystem::Value])
                    .values_panic([INSTANCE_ID_NAME.into(), Expr::cust("gen_random_uuid()::text")])
                    .on_conflict(OnConflict::column(TabsdataSystem::Name).do_nothing().to_owned())
                    .to_owned(),
            ),
            update_db_version(Self::TO_VERSION),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v19_to_v20() {
        validate_delta::<Migration>();
    }
}
