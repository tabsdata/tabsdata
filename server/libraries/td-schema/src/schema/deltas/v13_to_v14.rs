//
// Copyright 2026 Tabsdata Inc.
//

//! v13 → v14: `metrics_facts.id`, minted for every existing fact at its `ts`.

use sea_query::{Expr, Iden, Query, Table};

use crate::schema::render::{render_query, render_schema, update_db_version};
use crate::schema::sql::id_column;
use crate::schema::sql::mint::{DROP_MINT_ID, MINT_ID};
use crate::schema::version::SchemaDelta;

/// Frozen at the shape it had at v14.
#[derive(Iden, Clone, Copy)]
enum MetricsFacts {
    Table,
    Id,
}

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 13;
    const TO_VERSION: usize = 14;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        vec![
            MINT_ID.to_string(),
            render_schema(Table::alter().table(MetricsFacts::Table).add_column(id_column(MetricsFacts::Id).null()).to_owned()),
            render_query(Query::update().table(MetricsFacts::Table).value(MetricsFacts::Id, Expr::cust("td_mint_id(ts)")).to_owned()),
            r#"ALTER TABLE "metrics_facts" ADD PRIMARY KEY ("id")"#.to_string(),
            DROP_MINT_ID.to_string(),
            update_db_version(Self::TO_VERSION),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::Migration;
    use crate::schema::tests::validate_delta;

    #[test]
    fn test_validate_delta_v13_to_v14() {
        validate_delta::<Migration>();
    }
}
