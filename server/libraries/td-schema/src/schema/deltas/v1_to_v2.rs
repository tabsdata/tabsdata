//
// Copyright 2026 Tabsdata Inc.
//

//! v1 → v2: the seeded admin user's unset email becomes SQL NULL (it was the
//! placeholder string `-`, which does not satisfy the `Email` type).

use sea_query::{Expr, ExprTrait, Iden, Query};

use crate::schema::render::{render_query, update_db_version};
use crate::schema::version::SchemaDelta;

// Deltas are frozen: they carry their own idens instead of borrowing
// `current`'s, so later schema renames can't rewrite migration history.
#[derive(Iden, Clone, Copy)]
enum Users {
    Table,
    Email,
}

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 1;
    const TO_VERSION: usize = 2;

    fn up() -> Vec<String> {
        let unset_placeholder_email = Query::update()
            .table(Users::Table)
            .value(Users::Email, Option::<String>::None)
            .and_where(Expr::col(Users::Email).eq("-"))
            .to_owned();
        vec![render_query(unset_placeholder_email), update_db_version(Self::TO_VERSION)]
    }
}
