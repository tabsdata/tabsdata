//
// Copyright 2026 Tabsdata Inc.
//

//! v11 → v12: `service_tokens`. The table only: its index and view are derived
//! objects the ladder rebuilds from `current/` after the rungs. Also drops the
//! session that backed the retired instance token, a far-future JWT whose
//! place a server-managed service token takes.

use sea_query::{ColumnDef, Expr, ExprTrait, ForeignKey, ForeignKeyAction, Iden, Query, Table};

use crate::schema::render::{render_query, render_schema, update_db_version};
use crate::schema::sql::id_column;
use crate::schema::version::SchemaDelta;

#[derive(Iden, Clone, Copy)]
enum ServiceTokens {
    Table,
    Id,
    UserId,
    Slot,
    Digest,
    Hint,
    Comment,
    CreatedOn,
    ExpiresOn,
    LastUsedOn,
    Scope,
    System,
}

#[derive(Iden, Clone, Copy)]
enum Users {
    Table,
    Id,
}

#[derive(Iden, Clone, Copy)]
enum Sessions {
    Table,
    AccessTokenId,
}

const RETIRED_INSTANCE_TOKEN_SESSION: &str = "k0000000000000000000000004";

const SLOTS: [i32; 2] = [1, 2];

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 11;
    const TO_VERSION: usize = 12;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        vec![
            render_schema(
                Table::create()
                    .table(ServiceTokens::Table)
                    .col(id_column(ServiceTokens::Id).not_null().primary_key())
                    .col(id_column(ServiceTokens::UserId).not_null())
                    .col(ColumnDef::new(ServiceTokens::Slot).integer().not_null().check(Expr::col(ServiceTokens::Slot).is_in(SLOTS)))
                    .col(ColumnDef::new(ServiceTokens::Digest).text().not_null().unique_key())
                    .col(ColumnDef::new(ServiceTokens::Hint).text().not_null())
                    .col(ColumnDef::new(ServiceTokens::Comment).text().null())
                    .col(ColumnDef::new(ServiceTokens::CreatedOn).timestamp_with_time_zone().not_null())
                    .col(ColumnDef::new(ServiceTokens::ExpiresOn).timestamp_with_time_zone().not_null())
                    .col(ColumnDef::new(ServiceTokens::LastUsedOn).timestamp_with_time_zone().null())
                    .col(ColumnDef::new(ServiceTokens::Scope).text().not_null())
                    .col(ColumnDef::new(ServiceTokens::System).boolean().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .from(ServiceTokens::Table, ServiceTokens::UserId)
                            .to(Users::Table, Users::Id)
                            .on_delete(ForeignKeyAction::Restrict)
                            .on_update(ForeignKeyAction::Restrict),
                    )
                    .to_owned(),
            ),
            render_query(
                Query::delete()
                    .from_table(Sessions::Table)
                    .and_where(Expr::col(Sessions::AccessTokenId).eq(RETIRED_INSTANCE_TOKEN_SESSION))
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
    fn test_validate_delta_v11_to_v12() {
        validate_delta::<Migration>();
    }
}
