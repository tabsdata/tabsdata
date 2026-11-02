//
// Copyright 2026 Tabsdata Inc.
//

//! v21 → v22: `user_ui_data`, one opaque UI document per user.

use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Iden, Table};

use crate::schema::render::{render_schema, update_db_version};
use crate::schema::sql::id_column;
use crate::schema::version::SchemaDelta;

/// Frozen at the shape it had at v22.
#[derive(Iden, Clone, Copy)]
enum UserUiData {
    Table,
    UserId,
    Etag,
    ModifiedOn,
    Data,
}

/// Frozen at the shape it had at v22.
#[derive(Iden, Clone, Copy)]
enum Users {
    Table,
    Id,
}

pub struct Migration;

impl SchemaDelta for Migration {
    const FROM_VERSION: usize = 21;
    const TO_VERSION: usize = 22;

    fn retired() -> Vec<String> {
        vec![]
    }

    fn up() -> Vec<String> {
        vec![
            render_schema(
                Table::create()
                    .table(UserUiData::Table)
                    .col(id_column(UserUiData::UserId).not_null().primary_key())
                    .col(ColumnDef::new(UserUiData::Etag).uuid().not_null())
                    .col(ColumnDef::new(UserUiData::ModifiedOn).timestamp_with_time_zone().not_null())
                    .col(ColumnDef::new(UserUiData::Data).text().not_null())
                    .foreign_key(
                        ForeignKey::create()
                            .from(UserUiData::Table, UserUiData::UserId)
                            .to(Users::Table, Users::Id)
                            .on_delete(ForeignKeyAction::Restrict)
                            .on_update(ForeignKeyAction::Restrict),
                    )
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
    fn test_validate_delta_v21_to_v22() {
        validate_delta::<Migration>();
    }
}
