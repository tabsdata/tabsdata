//
// Copyright 2026 Tabsdata Inc.
//

use sea_query::{ColumnDef, ForeignKey, ForeignKeyAction, Iden, Table, TableCreateStatement};

use super::users::Users;
use crate::schema::Entity;
use crate::schema::sql::id_column;

#[derive(Iden, Clone, Copy, strum::EnumIter)]
pub enum UserUiData {
    Table,
    UserId,
    Etag,
    ModifiedOn,
    Data,
}

pub(super) struct Defs;

impl Entity for Defs {
    fn create_tables(&self) -> Vec<TableCreateStatement> {
        vec![
            Table::create()
                .table(UserUiData::Table)
                .col(id_column(UserUiData::UserId).not_null().primary_key())
                .col(ColumnDef::new(UserUiData::Etag).uuid().not_null())
                .col(ColumnDef::new(UserUiData::ModifiedOn).timestamp_with_time_zone().not_null())
                // Not json/jsonb: the client's bytes are returned unchanged and never parsed.
                .col(ColumnDef::new(UserUiData::Data).text().not_null())
                .foreign_key(
                    ForeignKey::create()
                        .from(UserUiData::Table, UserUiData::UserId)
                        .to(Users::Table, Users::Id)
                        .on_delete(ForeignKeyAction::Restrict)
                        .on_update(ForeignKeyAction::Restrict),
                )
                .to_owned(),
        ]
    }
}
